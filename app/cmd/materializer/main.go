package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"monolith-kv-sim/internal/hdfsx"
	"monolith-kv-sim/internal/redisx"
)

const (
	keyFeatureList = "training_examples"
	spoolDir       = "/tmp/spool"
)

func envInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "replay" {
		replayMain(os.Args[2:])
		return
	}
	runMaterializer()
}

func runMaterializer() {
	interval := envInt("MATERIALIZE_INTERVAL_SECONDS", 10)
	batch := envInt("MATERIALIZE_BATCH_SIZE", 500)
	if batch < 1 {
		batch = 1
	}

	rdb := redisx.NewCluster()
	ctx := context.Background()
	w := hdfsx.NewWriter()
	_ = os.MkdirAll(spoolDir, 0755)

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()

	flushSpool(w)
	processListBatch(ctx, rdb, w, batch)
	for range tick.C {
		flushSpool(w)
		processListBatch(ctx, rdb, w, batch)
	}
}

func processListBatch(ctx context.Context, rdb *redis.ClusterClient, w *hdfsx.Writer, batch int) {
	vals, err := rdb.LRange(ctx, keyFeatureList, 0, int64(batch-1)).Result()
	if err != nil || len(vals) == 0 {
		return
	}
	events := make([]any, 0, len(vals))
	for _, s := range vals {
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			m = map[string]any{"_raw": s}
		}
		events = append(events, m)
	}
	now := time.Now().UTC()
	rel := fmt.Sprintf(
		"training_examples/date=%s/hour=%02d/min=%02d/part-%d.jsonl",
		now.Format("2006-01-02"), now.Hour(), now.Minute(), now.UnixMilli(),
	)
	err = w.WriteJSONLToPath(rel, events)
	if err != nil {
		spoolPath := filepath.Join(spoolDir, fmt.Sprintf("spool_%d.jsonl", now.UnixNano()))
		if werr := writeLinesFile(spoolPath, vals); werr != nil {
			fmt.Fprintf(os.Stderr, "materializer: hdfs err %v; spool err %v\n", err, werr)
			return
		}
		fmt.Fprintf(os.Stderr, "materializer: hdfs write failed, spooled %s: %v\n", spoolPath, err)
	}
	if err2 := rdb.LTrim(ctx, keyFeatureList, int64(len(vals)), -1).Err(); err2 != nil {
		fmt.Fprintf(os.Stderr, "materializer: LTRIM: %v\n", err2)
	}
}

func writeLinesFile(path string, lines []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, ln := range lines {
		if _, err := w.WriteString(ln + "\n"); err != nil {
			return err
		}
	}
	return w.Flush()
}

func flushSpool(w *hdfsx.Writer) {
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		full := filepath.Join(spoolDir, e.Name())
		b, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		var lines []string
		sc := bufio.NewScanner(strings.NewReader(string(b)))
		for sc.Scan() {
			if t := strings.TrimSpace(sc.Text()); t != "" {
				lines = append(lines, t)
			}
		}
		if len(lines) == 0 {
			_ = os.Remove(full)
			continue
		}
		events := make([]any, 0, len(lines))
		for _, s := range lines {
			var m map[string]any
			if err := json.Unmarshal([]byte(s), &m); err != nil {
				m = map[string]any{"_raw": s}
			}
			events = append(events, m)
		}
		now := time.Now().UTC()
		rel := fmt.Sprintf(
			"training_examples/date=%s/hour=%02d/min=%02d/part-%d.jsonl",
			now.Format("2006-01-02"), now.Hour(), now.Minute(), now.UnixMilli(),
		)
		if err := w.WriteJSONLToPath(rel, events); err != nil {
			fmt.Fprintf(os.Stderr, "materializer: flush spool retry %s: %v\n", full, err)
			continue
		}
		_ = os.Remove(full)
	}
}

func replayMain(args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	date := fs.String("date", "", "YYYY-MM-DD")
	hour := fs.Int("hour", -1, "0-23")
	minute := fs.Int("minute", -1, "0-59, optional; if unset all minutes under hour")
	_ = fs.Parse(args)

	if *date == "" || *hour < 0 || *hour > 23 {
		fmt.Fprintf(os.Stderr, "usage: materializer replay -date YYYY-MM-DD -hour H [-minute M]\n")
		os.Exit(2)
	}

	rdb := redisx.NewCluster()
	ctx := context.Background()
	w := hdfsx.NewWriter()
	base := strings.TrimSuffix(w.Path, "/")
	var paths []string

	if *minute >= 0 && *minute <= 59 {
		p := fmt.Sprintf("%s/training_examples/date=%s/hour=%02d/min=%02d", base, *date, *hour, *minute)
		paths = listJSONLFiles(w, p)
	} else {
		hp := fmt.Sprintf("%s/training_examples/date=%s/hour=%02d", base, *date, *hour)
		out, err := w.ListRecursive(hp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay: ls -R %s: %v\n", hp, err)
			os.Exit(1)
		}
		paths = parseLsRPaths(string(out), hp)
	}

	n := 0
	for _, hdfsPath := range paths {
		b, err := w.CatFile(hdfsPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay: cat %s: %v\n", hdfsPath, err)
			continue
		}
		sc := bufio.NewScanner(strings.NewReader(string(b)))
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			if err := rdb.LPush(ctx, keyFeatureList, line).Err(); err != nil {
				fmt.Fprintf(os.Stderr, "replay: LPUSH: %v\n", err)
				os.Exit(1)
			}
			n++
		}
	}
	fmt.Printf("replay: pushed %d lines to Redis list %s\n", n, keyFeatureList)
}

func listJSONLFiles(w *hdfsx.Writer, dir string) []string {
	out, err := w.ListRecursive(dir)
	if err != nil {
		return nil
	}
	return parseLsRPaths(string(out), dir)
}

func parseLsRPaths(lsOut, prefix string) []string {
	var paths []string
	for _, line := range strings.Split(lsOut, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Found") {
			continue
		}
		if !strings.HasPrefix(line, "-") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		p := fields[len(fields)-1]
		if strings.HasSuffix(p, ".jsonl") && strings.HasPrefix(p, prefix) {
			paths = append(paths, p)
		}
	}
	return paths
}
