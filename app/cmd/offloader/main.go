// Offloader: memindahkan data yang sudah terlalu lama di Redis ke HDFS
// sesuai skema monolith recommendation — agar in-memory cache tidak penuh.
// Setiap interval, scan key di Redis; jika umur data > OFFLOAD_AFTER_SECONDS,
// tulis ke HDFS (on-disk KV store) lalu hapus dari Redis.

package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"monolith-kv-sim/internal/hdfsx"
	"monolith-kv-sim/internal/redisx"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Print("offloader: process started")

	ctx := context.Background()
	r := redisx.NewCluster()
	hdfs := hdfsx.NewWriter()

	// Setelah berapa detik data dianggap "terlalu lama" dan dipindah ke HDFS
	offloadAfterSec := getInt("OFFLOAD_AFTER_SECONDS", 600) // default 10 menit
	// Interval jalannya proses offload (detik)
	intervalSec := getInt("OFFLOAD_INTERVAL_SECONDS", 60)
	// Jika rasio memori cluster >= nilai ini, offloader boleh memindahkan data lebih agresif
	forceMemRatio := getFloat("OFFLOAD_FORCE_MEM_RATIO", 0.70)
	// Saat mode agresif aktif, minimal umur key (detik) agar tetap tidak memindahkan key yang terlalu baru
	forceMinAgeSec := getInt("OFFLOAD_FORCE_MIN_AGE_SECONDS", 5)
	// Berapa detik menunggu Redis siap saat startup (menghindari connection refused saat Redis/cluster-init belum selesai)
	redisInitTimeoutSec := getInt("OFFLOAD_REDIS_INIT_TIMEOUT_SEC", 120)

	log.Printf("offloader: connecting to Redis (retry up to %d sec)...", redisInitTimeoutSec)
	deadline := time.Now().Add(time.Duration(redisInitTimeoutSec) * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = r.Ping(ctx).Err()
		if lastErr == nil {
			break
		}
		log.Printf("offloader: redis ping failed, retry in 5s: %v", lastErr)
		time.Sleep(5 * time.Second)
	}
	if lastErr != nil {
		log.Fatalf("offloader: redis ping failed after %d sec: %v", redisInitTimeoutSec, lastErr)
	}
	log.Printf("offloader started: OFFLOAD_AFTER_SECONDS=%d, OFFLOAD_INTERVAL_SECONDS=%d, OFFLOAD_FORCE_MEM_RATIO=%.2f, OFFLOAD_FORCE_MIN_AGE_SECONDS=%d, HDFS_PATH=%s",
		offloadAfterSec, intervalSec, forceMemRatio, forceMinAgeSec, os.Getenv("HDFS_PATH"))

	// Pastikan path HDFS sudah dibuat sejak awal agar kegagalan bisa terlihat di log lebih cepat.
	hdfs.EnsureDir()

	for {
		doOffload(ctx, r, hdfs, offloadAfterSec, forceMemRatio, forceMinAgeSec)
		time.Sleep(time.Duration(intervalSec) * time.Second)
	}
}

func doOffload(ctx context.Context, r *redis.ClusterClient, hdfs *hdfsx.Writer, offloadAfterSec int, forceMemRatio float64, forceMinAgeSec int) {
	memRatio, memErr := redisx.ClusterMemRatio(ctx, r)
	forceByMem := memErr == nil && memRatio >= forceMemRatio

	cutoff := time.Now().Add(-time.Duration(offloadAfterSec) * time.Second).Unix()
	forceCutoff := time.Now().Add(-time.Duration(forceMinAgeSec) * time.Second).Unix()
	var scanned, old, moved, writeFail, parseFail int

	// Iterasi tiap shard di cluster; SCAN penuh (cursor sampai 0) agar semua key terproses
	err := r.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		var cursor uint64
		for {
			keys, next, err := shard.Scan(ctx, cursor, "*", 100).Result()
			if err != nil {
				return err
			}
			for _, key := range keys {
				scanned++
				val, err := shard.Get(ctx, key).Bytes()
				if err != nil {
					continue
				}
				ts, hasTS := extractTS(val)
				if !hasTS {
					parseFail++
				}

				shouldMove := false
				if hasTS && ts < cutoff {
					old++
					shouldMove = true
				} else if forceByMem && hasTS && ts < forceCutoff {
					shouldMove = true
				} else if forceByMem && !hasTS {
					shouldMove = true
				}

				if shouldMove {
					if err := hdfs.WriteKeyValue(key, val); err != nil {
						writeFail++
						log.Printf("offload write failed key=%q: %v", key, err)
						continue
					}
					if shard.Del(ctx, key).Err() == nil {
						moved++
					}
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("offload scan error: %v", err)
		return
	}
	if memErr != nil {
		log.Printf("offload mem ratio check failed: %v", memErr)
	}
	if scanned > 0 || old > 0 || moved > 0 || writeFail > 0 || parseFail > 0 || forceByMem {
		log.Printf("offload run: scanned=%d old=%d moved=%d write_fail=%d parse_fail=%d mem_ratio=%.4f force_by_mem=%t (age_cutoff=%d sec, force_min_age=%d sec)", scanned, old, moved, writeFail, parseFail, memRatio, forceByMem, offloadAfterSec, forceMinAgeSec)
	}
}

func extractTS(val []byte) (int64, bool) {
	var payload map[string]any
	if err := json.Unmarshal(val, &payload); err != nil {
		return 0, false
	}
	raw, ok := payload["_ts"]
	if !ok {
		return 0, false
	}
	switch ts := raw.(type) {
	case float64:
		return int64(ts), true
	case int64:
		return ts, true
	case int:
		return int64(ts), true
	case string:
		if v, err := strconv.ParseInt(ts, 10, 64); err == nil {
			return v, true
		}
	}
	return 0, false
}

func getFloat(env string, def float64) float64 {
	if s := os.Getenv(env); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	}
	return def
}

func getInt(env string, def int) int {
	if s := os.Getenv(env); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}
