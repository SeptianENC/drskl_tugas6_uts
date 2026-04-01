package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"monolith-kv-sim/internal/activity"
	"monolith-kv-sim/internal/pipeline"
	"monolith-kv-sim/internal/redisx"
)

// Event legacy untuk ingestor (opsional, DDIA Part 2).
type Event struct {
	Key        string         `json:"key"`
	Value      map[string]any `json:"value"`
	TTLSeconds int            `json:"ttl_sec"`
	CacheHint  string         `json:"cache_hint"`
}

type featureReq struct {
	RequestID string         `json:"request_id"`
	UserID    int64          `json:"user_id"`
	VideoID   int64          `json:"video_id"`
	Ts        float64        `json:"ts"`
	Context   map[string]any `json:"context"`
}

type actionReq struct {
	RequestID string  `json:"request_id"`
	Label     string  `json:"label"`
	Ts        float64 `json:"ts"`
}

var actionLabels = []string{"watch_time", "like", "click"}

// Client HTTP dengan pool koneksi — penting untuk ratusan RPS concurrent.
var httpClient = &http.Client{
	Timeout: 8 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
	},
}

func main() {
	joinerURL := os.Getenv("JOINER_URL")
	if joinerURL == "" {
		joinerURL = "http://localhost:8890"
	}
	ingestorURL := os.Getenv("INGESTOR_URL")
	legacyIngest := os.Getenv("ENABLE_LEGACY_INGESTOR") == "1"

	rps := getInt("RPS", 100)
	if rps < 1 {
		rps = 1
	}
	hotRatio := getFloat("HOTKEY_RATIO", 0.2)

	hotVideoIDs := make([]int64, 50)
	for i := range hotVideoIDs {
		hotVideoIDs[i] = int64(10_000_000 + i)
	}

	rand.Seed(time.Now().UnixNano())
	rdb := redisx.NewCluster()
	ctx := context.Background()

	var pairCounter uint64

	// Mode cepat: ticker memicu goroutine baru per slot RPS — banyak pasangan feature+action bersamaan (throughput ≈ RPS).
	tick := time.NewTicker(time.Second / time.Duration(rps))
	go func() {
		for range tick.C {
			if pipeline.IsSlowMotion(ctx, rdb) {
				continue
			}
			go func() {
				n := atomic.AddUint64(&pairCounter, 1)
				runOnePair(ctx, rdb, joinerURL, ingestorURL, legacyIngest, hotVideoIDs, hotRatio, delayFast)
				if n%500 == 0 {
					activity.Push(ctx, rdb, "generator", "load_tick", "pairs_fast≈"+strconv.FormatUint(n, 10))
				}
			}()
		}
	}()

	// Mode slow motion: satu pasangan demi satu, jeda panjang antar feature→action agar terbaca di dashboard.
	for {
		time.Sleep(150 * time.Millisecond)
		if !pipeline.IsSlowMotion(ctx, rdb) {
			continue
		}
		atomic.AddUint64(&pairCounter, 1)
		runOnePair(ctx, rdb, joinerURL, ingestorURL, legacyIngest, hotVideoIDs, hotRatio, delayDDIA)
		activity.Push(ctx, rdb, "generator", "slow_motion_pair", "visible_delay")
		time.Sleep(900 * time.Millisecond)
	}
}

func delayFast() time.Duration {
	// Sangat pendek: overlap banyak request → throughput mendekati RPS.
	return time.Duration(rand.Intn(35)) * time.Millisecond
}

func delayDDIA() time.Duration {
	r := rand.Float64()
	switch {
	case r < 0.70:
		return time.Duration(800+rand.Intn(1200)) * time.Millisecond
	case r < 0.90:
		return time.Duration(4+rand.Intn(8)) * time.Second
	default:
		return time.Duration(12+rand.Intn(20)) * time.Second
	}
}

func runOnePair(
	ctx context.Context,
	rdb *redis.ClusterClient,
	joinerURL, ingestorURL string,
	legacyIngest bool,
	hotVideoIDs []int64,
	hotRatio float64,
	delayFn func() time.Duration,
) {
	reqID := uuid.NewString()
	userID := int64(rand.Intn(1_000_000))
	videoID := pickVideoID(hotVideoIDs, hotRatio)
	ts := float64(time.Now().UnixNano()) / 1e9

	feat := featureReq{
		RequestID: reqID,
		UserID:    userID,
		VideoID:   videoID,
		Ts:        ts,
		Context: map[string]any{
			"device": "sim",
			"hot":    isHotVideo(hotVideoIDs, videoID),
		},
	}
	bf, _ := json.Marshal(feat)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, joinerURL+"/feature", bytes.NewReader(bf))
	req.Header.Set("Content-Type", "application/json")
	_, _ = httpClient.Do(req)

	if legacyIngest && ingestorURL != "" {
		ev := Event{
			Key: "feature:" + reqID,
			Value: map[string]any{
				"user_id":    userID,
				"video_id":   videoID,
				"ts":         ts,
				"watch_time": rand.Float64() * 30.0,
			},
			TTLSeconds: 3600,
			CacheHint:  "none",
		}
		if isHotVideo(hotVideoIDs, videoID) {
			ev.CacheHint = "hot_read"
		}
		bi, _ := json.Marshal(ev)
		ireq, _ := http.NewRequestWithContext(ctx, http.MethodPost, ingestorURL+"/ingest", bytes.NewReader(bi))
		ireq.Header.Set("Content-Type", "application/json")
		_, _ = httpClient.Do(ireq)
	}

	time.Sleep(delayFn())

	actionTs := float64(time.Now().UnixNano()) / 1e9
	act := actionReq{
		RequestID: reqID,
		Label:     actionLabels[rand.Intn(len(actionLabels))],
		Ts:        actionTs,
	}
	ba, _ := json.Marshal(act)
	areq, _ := http.NewRequestWithContext(ctx, http.MethodPost, joinerURL+"/action", bytes.NewReader(ba))
	areq.Header.Set("Content-Type", "application/json")
	_, _ = httpClient.Do(areq)
}

func isHotVideo(hot []int64, vid int64) bool {
	for _, h := range hot {
		if h == vid {
			return true
		}
	}
	return false
}

func pickVideoID(hotIDs []int64, hotRatio float64) int64 {
	if rand.Float64() < hotRatio {
		return hotIDs[rand.Intn(len(hotIDs))]
	}
	return int64(rand.Intn(5_000_000))
}

func getInt(env string, def int) int {
	if s := os.Getenv(env); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

func getFloat(env string, def float64) float64 {
	if s := os.Getenv(env); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	}
	return def
}
