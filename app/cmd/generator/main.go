package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"monolith-kv-sim/internal/activity"
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

func main() {
	joinerURL := os.Getenv("JOINER_URL")
	if joinerURL == "" {
		joinerURL = "http://localhost:8890"
	}
	ingestorURL := os.Getenv("INGESTOR_URL")
	legacyIngest := os.Getenv("ENABLE_LEGACY_INGESTOR") == "1"

	rps := getInt("RPS", 200)
	hotRatio := getFloat("HOTKEY_RATIO", 0.2)

	hotVideoIDs := make([]int64, 50)
	for i := range hotVideoIDs {
		hotVideoIDs[i] = int64(10_000_000 + i)
	}

	rand.Seed(time.Now().UnixNano())
	interval := time.Second / time.Duration(max(1, rps))
	rdb := redisx.NewCluster()
	ctx := context.Background()
	cycle := 0

	for {
		cycle++
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
		_, _ = http.Post(joinerURL+"/feature", "application/json", bytes.NewReader(bf))

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
			_, _ = http.Post(ingestorURL+"/ingest", "application/json", bytes.NewReader(bi))
		}

		delay := sampleActionDelay()
		time.Sleep(delay)

		actionTs := float64(time.Now().UnixNano()) / 1e9
		act := actionReq{
			RequestID: reqID,
			Label:     actionLabels[rand.Intn(len(actionLabels))],
			Ts:        actionTs,
		}
		ba, _ := json.Marshal(act)
		_, _ = http.Post(joinerURL+"/action", "application/json", bytes.NewReader(ba))

		if cycle%100 == 0 {
			activity.Push(ctx, rdb, "generator", "load_tick", "feature+action pairs≈"+strconv.Itoa(cycle))
		}

		time.Sleep(interval)
	}
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

// sampleActionDelay mensimulasikan action yang datang setelah feature.
// Profil `fast` (default): pipeline terasa cepat untuk demo.
// Profil `ddia`: jeda panjang (sampai ~2 menit) untuk demo join miss / late event — lebih lambat.
func sampleActionDelay() time.Duration {
	profile := os.Getenv("ACTION_DELAY_PROFILE")
	if profile == "" {
		profile = "fast"
	}
	r := rand.Float64()
	switch profile {
	case "ddia", "slow", "teach":
		switch {
		case r < 0.70:
			return time.Duration(rand.Intn(2000)) * time.Millisecond
		case r < 0.90:
			return time.Duration(10+rand.Intn(21)) * time.Second
		default:
			return time.Duration(60+rand.Intn(61)) * time.Second
		}
	default: // fast
		switch {
		case r < 0.70:
			return time.Duration(rand.Intn(500)) * time.Millisecond
		case r < 0.90:
			return time.Duration(1+rand.Intn(5)) * time.Second
		default:
			return time.Duration(8+rand.Intn(18)) * time.Second
		}
	}
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
