package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"monolith-kv-sim/internal/redisx"
)

const (
	keyFeatureList      = "training_examples"
	prefixFeature       = "feature:"
	prefixProcessed     = "processed:"
	keyJoinSuccess      = "joiner:join_success_total"
	keyJoinMiss         = "joiner:join_miss_total"
	keyEmitted          = "joiner:emitted_total"
)

type FeaturePayload struct {
	RequestID string         `json:"request_id" binding:"required"`
	UserID    int64          `json:"user_id"`
	VideoID   int64          `json:"video_id"`
	Ts        float64        `json:"ts"`
	Context   map[string]any `json:"context"`
}

type ActionPayload struct {
	RequestID string  `json:"request_id" binding:"required"`
	Label     string  `json:"label" binding:"required"`
	Ts        float64 `json:"ts"`
}

func envInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

func main() {
	_ = os.Setenv("GIN_MODE", "release")
	featureTTL := envInt("FEATURE_TTL_SECONDS", 300)
	processedTTL := envInt("PROCESSED_TTL_SECONDS", 3600)

	rdb := redisx.NewCluster()
	ctx := context.Background()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", func(c *gin.Context) {
		if err := rdb.Ping(ctx).Err(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	router.POST("/feature", func(c *gin.Context) {
		var p FeaturePayload
		if err := c.ShouldBindJSON(&p); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		b, err := json.Marshal(p)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		key := prefixFeature + p.RequestID
		if err := rdb.Set(ctx, key, b, time.Duration(featureTTL)*time.Second).Err(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "stored": "redis", "key": key})
	})

	router.POST("/action", func(c *gin.Context) {
		var p ActionPayload
		if err := c.ShouldBindJSON(&p); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		fkey := prefixFeature + p.RequestID
		raw, err := rdb.Get(ctx, fkey).Result()
		if err == redis.Nil {
			_ = rdb.Incr(ctx, keyJoinMiss).Err()
			c.JSON(http.StatusOK, gin.H{"ok": true, "joined": false, "reason": "join_miss"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		procKey := prefixProcessed + p.RequestID
		ok, err := rdb.SetNX(ctx, procKey, "1", time.Duration(processedTTL)*time.Second).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if !ok {
			c.JSON(http.StatusOK, gin.H{"ok": true, "joined": false, "reason": "idempotent_skip"})
			return
		}

		var feat FeaturePayload
		if err := json.Unmarshal([]byte(raw), &feat); err != nil {
			_ = rdb.Del(ctx, procKey).Err()
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "bad feature json"})
			return
		}

		now := float64(time.Now().UnixNano()) / 1e9
		ex := map[string]any{
			"request_id": p.RequestID,
			"feature":    json.RawMessage(raw),
			"action": map[string]any{
				"label": p.Label,
				"ts":    p.Ts,
			},
			"feature_ts": feat.Ts,
			"joined_at":  now,
		}
		line, err := json.Marshal(ex)
		if err != nil {
			_ = rdb.Del(ctx, procKey).Err()
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if err := rdb.LPush(ctx, keyFeatureList, string(line)).Err(); err != nil {
			_ = rdb.Del(ctx, procKey).Err()
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		_ = rdb.Incr(ctx, keyJoinSuccess).Err()
		_ = rdb.Incr(ctx, keyEmitted).Err()
		c.JSON(http.StatusOK, gin.H{"ok": true, "joined": true})
	})

	router.GET("/stats", func(c *gin.Context) {
		js, ms, em := int64(0), int64(0), int64(0)
		if v, err := rdb.Get(ctx, keyJoinSuccess).Int64(); err == nil {
			js = v
		}
		if v, err := rdb.Get(ctx, keyJoinMiss).Int64(); err == nil {
			ms = v
		}
		if v, err := rdb.Get(ctx, keyEmitted).Int64(); err == nil {
			em = v
		}
		c.JSON(http.StatusOK, gin.H{
			"join_success_total": js,
			"join_miss_total":    ms,
			"emitted_total":      em,
		})
	})

	router.GET("/metrics", func(c *gin.Context) {
		js, ms, em := int64(0), int64(0), int64(0)
		if v, err := rdb.Get(ctx, keyJoinSuccess).Int64(); err == nil {
			js = v
		}
		if v, err := rdb.Get(ctx, keyJoinMiss).Int64(); err == nil {
			ms = v
		}
		if v, err := rdb.Get(ctx, keyEmitted).Int64(); err == nil {
			em = v
		}
		c.Header("Content-Type", "text/plain; charset=utf-8")
		c.String(http.StatusOK, fmt.Sprintf(
			"# HELP joiner_join_success_total Successful joins (feature+action).\n"+
				"# TYPE joiner_join_success_total counter\n"+
				"joiner_join_success_total %d\n"+
				"# HELP joiner_join_miss_total Actions without feature or after TTL.\n"+
				"# TYPE joiner_join_miss_total counter\n"+
				"joiner_join_miss_total %d\n"+
				"# HELP joiner_emitted_total Training examples pushed to Redis list.\n"+
				"# TYPE joiner_emitted_total counter\n"+
				"joiner_emitted_total %d\n",
			js, ms, em,
		))
	})

	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}
	if err := router.Run(addr); err != nil {
		fmt.Fprintf(os.Stderr, "joiner: %v\n", err)
		os.Exit(1)
	}
}
