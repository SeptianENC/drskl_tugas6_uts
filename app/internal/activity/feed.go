// Package activity menyimpan jejak peristiwa ke Redis LIST untuk ditampilkan di dashboard (derived pipeline terlihat di browser).
package activity

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// KeyFeed adalah LIST JSON satu baris per peristiwa (LPUSH + LTRIM).
	KeyFeed = "system:activity_feed"
	maxLen  = 250
)

// Push menambahkan satu baris aktivitas (best-effort; error diabaikan agar tidak mengganggu path utama).
func Push(ctx context.Context, rdb *redis.ClusterClient, service, event, detail string) {
	if rdb == nil {
		return
	}
	rec := map[string]string{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"service": service,
		"event":   event,
		"detail":  detail,
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_ = rdb.LPush(ctx, KeyFeed, string(b)).Err()
	_ = rdb.LTrim(ctx, KeyFeed, 0, maxLen-1).Err()
}
