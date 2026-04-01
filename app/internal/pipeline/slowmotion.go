package pipeline

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisKeySlowMotion diset dashboard (1=slow motion, 0/empty=cepat).
const RedisKeySlowMotion = "pipeline:slow_motion"

var (
	slowMu     sync.RWMutex
	slowCached bool
	slowExpiry time.Time
)

// IsSlowMotion membaca flag dari Redis dengan cache singkat agar generator tidak memanggil GET tiap milidetik.
func IsSlowMotion(ctx context.Context, rdb *redis.ClusterClient) bool {
	if rdb == nil {
		return os.Getenv("GENERATOR_FORCE_SLOW") == "1"
	}
	slowMu.RLock()
	if time.Now().Before(slowExpiry) {
		v := slowCached
		slowMu.RUnlock()
		return v
	}
	slowMu.RUnlock()

	v, err := rdb.Get(ctx, RedisKeySlowMotion).Result()
	on := err == nil && (v == "1" || v == "true" || v == "yes")

	slowMu.Lock()
	slowCached = on
	slowExpiry = time.Now().Add(80 * time.Millisecond)
	slowMu.Unlock()
	return on
}

