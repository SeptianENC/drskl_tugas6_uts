package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"monolith-kv-sim/internal/activity"
	"monolith-kv-sim/internal/cachex"
	"monolith-kv-sim/internal/hdfsx"
	"monolith-kv-sim/internal/redisx"
)

const keyIngestTotal = "stats:ingestor:ingest_total"

// seedOldKeysHandler menulis N key ke Redis dengan _ts 2 menit lalu agar offloader bisa memindahkan ke HDFS (uji pipeline).
func seedOldKeysHandler(r *redis.ClusterClient, ctx context.Context) gin.HandlerFunc {
	return func(c *gin.Context) {
		count := 20
		if s := c.Query("count"); s != "" {
			if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 500 {
				count = n
			}
		}
		ts := time.Now().Unix() - 120 // 2 menit lalu
		for i := 0; i < count; i++ {
			key := "seed:old:" + strconv.Itoa(i)
			val := map[string]any{"_ts": ts, "seed": true, "i": i}
			b, _ := json.Marshal(val)
			_ = r.Set(ctx, key, b, 10*time.Minute).Err()
		}
		activity.Push(ctx, r, "ingestor", "seed_old_keys", "count="+strconv.Itoa(count))
		c.JSON(200, gin.H{"ok": true, "seeded": count, "message": "keys written with _ts 2 min ago; offloader will move them within OFFLOAD_INTERVAL_SECONDS"})
	}
}

// Event merepresentasikan data event yang akan diingest ke sistem.
// Key: identifier unik untuk event ini
// Value: data payload dalam bentuk map
// TTLSeconds: waktu hidup data di Redis (dalam detik)
// CacheHint: petunjuk apakah data ini hot (sering diakses) atau tidak
// RequestID: idempotency key opsional (jika dikirim sama berulang kali, server akan menghindari double-processing selama masih tersimpan di Redis)
type Event struct {
	Key        string                 `json:"key"`
	Value      map[string]any         `json:"value"`
	TTLSeconds int                    `json:"ttl_sec"`
	CacheHint  string                 `json:"cache_hint"`
	RequestID  string                 `json:"request_id"`
}

const (
	idemPrefix           = "idem:req:"
	defaultRedisTimeout  = 2 * time.Second
	defaultRedisMaxRetry = 3
)

// getEnvInt membaca integer dari env dengan default.
func getEnvInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

// withRetry menjalankan fn beberapa kali sampai berhasil atau context habis.
func withRetry(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	backoff := 50 * time.Millisecond
	for i := 0; i < maxAttempts; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err = fn(); err == nil {
			return nil
		}
		if i < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
	}
	return err
}

// checkIdempotency mengecek apakah RequestID sudah pernah diproses (disimpan di Redis).
// Jika ya, akan mengembalikan label "stored".
func checkIdempotency(ctx context.Context, r *redis.ClusterClient, reqID string) (string, bool) {
	if reqID == "" {
		return "", false
	}
	val, err := r.Get(ctx, idemPrefix+reqID).Result()
	if err != nil {
		return "", false
	}
	return val, true
}

func main() {
	// Inisialisasi koneksi ke Redis cluster (in-memory cache)
	r := redisx.NewCluster()
	ctx := context.Background()

	redisTimeoutMs := getEnvInt("INGEST_REDIS_TIMEOUT_MS", int(defaultRedisTimeout.Milliseconds()))
	redisMaxRetry := getEnvInt("INGEST_REDIS_MAX_RETRY", defaultRedisMaxRetry)

	// Ambil threshold penggunaan memori dari environment variable.
	// Jika penggunaan memori Redis >= threshold ini, data akan diarahkan ke HDFS.
	soft := 0.80
	if s := os.Getenv("REDIS_MAXMEM_SOFT"); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			soft = v
		}
	}

	// Inisialisasi local LRU cache untuk hot keys (opsional, untuk optimasi)
	cache := cachex.NewLRU()
	// Inisialisasi writer untuk HDFS (on-disk KV store)
	hdfs := hdfsx.NewWriter()

	// Setup Gin router untuk HTTP API
	router := gin.Default()

	// Endpoint GET /health: simple heartbeat untuk Uptime Kuma / monitoring eksternal
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Endpoint POST /ingest: menerima event dan menyimpannya ke Redis atau HDFS
	// Mengimplementasikan cache-aside pattern dengan overflow ke HDFS
	router.POST("/ingest", func(c *gin.Context) {
		var ev Event
		// Validasi input: pastikan event memiliki key dan value yang valid
		if err := c.ShouldBindJSON(&ev); err != nil || ev.Key == "" || ev.Value == nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid event"})
			return
		}
		// Set default TTL jika tidak disediakan
		if ev.TTLSeconds <= 0 {
			ev.TTLSeconds = 3600
		}

		// Jika event ditandai sebagai "hot_read", tambahkan ke local LRU cache
		// Ini membantu mengurangi beban ke Redis untuk data yang sering diakses
		if cache.Enabled && ev.CacheHint == "hot_read" {
			cache.LRU.Add(ev.Key, "cached") // Flag saja, nilai aktual di-cache di path read
		}

		// Cek rasio memori Redis sekali di awal agar tersedia untuk semua response (termasuk error path).
		ratio, _ := redisx.ClusterMemRatio(ctx, r)

		// Serialize nilai event ke JSON sebelum disimpan di Redis.
		// Tambah _ts (timestamp) agar offloader bisa tahu umur data dan memindahkan yang sudah lama ke HDFS.
		payload := ev.Value
		if payload == nil {
			payload = make(map[string]any)
		}
		payload["_ts"] = time.Now().Unix()
		b, err := json.Marshal(payload)
		if err != nil {
			// Jika gagal serialize, fallback ke HDFS
			_ = hdfs.WriteJSONL([]any{ev})
			_, _ = r.Incr(ctx, keyIngestTotal).Result()
			activity.Push(ctx, r, "ingestor", "ingest_overflow", "serialize_err→hdfs key="+ev.Key)
			c.JSON(200, gin.H{"ok": false, "stored": "hdfs", "error": err.Error(), "mem_ratio": ratio})
			return
		}

		// Jika rasio memori sudah mencapai threshold, alihkan ke HDFS (overflow pattern).
		if ratio >= soft {
			_ = hdfs.WriteJSONL([]any{ev})
			_, _ = r.Incr(ctx, keyIngestTotal).Result()
			activity.Push(ctx, r, "ingestor", "ingest_overflow", "redis_soft_limit→hdfs key="+ev.Key)
			c.JSON(200, gin.H{"ok": true, "stored": "hdfs", "reason": "redis_soft_limit", "mem_ratio": ratio})
			return
		}

		// Konteks khusus untuk operasi Redis (timeout + retry)
		redisCtx, cancel := context.WithTimeout(ctx, time.Duration(redisTimeoutMs)*time.Millisecond)
		defer cancel()

		// Idempotency: jika RequestID dikirim dan sudah pernah diproses, hindari double-processing.
		if ev.RequestID != "" {
			if stored, ok := checkIdempotency(redisCtx, r, ev.RequestID); ok {
				activity.Push(ctx, r, "ingestor", "ingest_idempotent", "request_id="+ev.RequestID+" stored="+stored)
				c.JSON(200, gin.H{
					"ok":          true,
					"stored":      stored,
					"idempotent":  true,
					"mem_ratio":   ratio,
					"request_id":  ev.RequestID,
					"redis_retry": 0,
				})
				return
			}
		}

		var lastErr error
		err = withRetry(redisCtx, redisMaxRetry, func() error {
			lastErr = r.Set(redisCtx, ev.Key, b, time.Duration(ev.TTLSeconds)*time.Second).Err()
			return lastErr
		})
		if err != nil {
			// Jika gagal menyimpan ke Redis setelah beberapa kali retry (timeout/network),
			// fallback ke HDFS sebagai penyimpanan utama sementara.
			_ = hdfs.WriteJSONL([]any{ev})
			_, _ = r.Incr(ctx, keyIngestTotal).Result()
			activity.Push(ctx, r, "ingestor", "ingest_fallback", "redis_unavailable→hdfs key="+ev.Key)
			c.JSON(200, gin.H{
				"ok":         true,
				"stored":     "hdfs",
				"reason":     "redis_unavailable",
				"error":      lastErr.Error(),
				"mem_ratio":  ratio,
				"request_id": ev.RequestID,
			})
			return
		}

		// Jika operasi Redis sukses dan ada RequestID, simpan marker idempotency.
		if ev.RequestID != "" {
			_ = r.Set(ctx, idemPrefix+ev.RequestID, "redis", 10*time.Minute).Err()
		}

		_, _ = r.Incr(ctx, keyIngestTotal).Result()
		activity.Push(ctx, r, "ingestor", "ingest_ok", "stored=redis key="+ev.Key)
		// Berhasil disimpan di Redis
		c.JSON(200, gin.H{
			"ok":         true,
			"stored":     "redis",
			"mem_ratio":  ratio,
			"request_id": ev.RequestID,
		})
	})

	// Endpoint GET /get/*key: mengambil data berdasarkan key
	// Mengimplementasikan cache-aside pattern: cek local cache -> Redis -> HDFS (jika perlu)
	router.GET("/get/*key", func(c *gin.Context) {
		key := c.Param("key")
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		if key == "" {
			c.JSON(400, gin.H{"ok": false, "error": "missing key"})
			return
		}

		// Cache-aside pattern: cek local LRU cache dulu 
		// Ini mengurangi latency untuk hot keys yang sering diakses
		if cache.Enabled {
			if v, ok := cache.LRU.Get("VAL:" + key); ok {
				c.JSON(200, gin.H{"ok": true, "source": "local_cache", "value": v})
				return
			}
		}

		// Jika tidak ada di local cache, coba ambil dari Redis cluster
		val, err := r.Get(ctx, key).Result()
		if err != nil {
			// Jika tidak ditemukan di cache, baca dari on-disk KV-Store (HDFS)
			if buf, readErr := hdfs.ReadByKey(key); readErr == nil {
				c.JSON(200, gin.H{"ok": true, "source": "hdfs", "value": string(buf)})
				return
			}
			c.JSON(404, gin.H{"ok": false, "error": err.Error()})
			return
		}
		// Jika ditemukan di Redis, cache di local LRU untuk akses berikutnya
		if cache.Enabled {
			cache.LRU.Add("VAL:"+key, val)
		}
		c.JSON(200, gin.H{"ok": true, "source": "redis", "value": val})
	})

	// Seed key dengan _ts di masa lalu agar offloader bisa memindahkan ke HDFS (uji deterministik).
	// GET/POST /seed-old-keys?count=20 menulis 20 key ke Redis dengan _ts = 2 menit lalu.
	router.GET("/seed-old-keys", seedOldKeysHandler(r, ctx))
	router.POST("/seed-old-keys", seedOldKeysHandler(r, ctx))

	// Test koneksi ke Redis sebelum start server
	_ = r.Ping(ctx).Err()
	// Start HTTP server di port 8080
	router.Run(":8080")
}
