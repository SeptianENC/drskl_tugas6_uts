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
	"monolith-kv-sim/internal/cachex"
	"monolith-kv-sim/internal/hdfsx"
	"monolith-kv-sim/internal/redisx"
)

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
		c.JSON(200, gin.H{"ok": true, "seeded": count, "message": "keys written with _ts 2 min ago; offloader will move them within OFFLOAD_INTERVAL_SECONDS"})
	}
}

// Event merepresentasikan data event yang akan diingest ke sistem.
// Key: identifier unik untuk event ini
// Value: data payload dalam bentuk map
// TTLSeconds: waktu hidup data di Redis (dalam detik)
// CacheHint: petunjuk apakah data ini hot (sering diakses) atau tidak
type Event struct {
	Key        string                 `json:"key"`
	Value      map[string]any         `json:"value"`
	TTLSeconds int                    `json:"ttl_sec"`
	CacheHint  string                 `json:"cache_hint"`
}

func main() {
	// Inisialisasi koneksi ke Redis cluster (in-memory cache)
	r := redisx.NewCluster()
	ctx := context.Background()

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

		// Cek rasio penggunaan memori Redis cluster
		// Jika sudah mencapai threshold (misalnya 80%), alihkan ke HDFS
		ratio, _ := redisx.ClusterMemRatio(ctx, r)
		if ratio >= soft {
			// Simpan ke HDFS karena Redis sudah penuh
			_ = hdfs.WriteJSONL([]any{ev})
			c.JSON(200, gin.H{"ok": true, "stored": "hdfs", "mem_ratio": ratio})
			return
		}

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
			c.JSON(200, gin.H{"ok": false, "stored": "hdfs", "error": err.Error(), "mem_ratio": ratio})
			return
		}

		// Coba simpan ke Redis cluster dengan TTL yang ditentukan
		err = r.Set(ctx, ev.Key, b, time.Duration(ev.TTLSeconds)*time.Second).Err()
		if err != nil {
			// Jika gagal menyimpan ke Redis (misalnya karena OOM), fallback ke HDFS
			_ = hdfs.WriteJSONL([]any{ev})
			c.JSON(200, gin.H{"ok": true, "stored": "hdfs", "error": err.Error(), "mem_ratio": ratio})
			return
		}
		// Berhasil disimpan di Redis
		c.JSON(200, gin.H{"ok": true, "stored": "redis", "mem_ratio": ratio})
	})

	// Endpoint GET /get/*key: mengambil data berdasarkan key
	// Mengimplementasikan cache-aside pattern: cek local cache -> Redis -> HDFS (jika perlu)
	router.GET("/get/*key", func(c *gin.Context) {
		key := c.Param("key")
		// Bersihkan key dari leading slash jika ada
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		if key == "" {
			c.JSON(400, gin.H{"ok": false, "error": "missing key"})
			return
		}

		// Cache-aside pattern: cek local LRU cache dulu (jika enabled)
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
			// Sesuai diagram: jika tidak ditemukan di cache, baca dari on-disk KV-Store (HDFS)
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
