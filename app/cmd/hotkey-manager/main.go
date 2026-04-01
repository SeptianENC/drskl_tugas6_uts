package main

import (
	"context"
	"os"
	"strconv"
	"time"

	"monolith-kv-sim/internal/activity"
	"monolith-kv-sim/internal/redisx"
)

// hotkey-manager adalah service yang memantau hot keys di Redis cluster.
// Service ini dapat digunakan untuk:
// 1. Deteksi hot keys (keys yang diakses sangat sering)
// 2. Monitoring cluster health
// 3. Trigger resharding jika diperlukan (opsional)
func main() {
	// Inisialisasi koneksi ke Redis cluster
	r := redisx.NewCluster()
	ctx := context.Background()

	// Ambil threshold untuk deteksi hot key dari environment variable
	// Hot key adalah key yang diakses lebih dari threshold kali per menit
	th := getInt("HOTKEY_THRESHOLD_PER_MIN", 2000)
	_ = th // TODO: implementasi deteksi hot key menggunakan threshold ini

	// Flag untuk enable/disable automatic resharding
	// Resharding adalah proses redistribusi data di cluster untuk balance load
	enableReshard := os.Getenv("ENABLE_RESHARD") == "1"

	// Loop utama: monitor cluster secara periodik
	tick := 0
	for {
		tick++
		// TODO: Implementasi deteksi hot keys
		// Contoh: baca top hot keys dari sorted set yang di-maintain oleh ingestor
		// keys, _ := r.ZRevRangeWithScores(ctx, "hotkeys:zset", 0, 20).Result()

		// Ambil informasi cluster untuk monitoring
		info, _ := r.ClusterInfo(ctx).Result()
		snippet := info
		if len(snippet) > 120 {
			snippet = snippet[:120] + "…"
		}
		if tick%4 == 0 {
			activity.Push(ctx, r, "hotkey-manager", "cluster_tick", snippet)
		}

		// Jika resharding enabled, trigger resharding jika diperlukan
		// Catatan: resharding biasanya dilakukan manual atau dengan tool khusus
		if enableReshard {
			// Placeholder: trigger reshard secara manual
			// Di production, ini bisa dilakukan dengan memanggil redis-cli --cluster reshard
			// atau menggunakan Redis management tool
		}

		// Sleep 30 detik sebelum check berikutnya
		time.Sleep(30 * time.Second)
	}
}

// getInt membaca integer dari environment variable dengan default value
func getInt(env string, def int) int {
	if s := os.Getenv(env); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}
