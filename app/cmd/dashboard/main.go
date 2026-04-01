package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"monolith-kv-sim/internal/activity"
	"monolith-kv-sim/internal/redisx"
)

func main() {
	_ = os.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)
	rdb := redisx.NewCluster()
	ctx := context.Background()

	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, pageHTML)
	})

	router.GET("/api/state", func(c *gin.Context) {
		feed, _ := rdb.LRange(ctx, activity.KeyFeed, 0, 99).Result()
		qLen, _ := rdb.LLen(ctx, "training_examples").Result()
		ingest, _ := rdb.Get(ctx, "stats:ingestor:ingest_total").Int64()
		js, ms, em := int64(0), int64(0), int64(0)
		if v, err := rdb.Get(ctx, "joiner:join_success_total").Int64(); err == nil {
			js = v
		}
		if v, err := rdb.Get(ctx, "joiner:join_miss_total").Int64(); err == nil {
			ms = v
		}
		if v, err := rdb.Get(ctx, "joiner:emitted_total").Int64(); err == nil {
			em = v
		}
		batches, _ := rdb.Get(ctx, "stats:materializer:batches_total").Int64()
		lastBatch, errLast := rdb.Get(ctx, "materializer:last_batch_json").Result()
		if errLast == redis.Nil {
			lastBatch = ""
		}
		memRatio, _ := redisx.ClusterMemRatio(ctx, rdb)

		var lastObj any
		if lastBatch != "" {
			_ = json.Unmarshal([]byte(lastBatch), &lastObj)
		}

		c.JSON(http.StatusOK, gin.H{
			"feed":                 feed,
			"queue_training_examples": qLen,
			"ingest_total":         ingest,
			"joiner": gin.H{
				"join_success_total": js,
				"join_miss_total":    ms,
				"emitted_total":      em,
			},
			"materializer_batches_total": batches,
			"materializer_last_batch":  lastObj,
			"redis_cluster_mem_ratio":    memRatio,
			"ts":                         time.Now().UTC().Format(time.RFC3339),
			// Penjelasan metrik vs Grafana (FilesTotal = seluruh namespace HDFS).
			"help_grafana_vs_dashboard": "Angka besar Materializer di sini = jumlah batch run (counter Redis), setara file JSONL yang ditulis materializer ke /derived. Panel Grafana namenode_FilesTotal menghitung SEMUA file di HDFS (overflow ingestor, offload per-key, derived, dll.) sehingga hampir selalu lebih besar.",
		})
	})

	router.GET("/health", func(c *gin.Context) {
		if err := rdb.Ping(ctx).Err(); err != nil {
			c.JSON(503, gin.H{"status": "down"})
			return
		}
		c.JSON(200, gin.H{"status": "ok"})
	})

	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}
	_ = router.Run(addr)
}

const pageHTML = `<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Derived data — live pipeline</title>
  <style>
    :root {
      --bg: #0f1419;
      --panel: #1a2332;
      --text: #e6edf3;
      --muted: #8b9cad;
      --accent: #3fb950;
      --warn: #d29922;
      --danger: #f85149;
      --border: #30363d;
      --joiner: #58a6ff;
      --ingest: #a371f7;
      --mat: #3fb950;
      --off: #f0883e;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: ui-sans-serif, system-ui, "Segoe UI", Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
    }
    header {
      padding: 1.25rem 1.5rem;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(180deg, #161b22 0%, var(--bg) 100%);
    }
    header h1 { margin: 0; font-size: 1.35rem; font-weight: 600; }
    header p { margin: 0.35rem 0 0; color: var(--muted); font-size: 0.9rem; max-width: 52rem; line-height: 1.45; }
    .meta { font-size: 0.75rem; color: var(--muted); margin-top: 0.5rem; }
    main { padding: 1rem 1.5rem 2rem; max-width: 1400px; margin: 0 auto; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 0.75rem;
      margin-bottom: 1rem;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 0.9rem 1rem;
    }
    .card h3 {
      margin: 0 0 0.5rem;
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--muted);
      font-weight: 600;
    }
    .card .big { font-size: 1.5rem; font-weight: 700; font-variant-numeric: tabular-nums; }
    .card.joiner h3 { color: var(--joiner); }
    .card.ingest h3 { color: var(--ingest); }
    .card.mat h3 { color: var(--mat); }
    .card.queue h3 { color: var(--warn); }
    .card.mem h3 { color: var(--muted); }
    .sub { font-size: 0.8rem; color: var(--muted); margin-top: 0.35rem; line-height: 1.4; }
    .feed-wrap {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      overflow: hidden;
    }
    .feed-wrap h2 {
      margin: 0;
      padding: 0.75rem 1rem;
      font-size: 0.85rem;
      border-bottom: 1px solid var(--border);
      color: var(--muted);
    }
    #feed {
      max-height: min(520px, 55vh);
      overflow-y: auto;
      font-family: ui-monospace, "Cascadia Code", monospace;
      font-size: 0.78rem;
      line-height: 1.45;
    }
    .row {
      padding: 0.45rem 1rem;
      border-bottom: 1px solid #21262d;
      display: grid;
      grid-template-columns: 88px 100px 1fr;
      gap: 0.5rem;
      align-items: baseline;
    }
    .row:hover { background: #21262d; }
    .svc { font-weight: 600; }
    .svc.ingestor { color: var(--ingest); }
    .svc.joiner { color: var(--joiner); }
    .svc.materializer { color: var(--mat); }
    .svc.offloader { color: var(--off); }
    .svc.hotkey-manager { color: #79c0ff; }
    .svc.generator { color: #ffa657; }
    .evt { color: var(--muted); }
    .detail { color: #c9d1d9; word-break: break-all; }
    .empty { padding: 2rem; text-align: center; color: var(--muted); }
    .legend {
      display: flex; flex-wrap: wrap; gap: 0.75rem;
      font-size: 0.75rem; color: var(--muted);
      margin-bottom: 0.75rem;
    }
    .legend span::before {
      content: ""; display: inline-block; width: 8px; height: 8px; border-radius: 2px;
      margin-right: 6px; vertical-align: middle;
    }
    .legend .l-ingest::before { background: var(--ingest); }
    .legend .l-join::before { background: var(--joiner); }
    .legend .l-mat::before { background: var(--mat); }
    .legend .l-off::before { background: var(--off); }
    .callout {
      font-size: 0.78rem;
      color: var(--muted);
      background: #161b22;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0.65rem 0.85rem;
      margin-bottom: 0.75rem;
      line-height: 1.45;
      max-width: 58rem;
    }
  </style>
</head>
<body>
  <header>
    <h1>Derived data — aktivitas pipeline (live)</h1>
    <p>
      Data turunan (training examples, file di HDFS) tidak terlihat langsung. Layar ini menampilkan
      alur <strong>ingestion</strong> (Part 2), <strong>join stream</strong>, <strong>materialization</strong> batch,
      dan <strong>offload</strong> Redis→HDFS lewat jejak peristiwa yang sama untuk semua layanan.
    </p>
    <div class="meta" id="clock">Memuat…</div>
  </header>
  <main>
    <div class="legend">
      <span class="l-ingest">Ingestor (KV / overflow)</span>
      <span class="l-join">Joiner (feature + action)</span>
      <span class="l-mat">Materializer (→ HDFS JSONL)</span>
      <span class="l-off">Offloader</span>
    </div>
    <p class="callout" id="metricHelp"></p>
    <div class="grid" id="cards"></div>
    <div class="feed-wrap">
      <h2>Feed aktivitas (terbaru di atas)</h2>
      <div id="feed"><div class="empty">Menghubungkan ke Redis…</div></div>
    </div>
  </main>
  <script>
    function svcClass(name) {
      const m = { ingestor: "ingestor", joiner: "joiner", materializer: "materializer", offloader: "offloader", "hotkey-manager": "hotkey-manager", generator: "generator" };
      return m[name] || "";
    }
    function renderCards(d) {
      const j = d.joiner || {};
      const last = d.materializer_last_batch || null;
      const lastStr = last ? JSON.stringify(last, null, 0) : "—";
      document.getElementById("cards").innerHTML = 
        '<div class="card joiner"><h3>Joiner</h3><div class="big">' + (j.emitted_total ?? 0) + '</div><div class="sub">emit · miss: ' + (j.join_miss_total ?? 0) + ' · ok: ' + (j.join_success_total ?? 0) + '</div></div>' +
        '<div class="card ingest"><h3>Ingestor</h3><div class="big">' + (d.ingest_total ?? 0) + '</div><div class="sub">event masuk (counter)</div></div>' +
        '<div class="card queue"><h3>Antrian derived</h3><div class="big">' + (d.queue_training_examples ?? 0) + '</div><div class="sub">Redis LIST <code>training_examples</code></div></div>' +
        '<div class="card mat"><h3>Materializer (batch runs)</h3><div class="big">' + (d.materializer_batches_total ?? 0) + '</div><div class="sub">Satu angka ≈ satu file part-*.jsonl di /derived. Bukan FilesTotal Grafana.</div><div class="sub">' + lastStr + '</div></div>' +
        '<div class="card mem"><h3>Redis cluster</h3><div class="big">' + ((d.redis_cluster_mem_ratio ?? 0) * 100).toFixed(1) + '%</div><div class="sub">perkiraan mem / max (aggregate)</div></div>';
    }
    function renderFeed(lines) {
      const el = document.getElementById("feed");
      if (!lines || !lines.length) {
        el.innerHTML = '<div class="empty">Belum ada peristiwa. Jalankan generator / kirim curl ke ingestor atau joiner.</div>';
        return;
      }
      el.innerHTML = lines.map(function(raw) {
        try {
          const o = JSON.parse(raw);
          const sc = svcClass(o.service);
          return '<div class="row"><span class="ts">' + (o.ts || "").replace("T", " ").slice(0, 19) + '</span><span class="svc ' + sc + '">' + (o.service || "?") + '</span><span><span class="evt">' + (o.event || "") + '</span> <span class="detail">' + escapeHtml(o.detail || "") + '</span></span></div>';
        } catch (e) {
          return '<div class="row"><span></span><span></span><span class="detail">' + escapeHtml(raw) + '</span></div>';
        }
      }).join("");
    }
    function escapeHtml(s) {
      const d = document.createElement("div");
      d.textContent = s;
      return d.innerHTML;
    }
    async function tick() {
      try {
        const r = await fetch("/api/state");
        const d = await r.json();
        document.getElementById("clock").textContent = "Update: " + (d.ts || "") + " · auto-refresh 1.2s";
        var h = document.getElementById("metricHelp");
        if (h && d.help_grafana_vs_dashboard) { h.textContent = d.help_grafana_vs_dashboard; }
        renderCards(d);
        renderFeed(d.feed);
      } catch (e) {
        document.getElementById("clock").textContent = "Error: " + e;
      }
    }
    tick();
    setInterval(tick, 1200);
  </script>
</body>
</html>
`
