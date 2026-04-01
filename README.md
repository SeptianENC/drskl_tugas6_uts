# Simulasi DDIA: Derived Data + Multi-Tier Storage

Repo ini adalah **lab Docker** yang menggabungkan dua tema dari *Designing Data-Intensive Applications*:

1. **DDIA Part II — penyimpanan terdistribusi (system of record untuk KV/event mentah)**  
   Redis Cluster + HDFS, cache-aside, offload ke disk.

2. **DDIA Part III — derived data**  
   Gabungan *stream* (HTTP) + *batch* (materialize ke file): feature + action → **training examples** → HDFS (JSONL), tanpa Kafka/Flink.

**Masalah yang diselesaikan:** data turunan (contoh latihan, file partisi di HDFS) **tidak terlihat** seperti tabel atau log user. Karena itu ada **Dashboard web** yang menampilkan aliran peristiwa dari semua layanan secara **live** (feed di Redis), plus angka agregat (join, ingest, antrian, materializer).

---

## Daftar isi

- [Ringkasan konsep](#ringkasan-konsep)
- [Dashboard live (wajib dibuka saat demo)](#dashboard-live-wajib-dibuka-saat-demo)
- [Arsitektur dua jalur](#arsitektur-dua-jalur)
- [Komponen](#komponen)
- [Prasyarat & port](#prasyarat--port)
- [Quick start](#quick-start)
- [API utama](#api-utama)
- [Konfigurasi](#konfigurasi)
- [Monitoring (Prometheus / Grafana)](#monitoring-prometheus--grafana)
- [Demo derived data (tiga skenario)](#demo-derived-data-tiga-skenario)
- [DDIA Part II — detail (KV, offload, konsistensi)](#ddia-part-ii--detail-kv-offload-konsistensi)
- [Akses Redis & HDFS](#akses-redis--hdfs)
- [Struktur repo & build](#struktur-repo--build)

---

## Ringkasan konsep

| Istilah | Di repo ini | Catatan |
|--------|-------------|--------|
| **System of record** | Event mentah: POST `/ingest` (ingestor), atau aliran `/feature` + `/action` (joiner) | Sumber kebenaran operasional. |
| **Derived data** | List Redis `training_examples` + file **JSONL** di HDFS `/derived/training_examples/...` | Hasil olahan; bisa dibuang dan dibuat ulang dari log/soR (dengan asumsi replay). |
| **Stream (simulasi)** | HTTP ke **joiner** | Bukan message broker; cukup untuk konsep join stateful + TTL. |
| **Batch** | **materializer** mem-drain list → file partisi waktu | Pola micro-batch sederhana. |

---

## Dashboard live (wajib dibuka saat demo)

| | |
|--|--|
| **URL** | **http://localhost:8887** |
| **Isi** | Kartu ringkas: joiner (emit / miss / sukses), panjang antrian `training_examples`, batch materializer, memori Redis cluster; **feed peristiwa** terbaru dari joiner, materializer, offloader, hotkey-manager (dan ingestor jika dipakai). |
| **Mekanisme** | Setiap layanan menulis baris JSON ke Redis LIST `system:activity_feed` (dipotong ~250 baris). Dashboard membaca Redis + beberapa counter (`stats:*`, `joiner:*`, dll.) lewat `GET /api/state`. |
| **API** | `GET /api/state` — JSON untuk UI; `GET /health` — liveness. |

Setelah `docker compose up -d`, buka browser ke **8887** sambil generator jalan: Anda akan melihat **feature_stored**, **training_example_emitted**, **materialized**, dan (jika diaktifkan) **ingest_ok** jika `ENABLE_LEGACY_INGESTOR=1`.

---

## Arsitektur dua jalur

```
                    ┌─────────────────────────────────────────┐
                    │  Generator (load test)                  │
                    │  → /feature + /action (joiner)          │
                    │  opsional → /ingest (ingestor, legacy)   │
                    └───────────────┬─────────────────────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         │ DDIA Part III (derived)  │  DDIA Part II (KV tier)   │
         ▼                          ▼                          │
   ┌──────────┐   LPUSH        ┌─────────────┐                │
   │ Joiner   │ ────────────► │ Redis LIST   │                │
   │ /feature │               │ training_    │                │
   │ /action  │               │ examples     │                │
   └──────────┘               └──────┬───────┘                │
                                   │ LRANGE+LTRIM             │
                                   ▼                          │
                            ┌─────────────┐                   │
                            │Materializer │                   │
                            │ → HDFS JSONL│                   │
                            └─────────────┘                   │
                                                              │
   ┌──────────┐         ┌──────────────┐        ┌──────────▼────────┐
   │ Ingestor │ ───────►│ Redis (keys) │◄───────│ Offloader        │
   │ /ingest  │         └──────┬───────┘        │ Redis → HDFS KV  │
   └──────────┘                │                └──────────────────┘
                               ▼
                         ┌───────────┐
                         │ HDFS      │
                         │ overflow  │
                         └───────────┘
```

---

## Komponen

| Layanan | Peran |
|--------|--------|
| **dashboard** | UI browser + `GET /api/state` — visibilitas pipeline. |
| **joiner** | Simpan feature (`feature:<request_id>`), terima action, join → `training_examples`; idempotensi `processed:<id>`; metrik `/stats`, `/metrics`. |
| **materializer** | Ambil batch dari list, tulis JSONL ke `/derived/training_examples/date=.../hour=.../min=.../`; fallback `/tmp/spool`; subcommand `replay`. |
| **ingestor** | Ingest KV ke Redis / overflow HDFS; GET cache-aside; `seed-old-keys` untuk uji offload. |
| **offloader** | Pindahkan key “tua” ke HDFS (`offloaded/`). |
| **generator** | Traffic: feature→delay→action ke joiner; opsional dual ke ingestor. |
| **hotkey-manager** | Heartbeat cluster (placeholder); feed setiap ~2 menit. |
| **Redis Cluster** | State + LIST derived + activity feed. |
| **HDFS** | Dataset materialized + overflow/offload. |
| **Prometheus / Grafana** | Metrik infrastruktur; Uptime Kuma opsional. |

---

## Prasyarat & port

- Docker + Docker Compose v2+
- Port umum: **8887** (dashboard), **8888** (ingestor), **8890** (joiner), 7001–7003 (Redis), 9870/9000 (HDFS NameNode), **9864–9866** (HDFS DataNode HTTP / WebHDFS redirect), 9090 (Prometheus), 3000 (Grafana), 3001 (Uptime Kuma), 9070 (hdfs-exporter), **9121–9123** (redis-exporter per node)

---

## Quick start

```bash
cd "Data Storage Apps"
docker compose build
docker compose up -d
```

Tunggu Redis cluster init + HDFS siap (~1–2 menit), lalu:

1. Buka **http://localhost:8887** — feed harus mulai bergerak jika generator hidup.  
2. Cek joiner: `curl -s http://localhost:8890/stats | jq`  
3. Cek file derived (dari host):  
   `docker compose exec materializer hdfs dfs -ls -R /derived/training_examples | head`

---

## API utama

### Joiner (`http://localhost:8890`)

- `POST /feature` — body: `request_id`, `user_id`, `video_id`, `ts`, `context` (opsional)  
- `POST /action` — body: `request_id`, `label`, `ts`  
- `GET /stats`, `GET /metrics`, `GET /health`

### Ingestor (`http://localhost:8888`)

- `POST /ingest`, `GET /get/*key`, `GET|POST /seed-old-keys?count=N`, `GET /health`

### Materializer (di dalam container)

- `docker compose exec materializer /app/materializer replay -date YYYY-MM-DD -hour HH [-minute MM]`

### Dashboard (`http://localhost:8887`)

- `GET /` — halaman UI  
- `GET /api/state` — snapshot JSON untuk polling  

---

## Konfigurasi

Variabel penting (set di `docker-compose.yml`):

| Layanan | Variabel | Keterangan |
|--------|----------|------------|
| Banyak | `REDIS_STARTUP_NODES` | `redis-1:7001,redis-2:7002,redis-3:7003` |
| Joiner | `FEATURE_TTL_SECONDS`, `PROCESSED_TTL_SECONDS` | TTL feature & idempotensi emit |
| Materializer | `HDFS_PATH`, `MATERIALIZE_INTERVAL_SECONDS`, `MATERIALIZE_BATCH_SIZE` | Prefix `/derived`, interval drain |
| Ingestor | `HDFS_PATH`, `REDIS_MAXMEM_SOFT` | Overflow Part II |
| Generator | `JOINER_URL`, `ENABLE_LEGACY_INGESTOR`, `RPS`, `HOTKEY_RATIO` | Traffic; `1` = juga kirim ke ingestor |
| Offloader | `OFFLOAD_*`, `HDFS_PATH` | Umur key & interval offload |

**Build image dengan klien HDFS:** Hadoop 3.2.1 disalin dari image `bde2020/hadoop-base` (satu versi dengan stack HDFS), tanpa `wget` ke mirror Apache.

---

## Monitoring (Prometheus / Grafana)

- Prometheus: **http://localhost:9090** — pastikan target `redis_exporter_redis1/2/3`, `joiner`, `hdfs` UP.  
- Grafana: **http://localhost:3000** (admin/admin) — dashboard Redis/HDFS (variabel job Redis kini per-node).  
- Dashboard **bukan** pengganti Prometheus: ia untuk **narasi operasional** (apa yang baru terjadi), bukan SLO detik-per-detik.

---

## Demo derived data (tiga skenario)

1. **Join sukses (cepat)** — default generator (70% delay 0–2 detik): `join_success_total` & `emitted_total` naik; feed menampilkan `training_example_emitted` & `materialized`.  
2. **Late masih join** — naikkan `FEATURE_TTL_SECONDS=600`; action dengan jeda 10–30 detik masih sering ter-join.  
3. **Join miss** — turunkan `FEATURE_TTL_SECONDS=30`; action dengan jeda 60–120 detik sering `join_miss` (feed + counter).

---

## DDIA Part II — detail (KV, offload, konsistensi)

**Replikasi & partisi:** Redis Cluster tiga master (hash slot); HDFS replikasi blok.  
**Konsistensi read path:** Ingestor: LRU lokal → Redis → HDFS (`ReadByKey` untuk key yang sudah di-offload).  
**Offload:** Offloader men-scan key; jika `_ts` melewati ambang atau memori tinggi, tulis ke `/events_overflow/offloaded/` dan hapus dari Redis.

### Contoh curl ingest & baca

```bash
curl -X POST http://localhost:8888/ingest \
  -H "Content-Type: application/json" \
  -d '{"key":"test:1","value":{"user_id":1,"video_id":10},"ttl_sec":3600,"cache_hint":"none"}'

curl http://localhost:8888/get/test:1
```

### Uji offload deterministik

```bash
curl "http://localhost:8888/seed-old-keys?count=20"
docker compose logs -f offloader
# Setelah offload:
curl "http://localhost:8888/get/seed:old:0"
docker compose exec offloader hdfs dfs -ls /events_overflow/offloaded
```

Lebih banyak contoh (fault injection HDFS, Uptime Kuma) dapat diadaptasi dari versi sebelumnya; prinsipnya sama: **Part II = KV + tiering**, **Part III = derived pipeline** di atas Redis/HDFS.

---

## Akses Redis & HDFS

- Redis dari host: `redis-cli -c -h localhost -p 7001`  
- HDFS UI: **http://localhost:9870** — browse `/derived` dan `/events_overflow`.  
- **Unduh file dari UI (WebHDFS) di server publik:** NameNode hanya memicu alur; browser lalu diarahkan ke **DataNode** (port HTTP, biasanya **9864 / 9865 / 9866**). Tanpa konfigurasi tambahan, redirect memakai **hostname container Docker** (bukan domain Anda), sehingga unduhan gagal. Untuk akses dari internet, set di **`.env`** di direktori yang sama dengan `docker-compose.yml`:  
  `HDFS_PUBLIC_HOST=senc.my.id`  
  (ganti dengan domain atau IP host yang bisa di-resolve dari browser). Buka firewall untuk **9870**, **9000**, dan **9864–9866** jika perlu. Alternatif: unduh dari mesin yang punya jaringan ke cluster, mis. `docker exec -it namenode hdfs dfs -get /derived/... /tmp/`.  
- Detail CLI: lihat juga `REDIS_CLUSTER_ACCESS.md` jika ada di repo.

---

## Struktur repo & build

```
Data Storage Apps/
├── docker-compose.yml
├── prometheus/prometheus.yml
├── grafana/...
├── README.md
└── app/
    ├── Dockerfile          # target: ingestor, joiner, materializer, dashboard, ...
    ├── cmd/
    │   ├── ingestor/
    │   ├── joiner/
    │   ├── materializer/
    │   ├── dashboard/      # UI live pipeline
    │   ├── generator/
    │   ├── offloader/
    │   └── hotkey-manager/
    └── internal/
        ├── activity/       # feed Redis system:activity_feed
        ├── hdfsx/
        ├── redisx/
        └── cachex/
```

---

## FAQ: lambat vs cepat, Grafana vs dashboard

**Mengapa dulu throughput sangat rendah meski `RPS` besar?**  
Generator lama menjalankan **satu pasangan** feature→action **secara berurutan** dan menunggu jeda (sampai puluhan detik) di antara keduanya. Jadi `RPS` hanya mengatur jeda *setelah* satu siklus panjang—**bukan** ratusan pasangan per detik.  
**Sekarang (mode cepat):** generator memakai **ticker + goroutine**: setiap slot `1/RPS` detik sebuah pasangan baru dimulai **parallel** (jeda feature→action hanya ~0–35 ms). Target throughput mendekati **`RPS` pasangan/detik** (mis. `RPS=100`). Mode **slow motion** dari tombol dashboard memakai satu pasangan demi satu dengan jeda panjang agar mudah dipantau.

**Tombol Slow motion (dashboard :8887)**  
Menyimpan flag Redis `pipeline:slow_motion`. **ON** = generator mengabaikan ticker cepat dan memproses satu pasangan per waktu dengan jeda DDIA. **OFF** = kembali ke throughput concurrent.

**Kenapa angka Materializer di dashboard (8887) ≠ “files” di Grafana HDFS?**  
- Dashboard menampilkan **`stats:materializer:batches_total`**: berapa kali materializer menulis satu batch (≈ satu file `part-*.jsonl` di `/derived/...`).  
- Grafana memakai metrik **`namenode_FilesTotal`**: **jumlah file di seluruh HDFS**, termasuk `/events_overflow` (overflow ingestor), `/events_overflow/offloaded/` (satu file per key offload), plus file di `/derived`. Jadi **FilesTotal hampir selalu lebih besar** dari counter batch materializer saja.

---

## Lisensi & referensi

- Konsep DDIA: Martin Kleppmann, *Designing Data-Intensive Applications*.  
- Redis, HDFS, Grafana: dokumentasi resmi masing-masing.
