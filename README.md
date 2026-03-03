# DDIA Part 2 — Simulasi Sistem Data Terdistribusi

Repo ini mensimulasikan konsep utama dari **Designing Data-Intensive Applications (DDIA), Part II** tentang bagaimana sistem data terdistribusi bekerja:

- **Replication**: Redis Cluster dengan master–replica + HDFS dengan beberapa DataNode.
- **Partitioning & Routing**: Redis Cluster (hash slots) + hot/cold keys yang tersebar ke beberapa shard.
- **Consistency**: cache-aside Redis → HDFS dengan offloader + GET yang membaca dari Redis atau HDFS.
- **Rebalancing**: operasi `CLUSTER REBALANCE` + penambahan node replica.
- **Permasalahan data terdistribusi**:
  - **Unreliable / async network**: timeout + retry + fallback ke HDFS.
  - **Fault detection**: kombinasi log (offloader, ingestor), metric (Prometheus/Grafana), dan health check (Uptime Kuma).

**Tech stack:** Go (Gin), Redis Cluster (3+2 node), HDFS (1 Namenode + beberapa Datanode), Prometheus, Grafana, Uptime Kuma.

---

## Daftar Isi

- [Mapping Konsep DDIA Part 2](#mapping-konsep-ddia-part-2)
- [Arsitektur](#arsitektur)
- [Prasyarat](#prasyarat)
- [Quick Start](#quick-start)
- [Service dan Port](#service-dan-port)
- [Cara Penggunaan](#cara-penggunaan)
- [Konfigurasi](#konfigurasi)
- [Monitoring](#monitoring)
- [Simulasi Konsep DDIA Part 2](#simulasi-konsep-ddia-part-2-replication-partitioning-consistency-fault)
- [Akses Redis Cluster](#akses-redis-cluster)
- [Akses HDFS](#akses-hdfs)
- [Struktur Project](#struktur-project)

---

## Mapping Konsep DDIA Part 2

Secara ringkas, implementasi ini memetakan konsep DDIA Part 2 sebagai berikut:

- **Replication**
  - Redis: master–replica melalui `docker-compose-replica.yml` (`redis-4`, `redis-5` sebagai slave).
  - HDFS: beberapa DataNode dengan `dfs.replication=2` (blok file direplikasi ke beberapa node).
- **Partitioning & Routing**
  - Redis Cluster: 3 master (`redis-1`, `redis-2`, `redis-3`) membagi key-space menggunakan **hash slots**.
  - Client (Ingestor) memakai `redis.ClusterClient` → routing key → shard otomatis.
- **Consistency**
  - Cache-aside: Ingestor menulis ke Redis, offloader memindahkan data lama ke HDFS.
  - Read path `/get/<key>`: `local LRU → Redis → HDFS`, sehingga data tetap bisa dibaca walau sudah di-offload.
  - Idempotensi di `/ingest` dengan `request_id` + marker di Redis.
- **Rebalancing**
  - Bisa didemokan dengan `redis-cli --cluster rebalance`, serta penambahan replica via `redis-cluster-replica-init`.
- **Permasalahan data terdistribusi**
  - **Unreliable / async network**: Ingestor punya timeout + retry ke Redis, lalu fallback ke HDFS.
  - **Fault detection**:
    - Log: offloader (`offload run`, `offload write failed`), ingestor.
    - Metrics: Prometheus + Grafana (Redis/HDFS dashboards).
    - Health check: Uptime Kuma memantau `/health` dan service lain, lalu relay ke Telegram/Slack.

Bagian-bagian berikut menjelaskan detail arsitektur dan cara mensimulasikan tiap konsep di atas.

---

## Arsitektur

Alur data mengikuti pola **cache-aside** + **multi-tier storage** dengan overflow ke disk:

```
  [Log Kafka / Feature Kafka]  (simulasi: Generator)
              │
              ▼
      ┌───────────────┐
      │ In-Memory     │  ← Redis Cluster (3 node, maxmemory 50MB/node)
      │ Cache         │
      └───────┬───────┘
              │ Found in Cache → [Join] → [Negative Sampling] → Training Example Kafka
              │
              │ Cache miss / Redis penuh
              ▼
      ┌───────────────┐
      │ On-Disk       │  ← HDFS (1 Namenode + beberapa Datanode, simulasi)
      │ KV-Store      │
      └───────┬───────┘
              │ Read from KV-Store → [Join] → ...
```

- **Ingestor (Go/Gin):** Menerima event via HTTP; simpan ke Redis selama memori di bawah threshold, selain itu tulis ke HDFS.
- **Redis Cluster:** 3 master node (ditambah replica via `docker-compose-replica.yml`), masing-masing `maxmemory 50mb`, policy `allkeys-lru` — sengaja kecil agar mudah terisi dan memicu overflow ke HDFS. Redis Cluster mengimplementasikan **partitioning + replication**.
- **HDFS:** 1 Namenode + beberapa Datanode (simulasi, storage kecil). Menyimpan event overflow dalam format JSONL di path yang dikonfigurasi (default `/events_overflow`). Replikasi dilakukan di level blok file (`dfs.replication=2`).
- **Generator:** Mensimulasikan traffic (user actions/features) dengan mix hot/cold keys ke Ingestor.
- **Hotkey-manager:** Service pemantauan hot keys di cluster (placeholder untuk perluasan).
- **Offloader:** Secara periodik memindahkan data yang sudah **terlalu lama** di Redis ke HDFS (on-disk KV store) agar in-memory cache tidak penuh. Sesuai diagram monolith: data di cache yang tidak lagi “segar” di-offload ke KV-store; saat **GET**, jika key tidak ada di Redis, dibaca dari HDFS.

### Skenario: Offload data lama (Redis → HDFS)

1. **Ingestor** menyimpan setiap event ke Redis dengan field `_ts` (timestamp) di value, plus optional `request_id` untuk idempotensi.
2. **Offloader** (service terpisah) setiap `OFFLOAD_INTERVAL_SECONDS` melakukan **SCAN** key per shard Redis. Untuk tiap key, jika umur data (`_ts`) lebih dari **OFFLOAD_AFTER_SECONDS** atau cluster dalam mode agresif (memori tinggi), value ditulis ke HDFS di path `/events_overflow/offloaded/<key>.json` lalu key di-**DEL** dari Redis.
3. **GET** di Ingestor: jika key **ditemukan di local LRU** → return `source: "local_cache"`; jika tidak, coba Redis (`source: "redis"`); jika tidak ada di Redis → baca dari HDFS (`ReadByKey`, `source: "hdfs"`).

Dengan ini, data yang “terlalu lama” di cache pindah ke on-disk KV store dan cache tidak penuh; lookup tetap lengkap lewat Redis + HDFS, mencerminkan pola **multi-tier storage + eventual consistency** yang dibahas di DDIA.

---

## Prasyarat

- **Docker** dan **Docker Compose** (v2+)
- **Git** (opsional, untuk clone)
- Port yang tidak bentrok: 3000, 7001–7003, 8080, 9090, 9070, 9121, 9870, 9000, 9864, 9865, 9866

---

## Quick Start

### 1. Clone / masuk ke folder project

```bash
cd "Data Storage Apps"
```

### 2. Build dan jalankan semua service

```bash
docker compose build
docker compose up -d
```

### 3. Tunggu inisialisasi

- Redis cluster: ~10–20 detik (healthcheck + `redis-cluster-init`).
- HDFS: ~30–60 detik (Namenode + 3 Datanode).
- Setelah itu Ingestor, Generator, Prometheus, dan Grafana akan berjalan.

### 4. Cek status

```bash
docker compose ps
```

Semua service seharusnya berstatus **running** (kecuali `redis-cluster-init` yang selesai sekali jalan).

### 5. Uji API

```bash
# Kirim satu event
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"key":"test:1","value":{"user_id":1,"video_id":10},"ttl_sec":3600,"cache_hint":"none"}'

# Baca kembali
curl http://localhost:8080/get/test:1
```

---

## Service dan Port

| Service              | Port (host)     | Fungsi |
|----------------------|-----------------|--------|
| **ingestor**         | 8080            | API HTTP: ingest & get (cache-aside + overflow HDFS) |
| **redis-1, 2, 3**    | 7001, 7002, 7003 | Redis Cluster (in-memory cache) |
| **namenode**         | 9870 (Web UI), 9000 (HDFS) | HDFS Namenode |
| **datanode**         | 9864 (Web UI)   | HDFS Datanode 1 |
| **datanode-2**       | 9865 (Web UI)   | HDFS Datanode 2 |
| **datanode-3**       | 9866 (Web UI)   | HDFS Datanode 3 |
| **redis-exporter**   | 9121            | Metrics Redis untuk Prometheus |
| **hdfs-exporter**    | 9070            | Metrics HDFS (Namenode JMX) untuk Prometheus |
| **prometheus**       | 9090            | Scrape & simpan metrics |
| **grafana**          | 3000            | Dashboard (Redis, HDFS) |
| **uptime-kuma**      | 3001            | Synthetic monitoring & alert relay (HTTP, ping, dsb.) |

*Generator*, *hotkey-manager*, dan *offloader* tidak expose port; mereka berkomunikasi lewat jaringan internal Docker.

---

## Cara Penggunaan

### 1. API Ingestor (HTTP)

Base URL (dari host): **http://localhost:8080**

#### POST `/ingest` — Menyimpan event

Request body (JSON):

| Field        | Tipe    | Wajib | Keterangan |
|-------------|---------|--------|------------|
| `key`       | string  | Ya     | Identifier unik (mis. `feature:user:123`) |
| `value`     | object  | Ya     | Payload bebas (user_id, video_id, watch_time, dll) |
| `ttl_sec`   | number  | Tidak  | TTL di Redis (detik). Default: 3600 |
| `cache_hint`| string  | Tidak  | `"hot_read"` = prioritaskan di local LRU cache |
| `request_id`| string  | Tidak  | Idempotency key opsional. Jika dikirim sama berulang, server akan menghindari double-processing selama marker masih ada di Redis |

Contoh:

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "key": "feature:user:1001",
    "value": {
      "user_id": 1001,
      "video_id": 5001,
      "ts": 1234567890.5,
      "watch_time": 15.2
    },
    "ttl_sec": 3600,
    "cache_hint": "none"
  }'
```

Response sukses (disimpan di Redis):

```json
{"ok": true, "stored": "redis", "mem_ratio": 0.45}
```

Response sukses (overflow ke HDFS karena memori penuh):

```json
{"ok": true, "stored": "hdfs", "mem_ratio": 0.82}
```

#### GET `/get/<key>` — Membaca nilai berdasarkan key

- Cache-aside: cek local LRU → Redis → jika tidak ada di Redis, **baca dari HDFS** (on-disk KV store, data yang sudah di-offload). Jika tidak ada di kedua tempat, 404.
- Key di URL tanpa leading slash: `/get/mykey` atau `/get/feature:user:1001`.

Contoh:

```bash
curl http://localhost:8080/get/feature:user:1001
```

Response (dari Redis):

```json
{"ok": true, "source": "redis", "value": "{\"user_id\":1001,\"video_id\":5001,...}"}
```

Response (dari local cache):

```json
{"ok": true, "source": "local_cache", "value": "..."}
```

Response (dari HDFS — key sudah di-offload dari Redis):

```json
{"ok": true, "source": "hdfs", "value": "{\"user_id\":1001,\"_ts\":1234567890,...}"}
```

#### GET `/health` — Heartbeat service Ingestor

- Endpoint sederhana untuk **heartbeat** (Uptime Kuma, k8s liveness/readiness, dsb.).

```bash
curl http://localhost:8080/health
```

Response:

```json
{"status": "ok"}
```

### 2. Generator (simulasi traffic)

Generator otomatis mengirim event ke Ingestor dengan:

- **RPS** (request per detik): env `RPS` (default 200).
- **Hot key ratio**: env `HOTKEY_RATIO` (default 0.2) — sebagian request pakai key yang sama berulang (hot), sisanya key acak (cold).

Tidak perlu dipanggil manual; cukup pastikan service `generator` jalan (`docker compose up -d`). **Generator dan Ingestor dirancang jalan terus (tanpa batas waktu)**; kalau container berhenti, biasanya proses sempat crash (cek log). Di `docker-compose` sudah diset `restart: unless-stopped` agar keduanya (dan offloader, hotkey-manager) otomatis hidup lagi setelah crash. Untuk mengubah beban, edit env di `docker-compose.yml` (bagian `generator`) lalu `docker compose up -d` lagi.

### 3. Hotkey-manager

Service ini memantau cluster (mis. `CLUSTER INFO`) dan placeholder untuk logika hot-key / reshard. Tidak ada API; hanya background loop. Konfigurasi lewat env (lihat [Konfigurasi](#konfigurasi)).

---

## Konfigurasi

Konfigurasi utama lewat **environment variables** di `docker-compose.yml`.

### Ingestor

| Variable              | Default (contoh) | Keterangan |
|-----------------------|-------------------|------------|
| `REDIS_STARTUP_NODES` | redis-1:7001,...  | Daftar node Redis Cluster |
| `HDFS_PATH`           | /events_overflow  | Path HDFS untuk event overflow |
| `REDIS_MAXMEM_SOFT`   | 0.80              | Threshold rasio memori (0–1). Di atas ini, tulis ke HDFS |
| `LOCAL_CACHE_HOTKEYS` | 1                 | 1 = aktifkan local LRU cache untuk hot keys |

### Generator

| Variable       | Default | Keterangan |
|----------------|--------|------------|
| `INGESTOR_URL` | http://ingestor:8080 | URL Ingestor |
| `RPS`          | 200    | Request per detik |
| `HOTKEY_RATIO` | 0.20   | Rasio request yang pakai hot key (0–1) |

### Offloader

| Variable                  | Default | Keterangan |
|---------------------------|--------|------------|
| `REDIS_STARTUP_NODES`     | redis-1:7001,... | Daftar node Redis Cluster |
| `HDFS_PATH`               | /events_overflow  | Path HDFS (offloaded data di subdir `offloaded/`) |
| `OFFLOAD_AFTER_SECONDS`   | 300    | Data di Redis yang lebih lama dari ini (detik) akan dipindah ke HDFS |
| `OFFLOAD_INTERVAL_SECONDS`| 60     | Interval (detik) jalannya proses offload |
| `OFFLOAD_FORCE_MEM_RATIO` | 0.70   | Jika rasio memori cluster >= nilai ini, offloader masuk mode agresif |
| `OFFLOAD_FORCE_MIN_AGE_SECONDS` | 5 | Saat mode agresif aktif, hanya key dengan umur minimal ini yang dipindah |

### Hotkey-manager

| Variable                 | Default | Keterangan |
|--------------------------|--------|------------|
| `REDIS_STARTUP_NODES`    | ...    | Sama seperti Ingestor |
| `HOTKEY_THRESHOLD_PER_MIN` | 2000 | Batas hit per menit untuk dianggap hot (untuk perluasan) |
| `ENABLE_RESHARD`         | 0      | 1 = enable placeholder reshard |

### Redis (per node)

Limit memori dan policy di-set di `docker-compose` (command `redis-server`):

- `--maxmemory 50mb` — sengaja kecil agar mudah overflow ke HDFS.
- `--maxmemory-policy allkeys-lru` — evict key yang paling jarang dipakai.

---

## Monitoring

**Troubleshooting metrics tidak tampil di Grafana:** ikuti panduan step-by-step di **[MONITORING_TROUBLESHOOTING.md](./MONITORING_TROUBLESHOOTING.md)** — cek dulu metrics di **Exporter** (curl), lalu **Prometheus** (targets & query), baru **Grafana** (datasource & dashboard).

### Grafana

- URL: **http://localhost:3000**
- Login: **admin** / **admin** (disarankan ganti password setelah pertama kali).

Dashboard yang di-provision:

1. **Redis - In-Memory Cache**  
   Performance, memory, evictions, connected clients, keys per DB, hit ratio, dll. Variable **Instance** bisa dipilih per node Redis atau "All".

2. **HDFS - On-Disk KV Store**  
   Capacity (total/used/remaining), blocks total, files total, corrupt blocks, stale datanodes, namenode active.

Datasource **Prometheus** sudah di-provision dan dipakai sebagai default.

### Prometheus

- URL: **http://localhost:9090**
- Menu **Status → Targets**: cek bahwa target Redis (`redis_exporter_targets`) dan HDFS (`hdfs`) status **UP**.
- Menu **Graph**: bisa cek metric, mis. `redis_memory_used_bytes`, `namenode_CapacityUsed`.

### Ringkasan akses

| Apa              | URL |
|------------------|-----|
| Grafana          | http://localhost:3000 |
| Uptime Kuma      | http://localhost:3001 |
| Prometheus       | http://localhost:9090 |
| HDFS Namenode UI | http://localhost:9870 |

---

## Simulasi Konsep DDIA Part 2 (Replication, Partitioning, Consistency, Fault)

Bagian ini menghubungkan implementasi dengan konsep di buku **Designing Data-Intensive Applications (DDIA), Part II**.

### 1. Partitioning & Routing (Redis Cluster + hot/cold keys)

**Tujuan:** menunjukkan bagaimana key dibagi ke beberapa shard dan client tidak perlu tahu node mana yang menyimpan data.

1. Lihat pembagian slot cluster:

   ```bash
   docker compose exec redis-1 redis-cli -p 7001 CLUSTER SLOTS
   ```

2. Lihat hash-slot untuk satu hot key:

   ```bash
   docker compose exec redis-1 redis-cli -p 7001 CLUSTER KEYSLOT feature:HOT:0
   ```

3. Di Grafana (dashboard Redis), lihat panel **Redis used memory per node** (`redis_memory_used_bytes` per instance) untuk melihat node mana yang lebih “panas” karena hot keys.

**Kaitannya dengan DDIA:** Redis Cluster memakai **hash partitioning** (slot → node). Client (Ingestor) hanya kirim key, routing shard ditangani oleh driver dan cluster.

### 2. Consistency & Data Movement (Redis → HDFS via Offloader)

**Tujuan:** menunjukkan data yang “terlalu lama” berpindah dari cache ke on-disk KV store dan read path yang tetap konsisten.

1. Seed data lama ke Redis dengan `_ts` di masa lalu:

   ```bash
   curl "http://localhost:8080/seed-old-keys?count=20"
   ```

2. Baca salah satu key sebelum offload:

   ```bash
   curl "http://localhost:8080/get/seed:old:0"
   # → {"ok":true,"source":"redis",...}
   ```

3. Pantau offloader:

   ```bash
   docker compose logs -f offloader
   ```

   Tunggu sampai muncul baris:

   ```text
   offload run: scanned=... old=20 moved=20 ...
   ```

4. Baca lagi key yang sama:

   ```bash
   curl "http://localhost:8080/get/seed:old:0"
   # → {"ok":true,"source":"hdfs",...}
   ```

5. Verifikasi di HDFS:

   ```bash
   docker compose exec offloader hdfs dfs -ls /events_overflow/offloaded
   docker compose exec offloader hdfs dfs -cat /events_overflow/offloaded/<nama_file>.json
   ```

**Kaitannya dengan DDIA:** ini contoh **multi-tier storage + eventual consistency**. Data baru ada di Redis, lalu dipindah ke HDFS berdasarkan umur/memori. Read path (`/get`) selalu mencari di Redis dulu lalu HDFS, sehingga dari sudut pandang klien data tetap konsisten.

### 3. Fault & Partial Failure (HDFS down, Redis up)

**Tujuan:** menunjukkan bagaimana gangguan di satu komponen (HDFS) tidak langsung mematikan keseluruhan sistem, tapi terlihat di metric/log.

1. Matikan Namenode (simulasi HDFS down):

   ```bash
   docker compose stop namenode
   ```

2. Seed lagi beberapa key:

   ```bash
   curl "http://localhost:8080/seed-old-keys?count=5"
   ```

3. Pantau log offloader:

   ```bash
   docker compose logs -f offloader
   ```

   Akan terlihat error seperti:

   ```text
   offload write failed key="seed:old:0": ...
   offload run: ... moved=0 write_fail>0 ...
   ```

4. Di Grafana, panel Redis tetap normal (cluster hidup), sedangkan panel HDFS (capacity, FilesTotal) tidak bergerak.

**Kaitannya dengan DDIA:** contoh **partial failure** di sistem terdistribusi — satu komponen (HDFS) gagal, sementara komponen lain (Redis) tetap melayani request. Deteksinya membutuhkan kombinasi log + metrics.

### 4. Service Health & Alerting (Uptime Kuma + `/health`)

**Tujuan:** mensimulasikan health check dan routing notifikasi ke Telegram/Slack.

1. Pastikan Uptime Kuma jalan:

   ```bash
   docker compose up -d uptime-kuma
   ```

2. Buka `http://localhost:3001`, buat akun admin.

3. Tambah monitor baru:

   - **Type**: HTTP(s)
   - **Friendly Name**: `Ingestor /health`
   - **URL** (dari container Kuma): `http://ingestor:8080/health`
   - Interval: 30 detik.

4. (Opsional) Tambah notifikasi Telegram/Slack di menu **Settings → Notifications**, lalu hubungkan ke monitor tersebut.

5. Untuk demo, matikan Ingestor:

   ```bash
   docker compose stop ingestor
   ```

   Di UI Kuma, status monitor berubah menjadi **DOWN** dan notifikasi dikirim ke channel yang Anda hubungkan.

**Kaitannya dengan DDIA:** meskipun Kuma sendiri bukan bagian dari data path, ia membantu **observability** dan deteksi otomatis terhadap failure, yang sangat ditekankan di bagian operasional DDIA (monitoring, alerting, SLO).

## Akses Redis Cluster

Setup menggunakan **Redis Cluster** dengan 3 master node (tanpa replica). Untuk detail koneksi, CLI, dan contoh kode (Go/Python/Node), lihat:

**[REDIS_CLUSTER_ACCESS.md](./REDIS_CLUSTER_ACCESS.md)**

Ringkasan singkat:

- Dari host: `redis-cli -c -h localhost -p 7001` (flag `-c` wajib untuk cluster mode).
- Dari dalam Docker: `docker compose exec redis-1 redis-cli -c -p 7001`.
- Lihat master dan slot: `CLUSTER NODES`, `CLUSTER INFO`.

Aplikasi Go sudah memakai `redis.ClusterClient`; tidak perlu konfigurasi khusus di sisi aplikasi selain `REDIS_STARTUP_NODES`.

---

## Akses HDFS

- **Web UI Namenode:** http://localhost:9870  
  - Browse file, cek status cluster, storage, daftar Datanode (3 node).
- **Web UI Datanode:** http://localhost:9864 (datanode 1), http://localhost:9865 (datanode 2), http://localhost:9866 (datanode 3).
- **Path default event overflow:** `/events_overflow`  
  - File JSONL hasil overflow dari Ingestor.
- **Path data hasil offloader (Redis → HDFS):** `/events_overflow/offloaded/`  
  - Satu file per key (nama file encoding key), bisa dibuka di Web UI atau lewat CLI.
- Dari **container yang punya `hdfs` CLI** (mis. ingestor atau offloader):

  ```bash
  docker compose exec ingestor hdfs dfs -ls /events_overflow
  docker compose exec ingestor hdfs dfs -cat /events_overflow/overflow_*.jsonl | head -5
  # Daftar file yang sudah di-offload dari Redis (bukti offloader sudah jalan):
  docker compose exec offloader hdfs dfs -ls /events_overflow/offloaded
  docker compose exec offloader hdfs dfs -cat /events_overflow/offloaded/<nama_file>.json
  ```

### Memverifikasi Offloader dan file offload di HDFS

**1. Cek apakah offloader berjalan**

```bash
docker compose ps offloader
```

Status harus **Up**. Lihat log (setiap interval akan ada baris jika ada key yang dipindah):

```bash
docker compose logs -f offloader
```

- Saat start: `offloader started: OFFLOAD_AFTER_SECONDS=..., OFFLOAD_INTERVAL_SECONDS=..., OFFLOAD_FORCE_MEM_RATIO=..., OFFLOAD_FORCE_MIN_AGE_SECONDS=..., HDFS_PATH=...`
- Setiap interval: `offload run: scanned=... old=... moved=... write_fail=... parse_fail=... mem_ratio=... force_by_mem=...`

**2. Lihat file offload di HDFS (dashboard / UI)**

- Buka **HDFS Web UI:** http://localhost:9870  
- Klik **Utilities** → **Browse the file system**.  
- Masuk ke path: **`/events_overflow`** → folder **`offloaded`**.  
- Di dalam `offloaded` akan terlihat file-file `.json` (satu file per key yang sudah dipindah). Klik nama file → **Open** atau **Download** untuk melihat isi (JSON dengan field `_ts`, dll).

Ini bisa Anda gunakan sebagai **bukti dalam simulasi**: sebelum offload, key ada di Redis; setelah offloader jalan (data > OFFLOAD_AFTER_SECONDS), key hilang dari Redis dan file muncul di `/events_overflow/offloaded/` di HDFS.

**3. Via CLI (tanpa buka browser)**

```bash
docker compose exec offloader hdfs dfs -ls /events_overflow/offloaded
```

Jika offloader sudah pernah memindahkan data, daftar file akan muncul. Untuk isi satu file:

```bash
docker compose exec offloader hdfs dfs -cat /events_overflow/offloaded/<nama_file>.json
```

---

## Struktur Project

```
Data Storage Apps/
├── README.md                         # Dokumen ini
├── REDIS_CLUSTER_ACCESS.md          # Akses Redis cluster (CLI, kode, troubleshooting)
├── MONITORING_TROUBLESHOOTING.md    # Troubleshooting metrics → Prometheus → Grafana (step-by-step)
├── docker-compose.yml               # Definisi semua service (Redis Cluster, HDFS, app, monitoring, uptime-kuma)
├── prometheus/
│   └── prometheus.yml          # Scrape config (Redis, HDFS, Prometheus)
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── datasources.yml # Datasource Prometheus
│       └── dashboards/
│           ├── dashboards.yml  # Provider dashboard
│           └── json/
│               ├── redis_official.json # Dashboard Redis (resmi, dimodifikasi untuk cluster)
│               ├── 14615_rev1.json     # Dashboard Redis (varian lain untuk cluster)
│               └── hdfs.json           # Dashboard HDFS
└── app/
    ├── Dockerfile              # Multi-stage build (ingestor, generator, hotkey-manager, offloader)
    ├── go.mod / go.sum
    ├── cmd/
    │   ├── ingestor/           # API HTTP + cache-aside + overflow HDFS + GET fallback dari HDFS + /health + /seed-old-keys
    │   ├── generator/          # Simulasi traffic (hot/cold keys) ke Ingestor
    │   ├── hotkey-manager/     # Pemantauan cluster (placeholder deteksi hot keys / reshard)
    │   └── offloader/          # Worker: pindahkan data lama dari Redis ke HDFS (per key), log statistik
    └── internal/
        ├── cachex/             # LRU cache (hot keys) di sisi Ingestor
        ├── hdfsx/              # Writer/reader HDFS (JSONL + offloaded KV per key)
        ├── redisx/             # Redis Cluster client + utilitas (ClusterMemRatio, dsb.)
        └── metrics/            # (placeholder) paket untuk metric internal (bila ingin menambah Prometheus client / custom metrics)
```

---

## Lisensi & Referensi

- Arsitektur data storage diadaptasi dari konsep pipeline monolith sistem rekomendasi (cache + on-disk overflow).
- Redis: https://redis.io  
- HDFS: https://hadoop.apache.org  
- Grafana provisioning: https://grafana.com/docs/grafana/latest/administration/provisioning/
