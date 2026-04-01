package hdfsx

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Writer adalah struct untuk menulis data ke HDFS (Hadoop Distributed File System).
// HDFS digunakan sebagai on-disk KV store ketika Redis cluster sudah penuh.
// Ini mengimplementasikan overflow pattern: Redis (fast) -> HDFS (persistent).
type Writer struct {
	Path string // Path di HDFS tempat data akan disimpan
}

// NewWriter membuat instance Writer baru dengan path dari environment variable.
// Default path adalah /events_overflow jika HDFS_PATH tidak di-set.
func NewWriter() *Writer {
	p := os.Getenv("HDFS_PATH")
	if p == "" {
		p = "/events_overflow"
	}
	return &Writer{Path: p}
}

// runHdfs menjalankan command hdfs dengan env container (JAVA_HOME, HADOOP_HOME) agar tidak error di container.
func (w *Writer) runHdfs(args string) *exec.Cmd {
	cmd := exec.Command("sh", "-c", fmt.Sprintf("export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk} HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}; %s", args))
	cmd.Env = os.Environ()
	return cmd
}

// EnsureDir memastikan direktori di HDFS sudah ada.
// Jika belum ada, direktori akan dibuat menggunakan command hdfs dfs -mkdir -p.
func (w *Writer) EnsureDir() {
	_ = w.runHdfs(fmt.Sprintf("hdfs dfs -mkdir -p %s", w.Path)).Run()
}

// WriteJSONL menulis array events ke HDFS dalam format JSONL (JSON Lines).
// Format JSONL: setiap event adalah satu baris JSON, cocok untuk big data processing.
// Proses:
// 1. Buat file temporary di local filesystem
// 2. Encode semua events ke file tersebut dalam format JSONL
// 3. Upload file ke HDFS menggunakan hdfs dfs -put
// 4. File temporary akan dihapus setelah upload (atau dibiarkan untuk cleanup manual)
func (w *Writer) WriteJSONL(events []any) error {
	// Pastikan direktori HDFS sudah ada
	w.EnsureDir()
	// Generate nama file temporary dengan timestamp untuk menghindari collision
	ts := time.Now().UnixMilli()
	tmp := fmt.Sprintf("/tmp/overflow_%d.jsonl", ts)

	// Buat file temporary di local filesystem
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	// Encode setiap event sebagai satu baris JSON (JSONL format)
	enc := json.NewEncoder(f)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			_ = f.Close()
			return err
		}
	}
	_ = f.Close()

	// Upload file ke HDFS menggunakan hdfs dfs command
	// -put: upload file dari local ke HDFS
	// -f: force overwrite jika file sudah ada
	return w.runHdfs(fmt.Sprintf("hdfs dfs -put -f %s %s/", tmp, w.Path)).Run()
}

// keyToSafeFileName mengubah key menjadi nama file yang aman untuk HDFS.
// Menggunakan base64 URL encoding agar key dengan karakter khusus tetap unik.
func keyToSafeFileName(key string) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString([]byte(key)), "=")
}

// OffloadDir mengembalikan subdir HDFS untuk data yang di-offload dari Redis (on-disk KV lookup).
func (w *Writer) OffloadDir() string {
	return strings.TrimSuffix(w.Path, "/") + "/offloaded"
}

// WriteKeyValue menulis satu pasangan key-value ke HDFS (satu file per key).
// Dipakai saat memindahkan data dari Redis ke HDFS agar nanti bisa dibaca lagi per key (Read by KV-Store).
func (w *Writer) WriteKeyValue(key string, value []byte) error {
	dir := w.OffloadDir()
	w.EnsureDir()
	_ = w.runHdfs(fmt.Sprintf("hdfs dfs -mkdir -p %s", dir)).Run()
	safe := keyToSafeFileName(key)
	tmp := fmt.Sprintf("/tmp/offload_%s.json", safe)
	if err := os.WriteFile(tmp, value, 0644); err != nil {
		return err
	}
	defer os.Remove(tmp)
	return w.runHdfs(fmt.Sprintf("hdfs dfs -put -f %s %s/%s.json", tmp, dir, safe)).Run()
}

// ReadByKey membaca value untuk key dari HDFS (dari offloaded KV store).
// Mengembalikan nil, error jika file tidak ada atau gagal baca.
func (w *Writer) ReadByKey(key string) ([]byte, error) {
	safe := keyToSafeFileName(key)
	path := fmt.Sprintf("%s/%s.json", w.OffloadDir(), safe)
	out, err := w.runHdfs(fmt.Sprintf("hdfs dfs -cat %s", path)).Output()
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WriteJSONLToPath menulis JSONL ke path relatif di bawah Writer.Path (mis. HDFS_PATH=/derived → file di /derived/<relativePath>).
// Membuat direktori parent di HDFS dengan mkdir -p, lalu put file dari /tmp.
func (w *Writer) WriteJSONLToPath(relativePath string, events []any) error {
	relativePath = strings.TrimSpace(relativePath)
	relativePath = strings.TrimPrefix(relativePath, "/")
	if relativePath == "" {
		return fmt.Errorf("hdfsx: empty relative path")
	}
	base := strings.TrimSuffix(w.Path, "/")
	fullHDFS := base + "/" + relativePath
	idx := strings.LastIndex(fullHDFS, "/")
	if idx <= 0 {
		return fmt.Errorf("hdfsx: invalid hdfs path %q", fullHDFS)
	}
	dir := fullHDFS[:idx]
	_ = w.runHdfs(fmt.Sprintf("hdfs dfs -mkdir -p %s", dir)).Run()

	ts := time.Now().UnixMilli()
	tmp := fmt.Sprintf("/tmp/derived_%d.jsonl", ts)
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	_ = f.Close()
	err = w.runHdfs(fmt.Sprintf("hdfs dfs -put -f %s %s", tmp, fullHDFS)).Run()
	_ = os.Remove(tmp)
	return err
}

// CatFile membaca seluruh isi file di HDFS (untuk replay).
func (w *Writer) CatFile(hdfsPath string) ([]byte, error) {
	return w.runHdfs(fmt.Sprintf("hdfs dfs -cat %s", hdfsPath)).Output()
}

// ListRecursive menjalankan hdfs dfs -ls -R dan mengembalikan stdout (untuk replay: parse path file .jsonl).
func (w *Writer) ListRecursive(hdfsPath string) ([]byte, error) {
	return w.runHdfs(fmt.Sprintf("hdfs dfs -ls -R %s", hdfsPath)).Output()
}
