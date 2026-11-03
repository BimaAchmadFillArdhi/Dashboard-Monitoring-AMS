# CCTV Monitoring Dashboard (Ant Media Server CE 3.1.0)

Dashboard monitoring CCTV operasional untuk operator non-teknis yang terintegrasi langsung dengan Ant Media Server (AMS) Community Edition 3.1.0 via REST API.

---

## Fitur Utama

1. **Logs (Prioritas Utama)**:
   - Ingest log background otomatis dari AMS ke database MySQL.
   - Parser multi-baris (menggabungkan stack trace menjadi satu entri).
   - Terjemahan cerdas ke Bahasa Indonesia awam berbasis kamus aturan `rules.yaml` (hot-reload tanpa restart).
   - Deduplikasi cerdas (pengelompokan log berulang dengan hitungan kemunculan, timestamp pertama, dan terakhir).
   - Mode Live (Server-Sent Events / SSE real-time dengan tombol Pause/Follow dan Jump to latest) dan Mode Histori (filter tanggal, level, aplikasi, CCTV, pencarian teks).
   - Panel deteksi CCTV bermasalah otomatis (*health scoring*).
   - Ekspor data log terfilter ke format CSV.

2. **Dashboard Real-Time**:
   - Ringkasan metrik sistem AMS: CPU, JVM Heap, RAM sistem, kapasitas disk, rata-rata waktu query database, dan status uptime.
   - Live stream count dan viewer count (WebRTC, HLS, DASH).
   - Sparkline chart riwayat 5-10 menit dengan Chart.js (warna solid, tanpa gradient).
   - Pembaruan metrik live via SSE tanpa refresh manual, disertai indikator data stale.
   - Ringkasan aplikasi AMS di bagian bawah dengan tautan langsung ke detail aplikasi.

3. **Applications (Read-Only)**:
   - Sidebar navigasi aplikasi (`atcs`, `atcs-kota`, `cctv-sekolah`, `live`, `WebRTCApp`, dll.) dengan badge jumlah live stream per aplikasi dari `applications-info` AMS.
   - Semua status stream (live dan non-live) diambil per halaman dari snapshot MySQL, dengan pencarian server-side dan pagination 25/50/100 baris; jumlah live memakai hitungan resmi AMS. Snapshot AMS lengkap merekonsiliasi stream yang hilang tanpa menghapus histori status.
   - Tabel stream kamera: nama, stream ID, status operasional (Sehat, Perhatian, Bermasalah, Offline), bitrate, viewer, tanggal dibuat.
   - Drawer panel detail stream di sebelah kanan:
     - URL sumber RTSP yang **disamarkan kredensialnya secara ketat** (`rtsp://user:****@host:port/path`).
     - Metrik live (bitrate, speed, viewer) diperbarui real-time via SSE ketika drawer terbuka.
     - Operasi bersifat strictly read-only untuk keamanan data produksi.

---

## Tech Stack

- **Backend**: Python 3.11+, FastAPI, httpx (async), Pydantic v2, SQLAlchemy 2.x + MySQL.
- **Frontend**: Jinja2 templates, Tailwind CSS (CLI v4), Alpine.js, Chart.js.
- **Real-Time**: Server-Sent Events (SSE).
- **Sistem Desain**: Komponen UI reusable berbasis Jinja macros di `app/templates/components/`.

---

## Struktur Folder

```
AMS/
├── app/
│   ├── main.py                  # Entrypoint FastAPI, background poller, lifecycles
│   ├── config.py                # Konfigurasi via pydantic-settings
│   ├── clients/
│   │   ├── ams_client.py        # Klien tunggal ke AMS (retry, circuit breaker, auth)
│   │   └── models.py            # Model Pydantic untuk respon AMS
│   ├── services/
│   │   ├── dashboard_service.py # Metrik dashboard & ring buffer memori
│   │   ├── application_service.py # Agregasi stream & masking RTSP
│   │   ├── log_service.py       # Query log, deduplikasi, dan ekspor CSV
│   │   ├── health_service.py    # Algoritma evaluasi kesehatan CCTV
│   │   └── auth_service.py      # Autentikasi aplikasi & hash password
│   ├── ingest/
│   │   └── log_ingestor.py      # Background poller log, parser, dedupe
│   ├── translator/
│   │   ├── rules.yaml           # Kamus aturan terjemahan log (data, bukan kode)
│   │   └── engine.py            # Mesin penerjemah log & hot-reloader
│   ├── db/
│   │   ├── session.py           # MySQL engine & session maker
│   │   └── models.py            # Tabel log_entries, unmatched_patterns, ingest_states
│   ├── api/                     # REST JSON & SSE endpoints
│   ├── web/                     # Web routes halaman HTML
│   ├── templates/
│   │   ├── layouts/base.html    # AppShell, Topbar, Sidebar, theme switcher
│   │   ├── components/          # Macros reusable (buttons, inputs, tables, panels)
│   │   └── pages/               # dashboard.html, applications.html, logs.html
│   └── static/                  # CSS terkompilasi, vendor scripts
├── docs/
│   ├── ams_api_notes.md         # Catatan endpoint hasil verifikasi nyata
│   └── ams_samples/             # Sampel JSON mentah dari AMS
├── scripts/
│   └── discover_ams.py          # Skrip discovery awal
├── tests/                       # Unit test & benchmark 1 juta baris
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Instalasi dan Menjalankan Aplikasi

### 1. Prasyarat
- Python 3.11 atau lebih baru
- Node.js & npm (untuk build Tailwind CLI)

### 2. Setup Lingkungan
```bash
# Clone dan masuk ke folder
cd AMS

# Buat virtual environment Python
python -m venv venv
source venv/bin/activate  # Linux/macOS
# atau
.\venv\Scripts\activate   # Windows

# Install dependensi Python
pip install -r requirements.txt

# Install dependensi npm & build Tailwind CSS
npm install
npm run build:css
```

### 3. Konfigurasi (.env)
Salin `.env.example` ke `.env` lalu sesuaikan konfigurasi:
```bash
cp .env.example .env
```
Variabel penting:
- `AMS_BASE_URL`: URL Ant Media Server (contoh: `http://103.255.15.138:5080`)
- `AMS_USER`: Email user AMS (contoh: `wiko@gmail.com`)
- `AMS_PASSWORD`: Password user AMS (contoh: `wiko2026`)
- `POLL_INTERVAL_SECONDS`: Frekuensi background ingest log (default: `5`)
- `LOG_RETENTION_DAYS`: Durasi retensi histori log sebelum dibersihkan (default: `30`)
- `STREAM_METRICS_INTERVAL_SECONDS`: Interval sinkronisasi stream dari AMS (default: `30` detik).
- `STREAM_METRICS_HISTORY_INTERVAL_SECONDS`: Interval penyimpanan histori bitrate/viewer (default: `30` detik); tampilan live tetap diperbarui terpisah.
- `METRIC_RETENTION_DAYS`: Retensi histori metrik server dan stream (default/maksimum efektif: `7` hari untuk mempertahankan retensi yang sudah berjalan); penghapusan dilakukan bertahap.
- Nilai yang sudah ada di `.env` mengalahkan default; sesuaikan interval sinkronisasi stream ke `30` detik setelah pembaruan bila masih memakai nilai lama.
- `LOG_RETENTION_LIMIT`: Maksimal riwayat perubahan status per stream dan log sinkronisasi per aplikasi; pilih `50` atau `100` (default: `100`). Log monitoring utama disimpan berdasarkan `LOG_RETENTION_DAYS` (30 hari secara default), bukan dipangkas menurut jumlah per kamera.
- Halaman log memuat 50 ringkasan terbaru, mengambil pesan teknis hanya saat detail dibuka, dan memperbarui daftar dengan ID baru setiap 10 detik. Statistik level/kategori di-cache selama 30 detik agar polling ingestion tidak memicu agregasi penuh berulang.

Snapshot dashboard dan ringkasan aplikasi memakai cache singkat agar request paralel tidak memicu polling AMS berulang. Retensi membatasi pertumbuhan data, tetapi ruang fisik DB tidak selalu mengecil setelah baris dihapus; MySQL `OPTIMIZE TABLE` memerlukan backup, ruang kosong yang cukup, dan maintenance window.

### 4. Menjalankan Server
```bash
# Development mode dengan reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```
Buka browser di: `http://localhost:8000`

Default kredensial login dashboard:
- Admin: `admin` / `admin12345`
- Operator: `operator` / `operator12345`

---

## Cara Menambah Aturan Terjemahan Log

Aturan terjemahan berada di file `app/translator/rules.yaml`. Sistem mendukung **hot-reload**, artinya aturan baru langsung aktif tanpa perlu merestart server.

Contoh format aturan:
```yaml
  - id: nama_aturan_unik
    match: 'cannot write video frame to muxer\((?P<stream_id>[A-Za-z0-9_\-\.]+)\.m3u8\).*Numerical result out of range'
    level: WARN                       # Opsional: ERROR | WARN | INFO | DEBUG
    title: Judul Singkat Masalah
    message: Penjelasan ramah operator untuk kamera {stream_id}.
    severity: masalah                 # info | perhatian | masalah
    saran_tindakan: Langkah perbaikan yang disarankan untuk teknisi/operator.
    kategori: muxing                  # muxing | jaringan | stream | sistem | auth | viewer
```

---

## Menjalankan Pengujian (Testing & Benchmark)

```bash
# Menjalankan seluruh test suite
pytest -v

# Menjalankan benchmark ingest & query performa 1 juta baris log
python tests/benchmark_1m_logs.py
```

---

## Deployment Docker

```bash
# Menjalankan container dengan docker-compose
docker-compose up -d --build
```
Aplikasi tersambung ke MySQL sesuai `DATABASE_URL`; database harus tersedia dan koneksi gagal tidak dialihkan ke penyimpanan lain.

Pengujian yang memakai database harus menetapkan `TEST_DATABASE_URL` ke database MySQL khusus bernama dengan `test` (contoh: `mysql+aiomysql://user:password@localhost:3306/ams_monitoring_test`). Pengujian menolak URL database utama dan tidak membuat fallback penyimpanan lokal.
