# Dokumentasi Ant Media Server (AMS) CE 3.1.0 REST API

Dokumentasi ini dibuat berdasarkan verifikasi langsung ke server Ant Media Server nyata di `http://103.255.15.138:5080` melalui skrip `scripts/discover_ams.py`.

---

## 1. Autentikasi

- **Endpoint**: `POST /rest/v2/users/authenticate`
- **Headers**: `Content-Type: application/json`
- **Payload**:
  ```json
  {
    "email": "wiko@gmail.com",
    "password": "<md5_hexdigest_of_plain_password>"
  }
  ```
- **Respons Sukses** (HTTP 200):
  ```json
  {
    "success": true,
    "message": "system/USER",
    "dataId": "",
    "errorId": 0
  }
  ```
  Header respons menyertakan cookie: `Set-Cookie: JSESSIONID=...; Path=/; HttpOnly`
- **Catatan & Batasan Keamanan**:
  - Password yang dikirim **WAJIB** berupa hash MD5 string huruf kecil (`hashlib.md5(password.encode()).hexdigest()`), bukan plain text.
  - Server memiliki mekanisme proteksi brute-force: jika gagal berulang kali, akun diblokir sementara selama 300 detik (`"Too many login attempts. User is blocked for 300 secs"`).
  - Session dijaga via cookie `JSESSIONID` pada setiap pemanggilan API berikutnya.

---

## 2. Status Sistem & Metrik (Dashboard)

### A. Versi Software
- **Endpoint**: `GET /rest/v2/version`
- **Respons**:
  ```json
  {
    "versionName": "3.1.0",
    "versionType": "Community Edition",
    "buildNumber": "20260904_1146"
  }
  ```

### B. Sumber Daya Sistem Terpadu (System Resources)
- **Endpoint**: `GET /rest/v2/system-resources`
- **Respons Ringkas**:
  - `cpuUsage`:
    - `systemCPULoad`: persentase beban CPU sistem (0-100)
    - `processCPULoad`: persentase beban CPU proses AMS
    - `systemLoadAverageLastMinute`: load average 1 menit (Linux)
    - `processCPUTime`: total waktu CPU
  - `jvmMemoryUsage`:
    - `maxMemory`, `totalMemory`, `freeMemory`, `inUseMemory` (dalam bytes)
  - `systemMemoryInfo`:
    - `totalMemory`, `freeMemory`, `inUseMemory`, `availableMemory`, swap (dalam bytes)
  - `fileSystemInfo`:
    - `usableSpace`, `totalSpace`, `freeSpace`, `inUseSpace` (dalam bytes)
  - `server-timing`:
    - `up-time`: durasi hidup server dalam milidetik
    - `start-time`: timestamp epoch ms server mulai berjalan
  - `dbAverageQueryTimeMs`: rata-rata waktu query database internal
  - `localLiveStreams`: jumlah live stream lokal yang aktif
  - `localHLSViewers`, `localWebRTCViewers`, `localDASHViewers`: penonton lokal

### C. Status Terpisah (Fallback / Granular)
- `GET /rest/v2/cpu-status`
- `GET /rest/v2/system-memory-status`
- `GET /rest/v2/file-system-status`

---

## 3. Aplikasi & Pengaturan

### A. Daftar Nama Aplikasi
- **Endpoint**: `GET /rest/v2/applications`
- **Respons**:
  ```json
  {
    "applications": ["LiveApp", "WebRTCApp", "atcs", "atcs-kota", "cctv-sekolah", "live"]
  }
  ```

### B. Info Ringkas Aplikasi
- **Endpoint**: `GET /rest/v2/applications-info`
- **Respons**:
  ```json
  [
    {
      "name": "atcs",
      "liveStreamCount": 1,
      "vodCount": 0,
      "storage": 32906152
    },
    {
      "name": "atcs-kota",
      "liveStreamCount": 0,
      "vodCount": 0,
      "storage": 32906152
    }
  ]
  ```

### C. Pengaturan Aplikasi
- **Endpoint**: `GET /rest/v2/applications/settings/{app_name}`
- Berisi konfigurasi HLS, MP4, WebRTC, storage, dan `jwtSecretKey`.

---

## 4. REST Stream Per-Aplikasi (Tomcat Proxy Pattern)

Ant Media Server berjalan di atas kontainer servlet Tomcat. Session autentikasi user berada pada root (`/`). Memanggil langsung `/{app}/rest/v2/...` akan menghasilkan HTTP 403 Forbidden.

Solusi resmi Ant Media Server adalah memanggil proxy internal:
`GET /rest/v2/request?_path={app}/rest/v2/{sub_path}`

### A. Jumlah Stream (Broadcast Count)
- **Path**: `GET /rest/v2/request?_path={app}/rest/v2/broadcasts/count`
- **Respons**:
  ```json
  {"number": 8}
  ```

### B. Daftar Stream (Broadcasts List)
- **Path**: `GET /rest/v2/request?_path={app}/rest/v2/broadcasts/list/{offset}/{size}`
- **Parameter**:
  - `offset`: index awal (0-based)
  - `size`: jumlah entri yang diambil (mis. 20, 50)
- **Respons**: Array entri broadcast. Field penting:
  - `streamId` (string, mis. "APMD.stream")
  - `name` (string)
  - `status` ("created", "broadcasting", "finished")
  - `type` ("streamSource", "ipCamera", dll.)
  - `streamUrl` (URL sumber RTSP/RTMP - mengandung password yang harus disamarkan di UI)
  - `originAdress` (string IP)
  - `date` (timestamp ms stream dibuat)
  - `speed` (float)
  - `bitrate` (integer bps)
  - `width`, `height` (integer resolusi video)
  - `hlsViewerCount`, `webRTCViewerCount`, `dashViewerCount`
  - `mp4Enabled`, `webMEnabled` (integer/boolean status rekaman)
  - `autoStartStopEnabled` (boolean)

### C. Detail Satu Stream
- **Path**: `GET /rest/v2/request?_path={app}/rest/v2/broadcasts/{streamId}`
- Mengembalikan satu objek broadcast dengan field lengkap seperti di atas.

### D. Statistik Broadcast
- **Path**: `GET /rest/v2/request?_path={app}/rest/v2/broadcasts/total-broadcast-statistics`
- **Respons**:
  ```json
  {
    "totalRTMPWatchersCount": -1,
    "totalHLSWatchersCount": 0,
    "totalWebRTCWatchersCount": 0,
    "totalDASHWatchersCount": 0,
    "activeLiveStreamCount": 1
  }
  ```

---

## 5. File Log Server (Log Ingestor)

### A. Pengambilan Potongan Log
- **Endpoint**: `GET /rest/v2/log-file/{offset}/{charSize}?logType={server|errors}`
- **Parameter**:
  - `offset`: byte offset awal pembacaan file di disk (0 untuk awal file)
  - `charSize`: ukuran chunk byte yang ingin dibaca (mis. 65536)
  - `logType`: `server` untuk `ant-media-server.log`, atau `errors` untuk `antmedia-error.log`
- **Respons**:
  ```json
  {
    "logContent": "2026-10-26 12:54:09,259 [Thread-704] INFO  io.antmedia.muxer.MuxAdaptor - Stream queue size:0 ...\n",
    "logContentSize": 4096,
    "logFileSize": 44010824
  }
  ```

### B. Format Baris Log AMS
- Pola standar:
  `YYYY-MM-DD HH:mm:ss,SSS [thread-name] LEVEL  logger.class.Name - message`
- Contoh baris:
  `2026-10-26 12:54:09,765 [Thread-704] WARN  io.antmedia.muxer.HLSMuxer - cannot write video frame to muxer(TC-DemenGlagah-VSS.m3u8). Pts: 598124250 dts:598124250  Error is No such file or directory`
- Deteksi Log Rotation:
  Jika `logFileSize < last_offset`, file log dirotasi (atau di-truncate). Ingestor harus mereset offset kembali ke 0.
