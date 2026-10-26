#!/usr/bin/env python3
"""
tests/benchmark_1m_logs.py - Benchmark batch insert & indexed query performance with 1,000,000 log entries.
"""
import os
import sys
import time
import sqlite3
import hashlib
from pathlib import Path

# Setup path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

BENCH_DB_PATH = BASE_DIR / "benchmark_1m.db"


def run_benchmark():
    if os.path.exists(BENCH_DB_PATH):
        os.remove(BENCH_DB_PATH)

    print(f"[*] Menyiapkan database benchmark SQLite di: {BENCH_DB_PATH}")
    conn = sqlite3.connect(str(BENCH_DB_PATH))
    cursor = conn.cursor()

    # Apply WAL mode and PRAGMAs
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.execute("PRAGMA cache_size=-64000;")
    cursor.execute("PRAGMA temp_store=MEMORY;")

    # Create schema matching app/db/models.py
    cursor.execute("""
    CREATE TABLE log_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts BIGINT NOT NULL,
        level VARCHAR(10) NOT NULL,
        logger VARCHAR(255),
        raw_message TEXT NOT NULL,
        app_name VARCHAR(64),
        stream_id VARCHAR(128),
        rule_id VARCHAR(64),
        friendly_title VARCHAR(255),
        friendly_message TEXT,
        severity_group VARCHAR(20) NOT NULL,
        action_hint TEXT,
        category VARCHAR(50),
        fingerprint VARCHAR(64) NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
    """)
    cursor.execute("CREATE INDEX ix_log_entries_ts ON log_entries (ts);")
    cursor.execute("CREATE INDEX ix_log_entries_stream_ts ON log_entries (stream_id, ts);")
    cursor.execute("CREATE INDEX ix_log_entries_ts_level ON log_entries (ts, level);")
    cursor.execute("CREATE INDEX ix_log_entries_level_ts ON log_entries (level, ts);")
    cursor.execute("CREATE INDEX ix_log_entries_stream_fp_ts ON log_entries (stream_id, fingerprint, ts);")
    cursor.execute("CREATE INDEX ix_log_entries_fingerprint ON log_entries (fingerprint);")
    cursor.execute("CREATE INDEX ix_log_entries_fingerprint_ts ON log_entries (fingerprint, ts);")
    conn.commit()

    TOTAL_ROWS = 1_000_000
    BATCH_SIZE = 50_000
    print(f"[*] Memulai batch insert {TOTAL_ROWS:,} baris log (batch per {BATCH_SIZE:,} baris)...")

    base_ts = 1792900000000  # Epoch ms
    streams = [
        "TC-DemenGlagah-VSS",
        "APMD.stream",
        "ATCS_pingit.stream",
        "cctv_sma1_depan",
        "cctv_tugu_utara",
    ]
    levels = ["WARN", "INFO", "ERROR", "DEBUG"]
    rule_ids = [
        "cannot_write_video_frame_numerical_range",
        "last_dts_bigger",
        "stream_queue_size_speed",
        "audio_video_sync_gap",
    ]

    t_start = time.perf_counter()

    insert_sql = """
    INSERT INTO log_entries (
        ts, level, logger, raw_message, app_name, stream_id,
        rule_id, friendly_title, friendly_message, severity_group,
        action_hint, category, fingerprint
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for batch_idx in range(TOTAL_ROWS // BATCH_SIZE):
        batch_data = []
        batch_base = batch_idx * BATCH_SIZE
        for i in range(BATCH_SIZE):
            idx = batch_base + i
            ts = base_ts + (idx * 50)  # incrementing timestamps
            sid = streams[idx % len(streams)]
            lvl = levels[idx % len(levels)]
            rid = rule_ids[idx % len(rule_ids)]
            fp = hashlib.md5(f"{rid}:{sid}:{lvl}".encode()).hexdigest()
            raw = f"cannot write video frame to muxer({sid}.m3u8). Pts: {idx} dts:{idx} Error is Numerical result out of range"
            msg = f"Video dari kamera {sid} tidak bisa disimpan ke pemutar HLS karena urutan frame kacau."

            batch_data.append((
                ts,
                lvl,
                "io.antmedia.muxer.HLSMuxer",
                raw,
                "atcs",
                sid,
                rid,
                "Gangguan Muxer Video HLS",
                msg,
                "masalah" if lvl in ("ERROR", "WARN") else "info",
                "Periksa koneksi jaringan CCTV.",
                "muxing",
                fp,
            ))

        cursor.executemany(insert_sql, batch_data)
        conn.commit()
        print(f"  [+] Terinput {batch_base + BATCH_SIZE:,} / {TOTAL_ROWS:,} baris...")

    t_insert = time.perf_counter() - t_start
    rate = TOTAL_ROWS / t_insert
    print(f"\n[HASIL INSERT] {TOTAL_ROWS:,} baris selesai dalam {t_insert:.2f} detik ({rate:,.0f} baris/detik)")
    assert rate > 20_000, f"Insert rate {rate} rows/sec is below target"

    # =========================================================================
    # QUERY BENCHMARKS on 1 MILLION ROWS
    # =========================================================================
    print("\n[*] Menjalankan uji performa query pada 1 juta baris:")

    # 1. Query by stream_id + timestamp (last 1 hour window)
    q_stream = streams[0]
    q_start_ts = base_ts + (500_000 * 50)
    q_end_ts = q_start_ts + (3600 * 1000)

    t0 = time.perf_counter()
    cursor.execute("""
        SELECT id, ts, level, friendly_title, friendly_message
        FROM log_entries
        WHERE stream_id = ? AND ts BETWEEN ? AND ?
        ORDER BY ts DESC
        LIMIT 50
    """, (q_stream, q_start_ts, q_end_ts))
    rows = cursor.fetchall()
    t_q1 = (time.perf_counter() - t0) * 1000
    print(f"  [Q1: stream_id + ts range] {len(rows)} baris diambil dalam {t_q1:.2f} ms")
    assert t_q1 < 50.0, f"Query 1 took too long: {t_q1}ms"

    # 2. Query level counts for date range (Level summary)
    t0 = time.perf_counter()
    cursor.execute("""
        SELECT level, count(*)
        FROM log_entries
        WHERE ts BETWEEN ? AND ?
        GROUP BY level
    """, (q_start_ts, q_end_ts))
    summary = cursor.fetchall()
    t_q2 = (time.perf_counter() - t0) * 1000
    print(f"  [Q2: level count group by] {summary} dalam {t_q2:.2f} ms")
    assert t_q2 < 150.0, f"Query 2 took too long: {t_q2}ms"
    # 3. Deduplicated group query by fingerprint
    t0 = time.perf_counter()
    cursor.execute("""
        SELECT fingerprint, stream_id, level, friendly_title, count(*) as cnt,
               min(ts) as first_seen, max(ts) as last_seen
        FROM log_entries
        WHERE stream_id = ? AND ts BETWEEN ? AND ?
        GROUP BY fingerprint
        ORDER BY last_seen DESC
        LIMIT 20
    """, (q_stream, q_start_ts, q_end_ts))
    grouped_rows = cursor.fetchall()
    t_q3 = (time.perf_counter() - t0) * 1000
    print(f"  [Q3: deduplication group by fingerprint] {len(grouped_rows)} pola grup dalam {t_q3:.2f} ms")
    assert t_q3 < 150.0, f"Query 3 took too long: {t_q3}ms"

    conn.close()

    # Clean up benchmark DB
    if os.path.exists(BENCH_DB_PATH):
        os.remove(BENCH_DB_PATH)
    wal_path = BASE_DIR / "benchmark_1m.db-wal"
    if os.path.exists(wal_path):
        os.remove(wal_path)
    shm_path = BASE_DIR / "benchmark_1m.db-shm"
    if os.path.exists(shm_path):
        os.remove(shm_path)

    print("\n[V] SEMUA UJI BENCHMARK 1 JUTA BARIS LOLOS DENGAN SANGAT CEPAT!")


if __name__ == "__main__":
    run_benchmark()
