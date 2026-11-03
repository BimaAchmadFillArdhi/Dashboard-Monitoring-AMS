#!/usr/bin/env python3
"""Benchmark log inserts and indexed queries in a temporary MySQL test table."""
import hashlib
import os
import sys
import time
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


def run_benchmark():
    raw_url = os.environ.get("TEST_DATABASE_URL")
    if not raw_url:
        raise RuntimeError("Set TEST_DATABASE_URL to a dedicated MySQL test database")

    url = make_url(raw_url)
    if url.drivername != "mysql+aiomysql" or not url.database or "test" not in url.database.lower():
        raise RuntimeError("TEST_DATABASE_URL must target a dedicated MySQL test database")

    engine = create_engine(
        url.set(drivername="mysql+pymysql"),
        poolclass=NullPool,
        echo=False,
    )
    connection = engine.raw_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            CREATE TEMPORARY TABLE benchmark_log_entries (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                ts BIGINT NOT NULL,
                level VARCHAR(10) NOT NULL,
                friendly_title VARCHAR(255),
                friendly_message TEXT,
                stream_id VARCHAR(128),
                fingerprint VARCHAR(64) NOT NULL,
                INDEX ix_bench_stream_ts (stream_id, ts),
                INDEX ix_bench_ts_level (ts, level),
                INDEX ix_bench_fingerprint (fingerprint)
            ) ENGINE=InnoDB
            """
        )

        total_rows = 1_000_000
        batch_size = 5_000
        base_ts = 1792900000000
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
        insert_sql = """
            INSERT INTO benchmark_log_entries
                (ts, level, friendly_title, friendly_message, stream_id, fingerprint)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        started = time.perf_counter()
        for batch_start in range(0, total_rows, batch_size):
            batch = []
            for index in range(batch_start, min(batch_start + batch_size, total_rows)):
                stream_id = streams[index % len(streams)]
                level = levels[index % len(levels)]
                rule_id = rule_ids[index % len(rule_ids)]
                fingerprint = hashlib.md5(
                    f"{rule_id}:{stream_id}:{level}".encode()
                ).hexdigest()
                batch.append(
                    (
                        base_ts + index * 50,
                        level,
                        "Gangguan Muxer Video HLS",
                        f"Video kamera {stream_id} mengalami gangguan.",
                        stream_id,
                        fingerprint,
                    )
                )
            cursor.executemany(insert_sql, batch)
            connection.commit()

        insert_seconds = time.perf_counter() - started
        print(
            f"Inserted {total_rows:,} test rows in {insert_seconds:.2f}s "
            f"({total_rows / insert_seconds:,.0f} rows/s)"
        )

        stream_id = streams[0]
        start_ts = base_ts + 500_000 * 50
        end_ts = start_ts + 3_600_000
        query_started = time.perf_counter()
        cursor.execute(
            """
            SELECT id, ts, level, friendly_title, friendly_message
            FROM benchmark_log_entries
            WHERE stream_id = %s AND ts BETWEEN %s AND %s
            ORDER BY ts DESC
            LIMIT 50
            """,
            (stream_id, start_ts, end_ts),
        )
        print(
            f"Indexed stream query returned {len(cursor.fetchall())} rows in "
            f"{(time.perf_counter() - query_started) * 1000:.2f}ms"
        )
    finally:
        cursor.close()
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    run_benchmark()
