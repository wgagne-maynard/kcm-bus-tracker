"""
GTFS Static Data Loader

Reads KCM GTFS CSV files and loads them into PostgreSQL tables.
Can be re-run whenever KCM publishes new schedules (drops and recreates tables).

Usage:
    DATABASE_URL=postgresql://... python load_gtfs.py [--gtfs-dir ./gtfs]
"""

import os
import csv
import sys
import time
import logging
import argparse
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# SQL for creating tables
CREATE_TABLES_SQL = """
DROP TABLE IF EXISTS gtfs_stop_times CASCADE;
DROP TABLE IF EXISTS gtfs_trips CASCADE;
DROP TABLE IF EXISTS gtfs_routes CASCADE;
DROP TABLE IF EXISTS gtfs_stops CASCADE;
DROP TABLE IF EXISTS gtfs_calendar CASCADE;
DROP TABLE IF EXISTS gtfs_calendar_dates CASCADE;

CREATE TABLE gtfs_routes (
    route_id TEXT PRIMARY KEY,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_desc TEXT,
    route_type INTEGER,
    route_url TEXT,
    route_color TEXT,
    route_text_color TEXT
);

CREATE TABLE gtfs_stops (
    stop_id TEXT PRIMARY KEY,
    stop_code TEXT,
    stop_name TEXT,
    stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION,
    location_type INTEGER,
    parent_station TEXT,
    wheelchair_boarding INTEGER
);

CREATE TABLE gtfs_trips (
    trip_id TEXT PRIMARY KEY,
    route_id TEXT REFERENCES gtfs_routes(route_id),
    service_id TEXT NOT NULL,
    trip_headsign TEXT,
    direction_id INTEGER,
    block_id TEXT,
    shape_id TEXT
);

CREATE TABLE gtfs_stop_times (
    trip_id TEXT NOT NULL,
    arrival_time TEXT NOT NULL,
    departure_time TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    timepoint INTEGER,
    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE gtfs_calendar (
    service_id TEXT PRIMARY KEY,
    monday INTEGER NOT NULL,
    tuesday INTEGER NOT NULL,
    wednesday INTEGER NOT NULL,
    thursday INTEGER NOT NULL,
    friday INTEGER NOT NULL,
    saturday INTEGER NOT NULL,
    sunday INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL
);

CREATE TABLE gtfs_calendar_dates (
    service_id TEXT NOT NULL,
    date TEXT NOT NULL,
    exception_type INTEGER NOT NULL,
    PRIMARY KEY (service_id, date)
);

-- Indexes for fast joins with bus_positions
CREATE INDEX idx_gtfs_stop_times_trip_stop ON gtfs_stop_times (trip_id, stop_id);
CREATE INDEX idx_gtfs_stop_times_trip_seq ON gtfs_stop_times (trip_id, stop_sequence);
CREATE INDEX idx_gtfs_trips_route ON gtfs_trips (route_id);
CREATE INDEX idx_gtfs_trips_service ON gtfs_trips (service_id);
CREATE INDEX idx_gtfs_calendar_dates_date ON gtfs_calendar_dates (date);
"""


def load_csv(gtfs_dir: Path, filename: str) -> list[dict]:
    """Read a GTFS CSV file and return list of row dicts."""
    filepath = gtfs_dir / filename
    if not filepath.exists():
        logger.warning(f"{filename} not found, skipping")
        return []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_routes(cur, rows: list[dict]) -> int:
    if not rows:
        return 0
    values = [
        (r["route_id"], r.get("agency_id", ""), r.get("route_short_name", ""),
         r.get("route_long_name", ""), r.get("route_desc", ""),
         int(r.get("route_type", 0)), r.get("route_url", ""),
         r.get("route_color", ""), r.get("route_text_color", ""))
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO gtfs_routes (route_id, agency_id, route_short_name, route_long_name,
            route_desc, route_type, route_url, route_color, route_text_color)
        VALUES %s
    """, values)
    return len(values)


def load_stops(cur, rows: list[dict]) -> int:
    if not rows:
        return 0
    values = [
        (r["stop_id"], r.get("stop_code", ""), r.get("stop_name", ""),
         float(r["stop_lat"]) if r.get("stop_lat") else None,
         float(r["stop_lon"]) if r.get("stop_lon") else None,
         int(r.get("location_type", 0)),
         r.get("parent_station", ""),
         int(r.get("wheelchair_boarding", 0)))
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO gtfs_stops (stop_id, stop_code, stop_name, stop_lat, stop_lon,
            location_type, parent_station, wheelchair_boarding)
        VALUES %s
    """, values)
    return len(values)


def load_trips(cur, rows: list[dict]) -> int:
    if not rows:
        return 0
    values = [
        (r["trip_id"], r["route_id"], r["service_id"],
         r.get("trip_headsign", ""), int(r.get("direction_id", 0)),
         r.get("block_id", ""), r.get("shape_id", ""))
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO gtfs_trips (trip_id, route_id, service_id, trip_headsign,
            direction_id, block_id, shape_id)
        VALUES %s
    """, values)
    return len(values)


def load_stop_times(cur, rows: list[dict]) -> int:
    if not rows:
        return 0
    # Process in batches for the ~1M row file
    batch_size = 50000
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        values = [
            (r["trip_id"], r["arrival_time"], r["departure_time"],
             r["stop_id"], int(r["stop_sequence"]),
             int(r.get("pickup_type", 0)), int(r.get("drop_off_type", 0)),
             int(r.get("timepoint", 0)))
            for r in batch
        ]
        execute_values(cur, """
            INSERT INTO gtfs_stop_times (trip_id, arrival_time, departure_time,
                stop_id, stop_sequence, pickup_type, drop_off_type, timepoint)
            VALUES %s
        """, values)
        total += len(values)
        logger.info(f"  stop_times: {total:,} / {len(rows):,} rows loaded")
    return total


def load_calendar(cur, rows: list[dict]) -> int:
    if not rows:
        return 0
    values = [
        (r["service_id"], int(r["monday"]), int(r["tuesday"]),
         int(r["wednesday"]), int(r["thursday"]), int(r["friday"]),
         int(r["saturday"]), int(r["sunday"]),
         r["start_date"], r["end_date"])
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO gtfs_calendar (service_id, monday, tuesday, wednesday,
            thursday, friday, saturday, sunday, start_date, end_date)
        VALUES %s
    """, values)
    return len(values)


def load_calendar_dates(cur, rows: list[dict]) -> int:
    if not rows:
        return 0
    values = [
        (r["service_id"], r["date"], int(r["exception_type"]))
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO gtfs_calendar_dates (service_id, date, exception_type)
        VALUES %s
    """, values)
    return len(values)


def main():
    parser = argparse.ArgumentParser(description="Load GTFS static data into PostgreSQL")
    parser.add_argument("--gtfs-dir", default="gtfs", help="Path to GTFS directory (default: ./gtfs)")
    args = parser.parse_args()

    gtfs_dir = Path(args.gtfs_dir)
    if not gtfs_dir.exists():
        logger.error(f"GTFS directory not found: {gtfs_dir}")
        sys.exit(1)

    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)

    logger.info(f"Loading GTFS data from {gtfs_dir.resolve()}")
    start = time.time()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            logger.info("Creating tables (dropping old ones)...")
            cur.execute(CREATE_TABLES_SQL)
            conn.commit()

            logger.info("Loading routes.txt...")
            routes = load_csv(gtfs_dir, "routes.txt")
            n = load_routes(cur, routes)
            logger.info(f"  routes: {n:,} rows")

            logger.info("Loading stops.txt...")
            stops = load_csv(gtfs_dir, "stops.txt")
            n = load_stops(cur, stops)
            logger.info(f"  stops: {n:,} rows")

            logger.info("Loading trips.txt...")
            trips = load_csv(gtfs_dir, "trips.txt")
            n = load_trips(cur, trips)
            logger.info(f"  trips: {n:,} rows")

            logger.info("Loading stop_times.txt...")
            stop_times = load_csv(gtfs_dir, "stop_times.txt")
            n = load_stop_times(cur, stop_times)
            logger.info(f"  stop_times: {n:,} rows total")

            logger.info("Loading calendar.txt...")
            calendar = load_csv(gtfs_dir, "calendar.txt")
            n = load_calendar(cur, calendar)
            logger.info(f"  calendar: {n:,} rows")

            logger.info("Loading calendar_dates.txt...")
            cal_dates = load_csv(gtfs_dir, "calendar_dates.txt")
            n = load_calendar_dates(cur, cal_dates)
            logger.info(f"  calendar_dates: {n:,} rows")

            conn.commit()

        elapsed = time.time() - start
        logger.info(f"GTFS load complete in {elapsed:.1f}s")

        # Print summary
        with conn.cursor() as cur:
            for table in ["gtfs_routes", "gtfs_stops", "gtfs_trips",
                          "gtfs_stop_times", "gtfs_calendar", "gtfs_calendar_dates"]:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                logger.info(f"  {table}: {count:,} rows")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
