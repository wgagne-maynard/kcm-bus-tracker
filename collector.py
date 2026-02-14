"""
King County Metro Bus Position Collector

Fetches real-time bus positions every 30 seconds and stores them in PostgreSQL/TimescaleDB.
Designed to run continuously on Railway, DigitalOcean, or similar cloud platforms.
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
import psycopg2
from psycopg2.extras import execute_values

# Configuration
KCM_FEED_URL = "https://s3.amazonaws.com/kcm-alerts-realtime-prod/vehiclepositions_enhanced.json"
FETCH_INTERVAL_SECONDS = 30
DATABASE_URL = os.environ.get("DATABASE_URL")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def init_database(conn) -> None:
    """Create tables if they don't exist."""
    with conn.cursor() as cur:
        # Main positions table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bus_positions (
                id BIGSERIAL,
                recorded_at TIMESTAMPTZ NOT NULL,
                feed_timestamp BIGINT NOT NULL,
                vehicle_id TEXT NOT NULL,
                route_id TEXT,
                trip_id TEXT,
                direction_id INTEGER,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                current_stop_sequence INTEGER,
                stop_id TEXT,
                current_status TEXT,
                vehicle_timestamp BIGINT,
                start_date TEXT,
                block_id TEXT,
                PRIMARY KEY (id, recorded_at)
            );
        """)
        
        # Create indexes for common queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bus_positions_vehicle_time 
            ON bus_positions (vehicle_id, recorded_at DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bus_positions_route_time 
            ON bus_positions (route_id, recorded_at DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bus_positions_recorded_at 
            ON bus_positions (recorded_at DESC);
        """)
        
        conn.commit()

        # Try to enable TimescaleDB hypertable (will fail gracefully if not available)
        try:
            cur.execute("""
                SELECT create_hypertable('bus_positions', 'recorded_at',
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                );
            """)
            conn.commit()
            logger.info("TimescaleDB hypertable enabled")
        except psycopg2.Error as e:
            conn.rollback()
            if "hypertable" in str(e).lower() or "timescaledb" in str(e).lower():
                logger.info("TimescaleDB not available, using regular PostgreSQL table")
            else:
                raise

        # Enable compression if TimescaleDB is available
        try:
            cur.execute("""
                ALTER TABLE bus_positions SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'vehicle_id, route_id'
                );
            """)
            cur.execute("""
                SELECT add_compression_policy('bus_positions', INTERVAL '7 days', if_not_exists => TRUE);
            """)
            conn.commit()
            logger.info("TimescaleDB compression policy enabled")
        except psycopg2.Error:
            conn.rollback()  # TimescaleDB not available, skip compression
        logger.info("Database initialized successfully")


def fetch_bus_positions() -> Optional[dict]:
    """Fetch current bus positions from KCM feed."""
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(KCM_FEED_URL)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        logger.error(f"HTTP error fetching bus positions: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching bus positions: {e}")
        return None


def parse_and_store(conn, data: dict) -> int:
    """Parse the feed data and store in database. Returns number of records inserted."""
    if not data or "entity" not in data:
        logger.warning("No entity data in feed")
        return 0
    
    feed_timestamp = data.get("header", {}).get("timestamp", 0)
    recorded_at = datetime.now(timezone.utc)
    
    records = []
    for entity in data["entity"]:
        vehicle = entity.get("vehicle", {})
        trip = vehicle.get("trip", {})
        position = vehicle.get("position", {})
        vehicle_info = vehicle.get("vehicle", {})
        
        # Skip if missing critical data
        if not position.get("latitude") or not position.get("longitude"):
            continue
        if not vehicle_info.get("id"):
            continue
        
        records.append((
            recorded_at,
            feed_timestamp,
            vehicle_info.get("id"),
            trip.get("route_id"),
            trip.get("trip_id"),
            trip.get("direction_id"),
            position.get("latitude"),
            position.get("longitude"),
            vehicle.get("current_stop_sequence"),
            vehicle.get("stop_id"),
            vehicle.get("current_status"),
            vehicle.get("timestamp"),
            trip.get("start_date"),
            vehicle.get("block_id"),
        ))
    
    if not records:
        logger.warning("No valid records to insert")
        return 0
    
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO bus_positions (
                recorded_at, feed_timestamp, vehicle_id, route_id, trip_id,
                direction_id, latitude, longitude, current_stop_sequence,
                stop_id, current_status, vehicle_timestamp, start_date, block_id
            ) VALUES %s
            """,
            records
        )
        conn.commit()
    
    return len(records)


def run_collector():
    """Main collector loop."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set")
        raise SystemExit(1)
    
    logger.info("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    
    logger.info("Initializing database schema...")
    init_database(conn)
    
    logger.info(f"Starting collector (fetching every {FETCH_INTERVAL_SECONDS}s)...")
    consecutive_failures = 0
    max_failures = 10
    
    while True:
        try:
            start_time = time.time()
            
            data = fetch_bus_positions()
            if data:
                count = parse_and_store(conn, data)
                logger.info(f"Stored {count} bus positions")
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logger.warning(f"Failed to fetch data ({consecutive_failures}/{max_failures})")
            
            # If too many consecutive failures, try reconnecting to DB
            if consecutive_failures >= max_failures:
                logger.error("Too many consecutive failures, attempting DB reconnect...")
                try:
                    conn.close()
                except:
                    pass
                conn = psycopg2.connect(DATABASE_URL)
                init_database(conn)
                consecutive_failures = 0
            
            # Sleep for the remaining time to maintain 30s interval
            elapsed = time.time() - start_time
            sleep_time = max(0, FETCH_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_time)
            
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            try:
                conn.close()
            except:
                pass
            time.sleep(5)
            conn = psycopg2.connect(DATABASE_URL)
            init_database(conn)

        except KeyboardInterrupt:
            logger.info("Shutting down...")
            conn.close()
            break
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run_collector()
