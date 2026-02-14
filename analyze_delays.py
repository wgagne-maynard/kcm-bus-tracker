"""
Delay Analysis Script

Compares actual bus stop arrivals (from bus_positions) to scheduled times
(from gtfs_stop_times) and prints per-route delay summaries.

Usage:
    DATABASE_URL=postgresql://... python analyze_delays.py [--date 20260213] [--min-obs 10]
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timezone

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# Day-of-week column names in gtfs_calendar, indexed by Python weekday (0=Monday)
DOW_COLUMNS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

DELAY_QUERY = """
WITH stopped AS (
    -- Get distinct stop visits: first time a vehicle was seen STOPPED_AT each stop
    SELECT DISTINCT ON (bp.trip_id, bp.stop_id)
        bp.trip_id,
        bp.stop_id,
        bp.route_id,
        bp.start_date,
        bp.vehicle_timestamp,
        bp.recorded_at
    FROM bus_positions bp
    WHERE bp.current_status = 'STOPPED_AT'
      AND bp.trip_id IS NOT NULL
      AND bp.stop_id IS NOT NULL
      AND bp.vehicle_timestamp IS NOT NULL
      AND bp.start_date = %(start_date)s
    ORDER BY bp.trip_id, bp.stop_id, bp.vehicle_timestamp ASC
),
with_schedule AS (
    SELECT
        s.trip_id,
        s.stop_id,
        s.route_id,
        s.start_date,
        s.vehicle_timestamp,
        st.arrival_time,
        gt.service_id,
        -- Convert scheduled arrival to epoch for the service day
        -- arrival_time is HH:MM:SS (can be >24:00:00 for next-day trips)
        EXTRACT(EPOCH FROM (%(start_date_ts)s::date + st.arrival_time::interval))
            AS scheduled_epoch
    FROM stopped s
    JOIN gtfs_stop_times st ON st.trip_id = s.trip_id AND st.stop_id = s.stop_id
    JOIN gtfs_trips gt ON gt.trip_id = s.trip_id
    -- Ensure the service was active on this date
    JOIN gtfs_calendar gc ON gc.service_id = gt.service_id
        AND %(start_date)s >= gc.start_date
        AND %(start_date)s <= gc.end_date
        AND CASE EXTRACT(DOW FROM %(start_date_ts)s::date)
                WHEN 0 THEN gc.sunday
                WHEN 1 THEN gc.monday
                WHEN 2 THEN gc.tuesday
                WHEN 3 THEN gc.wednesday
                WHEN 4 THEN gc.thursday
                WHEN 5 THEN gc.friday
                WHEN 6 THEN gc.saturday
            END = 1
    -- Exclude dates removed by calendar_dates
    LEFT JOIN gtfs_calendar_dates gcd
        ON gcd.service_id = gt.service_id
        AND gcd.date = %(start_date)s
        AND gcd.exception_type = 2
    WHERE gcd.service_id IS NULL
)
SELECT
    ws.route_id,
    gr.route_short_name,
    gr.route_desc,
    COUNT(*) AS observations,
    AVG(ws.vehicle_timestamp - ws.scheduled_epoch) / 60.0 AS avg_delay_min,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ws.vehicle_timestamp - ws.scheduled_epoch) / 60.0
        AS median_delay_min,
    COUNT(*) FILTER (WHERE ABS(ws.vehicle_timestamp - ws.scheduled_epoch) <= 300)::float
        / COUNT(*)::float * 100 AS on_time_pct
FROM with_schedule ws
JOIN gtfs_routes gr ON gr.route_id = ws.route_id
GROUP BY ws.route_id, gr.route_short_name, gr.route_desc
HAVING COUNT(*) >= %(min_obs)s
ORDER BY avg_delay_min DESC;
"""


def format_delay(minutes: float) -> str:
    """Format delay in minutes with sign."""
    if minutes >= 0:
        return f"+{minutes:.1f} min"
    return f"{minutes:.1f} min"


def main():
    parser = argparse.ArgumentParser(description="Analyze bus delays vs GTFS schedule")
    parser.add_argument("--date", help="Service date to analyze as YYYYMMDD (default: today)")
    parser.add_argument("--min-obs", type=int, default=10,
                        help="Minimum observations per route (default: 10)")
    args = parser.parse_args()

    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)

    # Determine the service date
    if args.date:
        start_date = args.date
    else:
        start_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    # Format for SQL date casting
    start_date_ts = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"

    logger.info(f"Analyzing delays for service date {start_date} ({start_date_ts})")

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            # Quick check: how many STOPPED_AT records exist for this date?
            cur.execute("""
                SELECT COUNT(*) FROM bus_positions
                WHERE current_status = 'STOPPED_AT'
                  AND start_date = %s
                  AND trip_id IS NOT NULL
            """, (start_date,))
            total_stopped = cur.fetchone()[0]
            logger.info(f"Found {total_stopped:,} STOPPED_AT records for {start_date}")

            if total_stopped == 0:
                logger.warning("No data found for this date. Make sure the collector has been running.")
                sys.exit(0)

            # Run the delay query
            cur.execute(DELAY_QUERY, {
                "start_date": start_date,
                "start_date_ts": start_date_ts,
                "min_obs": args.min_obs,
            })
            rows = cur.fetchall()

        if not rows:
            logger.warning("No routes matched. Check that GTFS data is loaded and service_ids align.")
            sys.exit(0)

        # Print results
        print(f"\n{'='*80}")
        print(f"  Delay Analysis for {start_date_ts}  |  {total_stopped:,} stop observations")
        print(f"{'='*80}\n")
        print(f"  {'Route':<8} {'Description':<40} {'Obs':>5}  {'Avg Delay':>10}  {'Median':>10}  {'On-Time':>7}")
        print(f"  {'-'*8} {'-'*40} {'-'*5}  {'-'*10}  {'-'*10}  {'-'*7}")

        total_obs = 0
        total_delay_weighted = 0.0
        for route_id, short_name, desc, obs, avg_delay, median_delay, on_time_pct in rows:
            name = short_name or route_id
            desc_short = (desc or "")[:38]
            print(f"  {name:<8} {desc_short:<40} {obs:>5}  {format_delay(avg_delay):>10}"
                  f"  {format_delay(median_delay):>10}  {on_time_pct:>6.0f}%")
            total_obs += obs
            total_delay_weighted += avg_delay * obs

        overall_avg = total_delay_weighted / total_obs if total_obs else 0
        print(f"\n  Overall: {total_obs:,} observations across {len(rows)} routes, "
              f"avg delay {format_delay(overall_avg)}")
        print()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
