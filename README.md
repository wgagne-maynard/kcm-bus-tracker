# KCM Bus Tracker

Collects real-time King County Metro bus positions every 30 seconds and stores them in PostgreSQL/TimescaleDB for historical analysis.

## What This Does

- Fetches bus positions from KCM's GTFS-realtime feed every 30 seconds
- Stores: vehicle ID, route, trip, position (lat/lng), stop info, timestamps
- Designed for years of historical data (~2-3GB/year with TimescaleDB compression)
- Runs continuously on Railway, DigitalOcean, or any Docker host

## Quick Deploy to Railway

### 1. Create a GitHub repo

```bash
# Clone or create your repo
git clone https://github.com/wgagne-maynard/kcm-bus-tracker.git
cd kcm-bus-tracker

# Or if starting fresh:
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/wgagne-maynard/kcm-bus-tracker.git
git push -u origin main
```

### 2. Set up Railway

1. Go to [railway.app](https://railway.app) and sign in with GitHub
2. Click **"New Project"** → **"Deploy from GitHub repo"**
3. Select your `kcm-bus-tracker` repo
4. Railway will detect the Dockerfile and start building

### 3. Add the database

1. In your Railway project, click **"+ New"** → **"Database"** → **"Add PostgreSQL"**
2. Railway automatically creates a `DATABASE_URL` variable
3. The collector will connect and create tables on first run

### 4. (Optional) Enable TimescaleDB

For better compression and time-series queries, use Timescale instead of plain Postgres:

1. In Railway, click **"+ New"** → **"Template"** → search "Timescale"
2. Or use [Timescale Cloud](https://www.timescale.com/cloud) free tier (30 days, then ~$9/mo)
3. Copy the connection string to your Railway variables as `DATABASE_URL`

## Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Set database URL (local PostgreSQL)
export DATABASE_URL="postgresql://user:pass@localhost:5432/kcm_buses"

# Run collector
python collector.py
```

## Database Schema

```sql
bus_positions (
    id                    BIGSERIAL,
    recorded_at           TIMESTAMPTZ,  -- When we fetched it
    feed_timestamp        BIGINT,       -- KCM's feed timestamp
    vehicle_id            TEXT,         -- Bus number (e.g., "7427")
    route_id              TEXT,         -- Route (e.g., "100001")
    trip_id               TEXT,         -- Specific trip
    direction_id          INTEGER,      -- 0 or 1
    latitude              DOUBLE,
    longitude             DOUBLE,
    current_stop_sequence INTEGER,
    stop_id               TEXT,
    current_status        TEXT,         -- "STOPPED_AT" or "IN_TRANSIT_TO"
    vehicle_timestamp     BIGINT,       -- When bus reported position
    start_date            TEXT,         -- Trip start date (YYYYMMDD)
    block_id              TEXT
)
```

## Useful Queries

```sql
-- Latest position for each bus
SELECT DISTINCT ON (vehicle_id) *
FROM bus_positions
ORDER BY vehicle_id, recorded_at DESC;

-- Route 40 positions in the last hour
SELECT * FROM bus_positions
WHERE route_id = '100040'
  AND recorded_at > NOW() - INTERVAL '1 hour'
ORDER BY recorded_at DESC;

-- How many records per day?
SELECT DATE(recorded_at) as day, COUNT(*) 
FROM bus_positions 
GROUP BY day 
ORDER BY day DESC;

-- Positions for a specific bus over time (for mapping)
SELECT recorded_at, latitude, longitude, current_status, stop_id
FROM bus_positions
WHERE vehicle_id = '7427'
  AND recorded_at > NOW() - INTERVAL '4 hours'
ORDER BY recorded_at;
```

## Monitoring

Check the Railway logs to see collection status:

```
2024-02-13 10:30:00 - INFO - Stored 156 bus positions
2024-02-13 10:30:30 - INFO - Stored 158 bus positions
```

## Cost Estimate

| Component | Service | Monthly Cost |
|-----------|---------|--------------|
| Collector | Railway (Hobby) | ~$5 |
| Database | Railway Postgres | ~$5-7 |
| **Total** | | **~$10-12** |

Or use Neon.tech free tier for the database to reduce costs further.

## Next Steps (Phase 2+)

- [ ] Web app for querying delays and visualizing routes
- [ ] Compare actual positions to GTFS schedule data for delay calculations
- [ ] Detect route deviations by comparing to expected stop sequences
- [ ] Add alerts for significant delays
