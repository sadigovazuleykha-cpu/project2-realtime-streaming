-- =============================================================================
-- Project 2 — Real-Time Streaming & Dashboards
-- Serving schema: curated aircraft state + windowed metrics
-- Auto-run by Postgres on first container start (docker-entrypoint-initdb.d)
-- =============================================================================

-- One row per aircraft state vector ingested. Backs the Metabase heat map,
-- callsign lookups, and general drill-down.
CREATE TABLE IF NOT EXISTS aircraft_state (
    icao24          VARCHAR(6)        NOT NULL,
    callsign        VARCHAR(8),
    origin_country  VARCHAR(100),
    longitude       DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,
    baro_altitude   DOUBLE PRECISION,
    velocity        DOUBLE PRECISION,
    true_track      DOUBLE PRECISION,
    vertical_rate   DOUBLE PRECISION,
    on_ground       BOOLEAN,
    event_time      TIMESTAMP         NOT NULL,   -- OpenSky `time_position`
    ingested_at     TIMESTAMP         NOT NULL DEFAULT now(),
    PRIMARY KEY (icao24, event_time)
);

CREATE INDEX IF NOT EXISTS idx_aircraft_state_event_time ON aircraft_state (event_time);
CREATE INDEX IF NOT EXISTS idx_aircraft_state_callsign    ON aircraft_state (callsign);

-- Windowed aggregates produced by the Spark Structured Streaming job:
-- active flights, average altitude, vertical-rate anomaly counts per window.
CREATE TABLE IF NOT EXISTS flight_window_metrics (
    window_start              TIMESTAMP NOT NULL,
    window_end                TIMESTAMP NOT NULL,
    active_flights            INT,
    avg_altitude              DOUBLE PRECISION,
    vertical_rate_anomalies   INT,
    PRIMARY KEY (window_start, window_end)
);

-- NOTE: this is a first draft. We'll revisit column names/types once the
-- Spark job (Step 3) is written, in case the aggregation shape changes.