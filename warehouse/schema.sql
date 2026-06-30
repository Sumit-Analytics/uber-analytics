-- ============================================================
-- warehouse_schema.sql
-- Uber Analytics — Star Schema DDL + Analytical Query Library
-- ============================================================


-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_zones (
    zone_id      SERIAL PRIMARY KEY,
    zone_name    VARCHAR(80)  NOT NULL UNIQUE,
    city         VARCHAR(60)  NOT NULL DEFAULT 'New York',
    region       VARCHAR(40),
    lat_center   DECIMAL(9,6),
    lon_center   DECIMAL(9,6),
    zone_type    VARCHAR(20)  CHECK (zone_type IN ('airport','downtown','residential','commercial','suburban','other')),
    created_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_drivers (
    driver_id        VARCHAR(36)  PRIMARY KEY,
    first_name       VARCHAR(60),
    last_name        VARCHAR(60),
    vehicle_type     VARCHAR(20)  CHECK (vehicle_type IN ('uberx','comfort','xl','black','green')),
    vehicle_year     SMALLINT,
    onboarded_date   DATE,
    home_zone_id     INT          REFERENCES dim_zones(zone_id),
    avg_rating       DECIMAL(3,2),
    total_trips      INT          DEFAULT 0,
    is_active        BOOLEAN      DEFAULT TRUE,
    updated_at       TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_riders (
    rider_id          VARCHAR(36)  PRIMARY KEY,
    signup_date       DATE,
    preferred_payment VARCHAR(20)  CHECK (preferred_payment IN ('card','cash','wallet','voucher')),
    is_subscriber     BOOLEAN      DEFAULT FALSE,
    home_zone_id      INT          REFERENCES dim_zones(zone_id),
    lifetime_trips    INT          DEFAULT 0,
    updated_at        TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key        INT          PRIMARY KEY,  -- YYYYMMDDHH integer surrogate
    date_actual     DATE         NOT NULL,
    hour_of_day     SMALLINT     NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    day_of_week     VARCHAR(10)  NOT NULL,
    day_of_week_num SMALLINT     NOT NULL CHECK (day_of_week_num BETWEEN 0 AND 6),
    week_number     SMALLINT     NOT NULL,
    month_num       SMALLINT     NOT NULL CHECK (month_num BETWEEN 1 AND 12),
    month_name      VARCHAR(10)  NOT NULL,
    quarter         SMALLINT     NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year_num        SMALLINT     NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    is_peak_hour    BOOLEAN      NOT NULL,  -- 7-9am, 5-7pm
    is_late_night   BOOLEAN      NOT NULL,  -- 10pm-4am
    is_holiday      BOOLEAN      NOT NULL DEFAULT FALSE
);


-- ============================================================
-- FACT TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id           VARCHAR(36)    PRIMARY KEY,
    driver_id         VARCHAR(36)    NOT NULL REFERENCES dim_drivers(driver_id),
    rider_id          VARCHAR(36)    NOT NULL REFERENCES dim_riders(rider_id),
    pickup_zone_id    INT            NOT NULL REFERENCES dim_zones(zone_id),
    dropoff_zone_id   INT            NOT NULL REFERENCES dim_zones(zone_id),
    time_key          INT            NOT NULL REFERENCES dim_time(time_key),

    -- Temporal
    start_time        TIMESTAMPTZ    NOT NULL,
    end_time          TIMESTAMPTZ    NOT NULL,
    duration_minutes  DECIMAL(6,1)   NOT NULL CHECK (duration_minutes > 0),

    -- Financials
    base_fare         DECIMAL(8,2)   NOT NULL,
    surge_multiplier  DECIMAL(4,2)   NOT NULL DEFAULT 1.0,
    gross_revenue     DECIMAL(8,2)   NOT NULL,
    platform_fee      DECIMAL(8,2)   NOT NULL,
    driver_payout     DECIMAL(8,2)   NOT NULL,

    -- Geography
    pickup_lat        DECIMAL(9,6)   NOT NULL,
    pickup_lon        DECIMAL(9,6)   NOT NULL,
    dropoff_lat       DECIMAL(9,6)   NOT NULL,
    dropoff_lon       DECIMAL(9,6)   NOT NULL,
    trip_distance_km  DECIMAL(7,2)   NOT NULL CHECK (trip_distance_km > 0),
    avg_speed_kmh     DECIMAL(6,1),

    -- Categorical
    vehicle_type      VARCHAR(20)    NOT NULL,
    payment_method    VARCHAR(20)    NOT NULL,
    rating_by_rider   DECIMAL(2,1),

    -- ETL metadata
    loaded_at         TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    pipeline_run_id   VARCHAR(36)
);

-- ============================================================
-- INDEXES (query performance)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fact_trips_start_time      ON fact_trips (start_time);
CREATE INDEX IF NOT EXISTS idx_fact_trips_driver_id        ON fact_trips (driver_id);
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_zone      ON fact_trips (pickup_zone_id);
CREATE INDEX IF NOT EXISTS idx_fact_trips_time_key         ON fact_trips (time_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_vehicle_type     ON fact_trips (vehicle_type);


-- ============================================================
-- ANALYTICAL VIEWS (Power BI connects to these)
-- ============================================================

-- 1. Daily revenue summary
CREATE OR REPLACE VIEW vw_daily_revenue AS
SELECT
    t.date_actual                         AS date,
    COUNT(*)                              AS total_trips,
    SUM(f.gross_revenue)                  AS gross_revenue,
    SUM(f.platform_fee)                   AS platform_revenue,
    SUM(f.driver_payout)                  AS driver_payouts,
    ROUND(AVG(f.base_fare)::NUMERIC, 2)   AS avg_fare,
    ROUND(AVG(f.surge_multiplier)::NUMERIC, 3) AS avg_surge,
    ROUND(AVG(f.duration_minutes)::NUMERIC, 1) AS avg_duration_min,
    ROUND(AVG(f.trip_distance_km)::NUMERIC, 2) AS avg_distance_km
FROM fact_trips f
JOIN dim_time   t ON f.time_key = t.time_key
GROUP BY t.date_actual
ORDER BY t.date_actual;


-- 2. Peak-hour demand heatmap
CREATE OR REPLACE VIEW vw_hourly_heatmap AS
SELECT
    t.day_of_week,
    t.day_of_week_num,
    t.hour_of_day,
    COUNT(*)                                  AS trip_count,
    ROUND(AVG(f.surge_multiplier)::NUMERIC, 2) AS avg_surge,
    ROUND(AVG(f.gross_revenue)::NUMERIC, 2)    AS avg_revenue
FROM fact_trips f
JOIN dim_time   t ON f.time_key = t.time_key
GROUP BY t.day_of_week, t.day_of_week_num, t.hour_of_day
ORDER BY t.day_of_week_num, t.hour_of_day;


-- 3. Zone-level demand analysis
CREATE OR REPLACE VIEW vw_zone_demand AS
SELECT
    pz.zone_name                              AS pickup_zone,
    dz.zone_name                              AS dropoff_zone,
    COUNT(*)                                  AS trip_count,
    SUM(f.gross_revenue)                      AS total_revenue,
    ROUND(AVG(f.base_fare)::NUMERIC, 2)       AS avg_fare,
    ROUND(AVG(f.duration_minutes)::NUMERIC,1) AS avg_duration_min,
    ROUND(AVG(f.trip_distance_km)::NUMERIC,2) AS avg_distance_km,
    ROUND(AVG(f.surge_multiplier)::NUMERIC,3) AS avg_surge
FROM fact_trips f
JOIN dim_zones pz ON f.pickup_zone_id  = pz.zone_id
JOIN dim_zones dz ON f.dropoff_zone_id = dz.zone_id
GROUP BY pz.zone_name, dz.zone_name
ORDER BY trip_count DESC;


-- 4. Driver performance leaderboard
CREATE OR REPLACE VIEW vw_driver_performance AS
SELECT
    d.driver_id,
    d.vehicle_type,
    COUNT(f.trip_id)                              AS total_trips,
    SUM(f.driver_payout)                          AS total_earnings,
    ROUND(AVG(f.rating_by_rider)::NUMERIC, 2)     AS avg_rating,
    ROUND(AVG(f.duration_minutes)::NUMERIC, 1)    AS avg_trip_min,
    -- Idle time proxy: avg gap between consecutive trips (minutes)
    ROUND(
        EXTRACT(EPOCH FROM (
            AVG(
                LEAD(f.start_time) OVER (PARTITION BY f.driver_id ORDER BY f.start_time)
                - f.end_time
            )
        )) / 60
    ::NUMERIC, 1) AS avg_idle_min
FROM fact_trips f
JOIN dim_drivers d ON f.driver_id = d.driver_id
GROUP BY d.driver_id, d.vehicle_type
ORDER BY total_trips DESC;


-- 5. Surge event analysis
CREATE OR REPLACE VIEW vw_surge_events AS
SELECT
    t.date_actual,
    t.day_of_week,
    t.hour_of_day,
    pz.zone_name                              AS pickup_zone,
    COUNT(*)                                  AS surge_trips,
    ROUND(AVG(f.surge_multiplier)::NUMERIC, 2) AS avg_surge_mult,
    ROUND(MAX(f.surge_multiplier)::NUMERIC, 2) AS max_surge_mult,
    SUM(f.gross_revenue)                      AS surge_revenue
FROM fact_trips f
JOIN dim_time  t  ON f.time_key = t.time_key
JOIN dim_zones pz ON f.pickup_zone_id = pz.zone_id
WHERE f.surge_multiplier > 1.2
GROUP BY t.date_actual, t.day_of_week, t.hour_of_day, pz.zone_name
ORDER BY surge_revenue DESC;