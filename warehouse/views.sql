-- Fix vw_hourly_heatmap (remove day_of_week)
CREATE OR REPLACE VIEW vw_hourly_heatmap AS
SELECT
    hour_of_day,
    is_peak_hour,
    COUNT(*) AS trip_count,
    ROUND(AVG(gross_revenue)::numeric, 2) AS avg_revenue,
    ROUND(SUM(gross_revenue)::numeric, 2) AS total_revenue,
    ROUND(AVG(surge_multiplier)::numeric, 3) AS avg_surge,
    ROUND(AVG(duration_minutes)::numeric, 1) AS avg_duration_min,
    COUNT(DISTINCT driver_id) AS drivers_active
FROM fact_trips
GROUP BY hour_of_day, is_peak_hour
ORDER BY hour_of_day;

-- Fix vw_surge_events (remove day_of_week)
CREATE OR REPLACE VIEW vw_surge_events AS
SELECT
    date,
    hour_of_day,
    pickup_zone,
    is_peak_hour,
    COUNT(*) AS surge_trips,
    ROUND(AVG(surge_multiplier)::numeric, 3) AS avg_surge_mult,
    ROUND(MAX(surge_multiplier)::numeric, 2) AS max_surge_mult,
    ROUND(SUM(gross_revenue)::numeric, 2) AS surge_revenue,
    COUNT(*) FILTER (WHERE surge_multiplier BETWEEN 1.2 AND 1.5) AS tier_low,
    COUNT(*) FILTER (WHERE surge_multiplier BETWEEN 1.5 AND 2.0) AS tier_mid,
    COUNT(*) FILTER (WHERE surge_multiplier > 2.0) AS tier_high
FROM fact_trips
WHERE surge_multiplier > 1.2
GROUP BY date, hour_of_day, pickup_zone, is_peak_hour
ORDER BY surge_revenue DESC;

-- Fix fn_warehouse_health (use pg_class instead of oid directly)
CREATE OR REPLACE FUNCTION fn_warehouse_health()
RETURNS TABLE (table_name TEXT, row_count BIGINT, table_size TEXT)
LANGUAGE sql AS
$$
    SELECT
        s.relname::TEXT,
        s.n_live_tup::BIGINT,
        pg_size_pretty(pg_total_relation_size(c.oid))
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.relname = s.relname
    WHERE s.relname IN (
        'fact_trips','dim_zones','dim_drivers',
        'dim_riders','dim_time','quarantine_trips'
    )
    ORDER BY s.n_live_tup DESC;
$$;