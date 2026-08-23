-- =============================================================================
-- Athena DDL — ClimateInsights Analytics Platform
-- Database: climateinsights
-- Tables:
--   silver.noaa_observations   (partitioned by year, month)
--   gold.annual_temperature_trends
--   gold.monthly_precipitation
--   gold.station_coverage
--
-- Prerequisites:
--   1. Run CloudFormation stack to create the Glue database and S3 buckets
--   2. Replace REPLACE_WITH_YOUR_BUCKET with actual bucket name
--   3. Set up Athena query results bucket in Athena console
--
-- After creating tables, run MSCK REPAIR TABLE to load existing partitions.
-- New monthly partitions added by Phase 2 require another MSCK REPAIR or
-- ALTER TABLE ADD PARTITION.
-- =============================================================================


-- =============================================================================
-- 0. Select database
-- =============================================================================
CREATE DATABASE IF NOT EXISTS climateinsights
COMMENT 'ClimateInsights climate analytics — Silver and Gold layers'
LOCATION 's3://REPLACE_WITH_YOUR_BUCKET/';

USE climateinsights;


-- =============================================================================
-- 1. Silver Layer — NOAA Observations
--    This is the cleaned Parquet version of the raw NOAA data.
--    Partitioned by year and month for fast predicate pushdown.
--
--    Query tip: ALWAYS include year / month in WHERE clause to avoid
--    full-table scans across 347M+ rows.
--
--    Example:  WHERE year = 2019 AND month = 7 AND element = 'TMAX'
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS climateinsights.silver_noaa_observations (
    station_id   STRING         COMMENT 'GHCN weather station identifier (e.g. USW00094728)',
    obs_date     DATE           COMMENT 'Observation date',
    element      STRING         COMMENT 'Weather element: TMAX | TMIN | PRCP | SNOW',
    value        INT            COMMENT 'Raw value in tenths-of-°C (temps) or tenths-of-mm (PRCP) or mm (SNOW)',
    m_flag       STRING         COMMENT 'Measurement flag — source quality indicator',
    q_flag       STRING         COMMENT 'Quality flag — NULL means passed QC',
    s_flag       STRING         COMMENT 'Source flag — data provenance code',
    obs_time     STRING         COMMENT 'Observation time HHMM (may be blank)',
    country_code STRING         COMMENT 'ISO 2-char country code derived from station_id prefix'
)
PARTITIONED BY (
    year   INT COMMENT 'Observation year  (partition key)',
    month  INT COMMENT 'Observation month (partition key)'
)
STORED AS PARQUET
LOCATION 's3://REPLACE_WITH_YOUR_BUCKET/silver/noaa_parquet/'
TBLPROPERTIES (
    'parquet.compress'             = 'SNAPPY',
    'projection.enabled'           = 'true',
    'projection.year.type'         = 'integer',
    'projection.year.range'        = '2010,2030',
    'projection.year.interval'     = '1',
    'projection.month.type'        = 'integer',
    'projection.month.range'       = '1,12',
    'projection.month.interval'    = '1',
    'storage.location.template'    = 's3://REPLACE_WITH_YOUR_BUCKET/silver/noaa_parquet/year=${year}/month=${month}/',
    'has_encrypted_data'           = 'false'
);
-- Note: Partition projection is used above so MSCK REPAIR is not needed.
-- New partitions are discovered automatically via the projection range.


-- =============================================================================
-- 2. Gold Layer — Annual Temperature Trends
--    One row per (year, element). Values in °C (already converted).
--    Refreshed monthly by the Phase 2 pipeline.
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS climateinsights.gold_annual_temperature_trends (
    year         INT            COMMENT 'Observation year',
    element      STRING         COMMENT 'TMAX or TMIN',
    avg_temp_c   DOUBLE         COMMENT 'Annual average temperature (°C)',
    min_temp_c   DOUBLE         COMMENT 'Annual minimum temperature recorded (°C)',
    max_temp_c   DOUBLE         COMMENT 'Annual maximum temperature recorded (°C)',
    record_count BIGINT         COMMENT 'Number of observations in this year/element'
)
STORED AS PARQUET
LOCATION 's3://REPLACE_WITH_YOUR_BUCKET/gold/annual_temperature_trends/'
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY'
);


-- =============================================================================
-- 3. Gold Layer — Monthly Precipitation
--    One row per (year, month, element). Values in mm.
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS climateinsights.gold_monthly_precipitation (
    year          INT            COMMENT 'Observation year',
    month         INT            COMMENT 'Observation month (1–12)',
    element       STRING         COMMENT 'PRCP or SNOW',
    total_mm      DOUBLE         COMMENT 'Total precipitation/snowfall for the month (mm)',
    avg_mm        DOUBLE         COMMENT 'Average daily precipitation/snowfall (mm)',
    station_count BIGINT         COMMENT 'Number of reporting stations',
    record_count  BIGINT         COMMENT 'Number of observations'
)
STORED AS PARQUET
LOCATION 's3://REPLACE_WITH_YOUR_BUCKET/gold/monthly_precipitation/'
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY'
);


-- =============================================================================
-- 4. Gold Layer — Station Coverage
--    One row per year showing data quality and geographic reach.
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS climateinsights.gold_station_coverage (
    year               INT            COMMENT 'Observation year',
    active_stations    BIGINT         COMMENT 'Number of distinct active weather stations',
    total_records      BIGINT         COMMENT 'Total observation count for the year',
    elements_covered   ARRAY<STRING>  COMMENT 'Distinct elements reported (TMAX, TMIN, PRCP, SNOW)',
    countries_covered  BIGINT         COMMENT 'Number of distinct countries reporting'
)
STORED AS PARQUET
LOCATION 's3://REPLACE_WITH_YOUR_BUCKET/gold/station_coverage/'
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY'
);


-- =============================================================================
-- 5. Athena query result bucket (run once via console or CLI)
-- =============================================================================
-- Set query result location in Athena settings:
--   s3://REPLACE_WITH_YOUR_BUCKET/athena-results/
-- Or create a named workgroup:

CREATE WORKGROUP climateinsights
WITH (
    result_configuration = (
        output_location = 's3://REPLACE_WITH_YOUR_BUCKET/athena-results/'
    ),
    publish_cloudwatch_metrics_enabled = true,
    bytes_scanned_cutoff_per_query = 5368709120,   -- 5 GB per query safety limit
    enforce_workgroup_configuration = true
);


-- =============================================================================
-- 6. Sample analytical queries
-- =============================================================================

-- ── Q1: 10-year annual temperature trend (for QuickSight line chart) ─────────
SELECT
    year,
    element,
    avg_temp_c,
    min_temp_c,
    max_temp_c,
    record_count
FROM climateinsights.gold_annual_temperature_trends
WHERE element = 'TMAX'
ORDER BY year;


-- ── Q2: Global temperature change 2010→2019 ───────────────────────────────
SELECT
    element,
    MAX(CASE WHEN year = 2010 THEN avg_temp_c END) AS avg_2010,
    MAX(CASE WHEN year = 2019 THEN avg_temp_c END) AS avg_2019,
    ROUND(
        MAX(CASE WHEN year = 2019 THEN avg_temp_c END)
      - MAX(CASE WHEN year = 2010 THEN avg_temp_c END),
    2) AS change_c
FROM climateinsights.gold_annual_temperature_trends
GROUP BY element
ORDER BY element;


-- ── Q3: Monthly precipitation seasonality ────────────────────────────────────
SELECT
    month,
    element,
    ROUND(AVG(total_mm), 2) AS avg_monthly_total_mm,
    ROUND(MAX(total_mm), 2) AS peak_total_mm,
    MIN(year)               AS earliest_year,
    MAX(year)               AS latest_year
FROM climateinsights.gold_monthly_precipitation
WHERE element = 'PRCP'
GROUP BY month, element
ORDER BY month;


-- ── Q4: Station network growth ────────────────────────────────────────────────
SELECT
    year,
    active_stations,
    countries_covered,
    total_records,
    ROUND(CAST(total_records AS DOUBLE) / active_stations, 0) AS avg_records_per_station
FROM climateinsights.gold_station_coverage
ORDER BY year;


-- ── Q5: Raw Silver query — warmest day per year (uses partition pruning) ──────
-- This demonstrates why Parquet + partitioning is so much faster than raw CSV.
SELECT
    year,
    MAX(value) / 10.0 AS max_tmax_c,
    MIN(value) / 10.0 AS min_tmin_c
FROM climateinsights.silver_noaa_observations
WHERE element IN ('TMAX', 'TMIN')
  AND year BETWEEN 2010 AND 2019
GROUP BY year
ORDER BY year;


-- ── Q6: Top 10 hottest months globally ───────────────────────────────────────
SELECT
    year,
    month,
    avg_temp_c
FROM climateinsights.gold_annual_temperature_trends
-- Note: monthly granularity would require querying Silver directly
ORDER BY avg_temp_c DESC
LIMIT 10;


-- ── Q7: Drought assessment — lowest precipitation years ──────────────────────
SELECT
    year,
    SUM(total_mm) AS total_annual_prcp_mm
FROM climateinsights.gold_monthly_precipitation
WHERE element = 'PRCP'
GROUP BY year
ORDER BY total_annual_prcp_mm ASC;
