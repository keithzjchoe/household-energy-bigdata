-- ============================================================
-- Household Energy Consumption — Apache Hive Queries
-- Dataset: UCI Household Power Consumption
-- Environment: Cloudera QuickStart VM
-- ============================================================

-- ------------------------------------------------------------
-- 1. Create External Table
-- ------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS power_consumption (
    dt STRING,
    intensity STRING,
    sub1 STRING,
    sub2 STRING,
    sub3 STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE
LOCATION '/user/cloudera/energy_data/'
TBLPROPERTIES ("skip.header.line.count"="1");


-- ------------------------------------------------------------
-- 2. Daily Total Active Power Consumption
-- Handles missing '?' values via CASE WHEN normalisation
-- ------------------------------------------------------------
SELECT dt,
    ROUND(SUM(CAST(CASE WHEN active_p = '?' THEN NULL ELSE active_p END AS DOUBLE)), 2) AS daily_sum
FROM power_consumption
GROUP BY dt
LIMIT 10;


-- ------------------------------------------------------------
-- 3. Average Energy Usage by Household Area
-- Kitchen (sub1), Laundry (sub2), HVAC (sub3)
-- ------------------------------------------------------------
SELECT 'Kitchen' AS area,
    AVG(CAST(CASE WHEN sub1 = '?' THEN NULL ELSE sub1 END AS DOUBLE)) AS avg_usage
FROM power_consumption
UNION ALL
SELECT 'Laundry',
    AVG(CAST(CASE WHEN sub2 = '?' THEN NULL ELSE sub2 END AS DOUBLE))
FROM power_consumption
UNION ALL
SELECT 'HVAC',
    AVG(CAST(CASE WHEN sub3 = '?' THEN NULL ELSE sub3 END AS DOUBLE))
FROM power_consumption;


-- ------------------------------------------------------------
-- 4. Voltage Range Analysis (Min / Max)
-- ------------------------------------------------------------
SELECT
    MAX(CAST(CASE WHEN voltage = '?' THEN NULL ELSE voltage END AS DOUBLE)) AS max_v,
    MIN(CAST(CASE WHEN voltage = '?' THEN NULL ELSE voltage END AS DOUBLE)) AS min_v
FROM power_consumption;
