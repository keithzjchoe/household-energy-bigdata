// ============================================================
// Household Energy Consumption — Apache Spark SQL Queries
// Dataset: UCI Household Power Consumption
// Environment: Cloudera QuickStart VM — Spark Shell (Scala)
// ============================================================

// ------------------------------------------------------------
// 1. Create External Table in Spark SQL
// ------------------------------------------------------------
sqlContext.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS power_consumption_spark (
        dt STRING,
        tm STRING,
        active_p STRING,
        reactive_p STRING,
        voltage STRING,
        intensity STRING,
        sub1 STRING,
        sub2 STRING,
        sub3 STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\073'
    STORED AS TEXTFILE
    LOCATION '/user/cloudera/energydata/'
    TBLPROPERTIES ("skip.header.line.count"="1")
""").show()


// ------------------------------------------------------------
// 2. Daily Total Active Power Consumption
// Handles missing '?' values via CASE WHEN normalisation
// ------------------------------------------------------------
sqlContext.sql("""
    SELECT dt,
        ROUND(SUM(CAST(CASE WHEN active_p='?' THEN NULL ELSE active_p END AS DOUBLE)), 2) AS daily_sum
    FROM power_consumption_spark
    GROUP BY dt
    LIMIT 10
""").show()


// ------------------------------------------------------------
// 3. Average Energy Usage by Household Area
// Kitchen (sub1), Laundry (sub2), HVAC (sub3)
// ------------------------------------------------------------
sqlContext.sql("""
    SELECT 'Kitchen' AS area,
        AVG(CAST(CASE WHEN sub1='?' THEN NULL ELSE sub1 END AS DOUBLE)) AS avg_usage
    FROM power_consumption_spark
    UNION ALL
    SELECT 'Laundry',
        AVG(CAST(CASE WHEN sub2='?' THEN NULL ELSE sub2 END AS DOUBLE))
    FROM power_consumption_spark
    UNION ALL
    SELECT 'HVAC',
        AVG(CAST(CASE WHEN sub3='?' THEN NULL ELSE sub3 END AS DOUBLE))
    FROM power_consumption_spark
""").show()


// ------------------------------------------------------------
// 4. Voltage Range Analysis (Min / Max)
// ------------------------------------------------------------
sqlContext.sql("""
    SELECT
        MAX(CAST(CASE WHEN voltage='?' THEN NULL ELSE voltage END AS DOUBLE)) AS max_voltage,
        MIN(CAST(CASE WHEN voltage='?' THEN NULL ELSE voltage END AS DOUBLE)) AS min_voltage
    FROM power_consumption_spark
""").show()


// ------------------------------------------------------------
// 5. Performance Benchmarking — measure execution time
// ------------------------------------------------------------
val startTime = System.nanoTime()
val df = sqlContext.sql("""
    SELECT dt,
        ROUND(SUM(CAST(CASE WHEN active_p='?' THEN NULL ELSE active_p END AS DOUBLE)), 2) AS daily_sum
    FROM power_consumption_spark
    GROUP BY dt
""")
df.show()
val endTime = System.nanoTime()
println(s"Execution Time: ${(endTime - startTime) / 1e9} seconds")
