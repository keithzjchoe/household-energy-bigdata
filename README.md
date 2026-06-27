# Household Energy Consumption : Big Data Analytics

A big data analytics project analysing over 2 million records of household electricity consumption using the full Hadoop ecosystem (Apache Hive, Apache Spark SQL, and Java MapReduce) deployed on a Cloudera QuickStart VM.

---

## Project Overview

This project demonstrates distributed batch analytics on the UCI Household Power Consumption dataset using three big data processing frameworks. The same analytical task (daily energy aggregation) was implemented across all three frameworks to enable a direct performance comparison.

Key analyses include:
- Daily total active power consumption aggregation
- Average energy usage by household area (Kitchen, Laundry, HVAC)
- Voltage range analysis (min/max)
- Framework-to-framework performance benchmarking

---

## Dataset

**Source:** [Individual Household Electric Power Consumption](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption) - UCI Machine Learning Repository

| Column | Description |
|---|---|
| `Date` | Date of measurement |
| `Time` | Time of measurement |
| `Global_active_power` | Household global minute-averaged active power (kW) |
| `Global_reactive_power` | Household global minute-averaged reactive power (kW) |
| `Voltage` | Minute-averaged voltage (V) |
| `Global_intensity` | Household global minute-averaged current intensity (A) |
| `Sub_metering_1` | Kitchen appliances energy consumption |
| `Sub_metering_2` | Laundry room appliances energy consumption |
| `Sub_metering_3` | Water heater and air conditioner energy consumption |

- **Total records:** 2,075,260 (minute-by-minute readings)
- **Period covered:** December 2006 - November 2010
- **File size:** 126.8 MB
- **Missing values:** 25,980 records (1.25%) marked as `?` — handled via query-level normalisation

---

## Architecture

The project follows a layered architecture deployed on **Cloudera QuickStart VM**:

```
┌─────────────────────────────────────────┐
│         Analytical Layer                │
│   Apache Hive │ Spark SQL │ MapReduce   │
├─────────────────────────────────────────┤
│         Storage Layer                   │
│   HDFS (Replication Factor: 3)          │
│   External Table — Schema-on-Read       │
└─────────────────────────────────────────┘
```

**Key design decisions:**
- **External Table** in Hive : decouples metadata from raw CSV files, prevents accidental deletion, allows multiple engines to access the same HDFS data
- **Schema-on-Read** : raw telemetry stored in native format in HDFS, schema applied only at query time
- **HDFS Replication Factor 3** : ensures fault tolerance, each data block copied across three nodes
- **Query-level normalisation** — `CASE WHEN active_p = '?' THEN NULL ELSE CAST(active_p AS DOUBLE)` handles missing `?` values inline, avoiding a separate preprocessing stage

---

## Implementations

### Apache Hive (HiveQL)

Daily total active power aggregation with query-level normalisation:

```sql
SELECT dt,
  ROUND(SUM(CAST(CASE WHEN active_p = '?' THEN NULL ELSE active_p END AS DOUBLE)), 2) AS daily_sum
FROM power_consumption
GROUP BY dt
LIMIT 10;
```

Average energy usage by household area:

```sql
SELECT 'Kitchen' AS area, AVG(CAST(CASE WHEN sub1 = '?' THEN NULL ELSE sub1 END AS DOUBLE)) AS avg_usage
FROM power_consumption
UNION ALL
SELECT 'Laundry', AVG(CAST(CASE WHEN sub2 = '?' THEN NULL ELSE sub2 END AS DOUBLE))
FROM power_consumption
UNION ALL
SELECT 'HVAC', AVG(CAST(CASE WHEN sub3 = '?' THEN NULL ELSE sub3 END AS DOUBLE))
FROM power_consumption;
```

### Spark SQL (Scala)

Same aggregation executed via Spark Shell using temporary views over HDFS external tables:

```scala
sqlContext.sql("""
  SELECT dt,
    ROUND(SUM(CAST(CASE WHEN active_p='?' THEN NULL ELSE active_p END AS DOUBLE)), 2) AS daily_sum
  FROM power_consumption_spark
  GROUP BY dt
""").show()
```

### Java MapReduce

A custom Java MapReduce job (`EnergyMapper`, `EnergyReducer`, `EnergyDriver`) processes the full 2M+ record dataset:

- **Map Phase:** Parses each line, filters `?` values and malformed rows, emits `(date, global_active_power)` key-value pairs : 2,049,280 valid records emitted
- **Shuffle & Sort:** Hadoop groups all values by date key automatically
- **Reduce Phase:** Sums all power values per date, rounds to 2 decimal places, writes 1,433 unique daily totals to HDFS

**Job execution counters:**

| Counter | Value |
|---|---|
| Map input records | 2,075,260 |
| Map output records | 2,049,280 |
| Records filtered (missing values) | 25,980 |
| Reduce output records (unique dates) | 1,433 |
| HDFS bytes read | 132,960,909 |
| Total map time | 7,789 ms |
| Total reduce time | 6,745 ms |

---

## Results

### Energy Insights

| Area | Average Energy Usage |
|---|---|
| Kitchen | ~1.12 kW |
| Laundry | ~1.30 kW |
| HVAC | ~6.46 kW |

HVAC dominates household energy consumption at nearly 5× the kitchen usage, highlighting it as the primary target for energy-saving interventions.

**Voltage range:** Min 223.2V — Max 254.15V

**Peak daily consumption:** December 2007 recorded the highest single-day usage at 3,134.95 Wh, consistent with increased winter heating demand.

### Framework Performance Comparison

The same aggregation query was executed 5 times per framework and averaged:

| Framework | Mean Execution Time |
|---|---|
| Apache Spark SQL | **2.76s** ✅ Fastest |
| Hadoop MapReduce | 9.95s |
| Apache Hive | 21.10s |

**Why the difference:**
- **Spark** processes data in memory, minimising disk I/O, ideal for iterative and repeated queries
- **MapReduce** writes intermediate results to disk between map and reduce stages, efficient for large-scale batch but slower than Spark
- **Hive** translates SQL to MapReduce internally, adding an extra translation layer overhead

---

## Tech Stack

- Apache Hadoop (HDFS, MapReduce)
- Apache Hive (HiveQL)
- Apache Spark (Spark SQL, Scala)
- Java (MapReduce job)
- Cloudera QuickStart VM
- Python + Matplotlib (visualisations)

---

## Environment

This project was developed and executed entirely within a **Cloudera QuickStart VM** (VirtualBox). There are no runnable notebooks — all queries were executed directly in the Hive CLI and Spark Shell, and the MapReduce job was compiled and submitted as a JAR file.

To replicate:
1. Set up Cloudera QuickStart VM
2. Download the UCI dataset and upload to HDFS: `hdfs dfs -put household_power_consumption.txt /user/cloudera/ASM2/`
3. Create the external table in Hive and run the HiveQL queries
4. Load the dataset into Spark Shell and execute Spark SQL queries
5. Compile `EnergyMapper.java`, `EnergyReducer.java`, `EnergyDriver.java` into a JAR and submit via `hadoop jar`

---

## Acknowledgements

Group project : Taylor's University, Bachelor of Computer Science.
Dataset: UCI Machine Learning Repository — Individual Household Electric Power Consumption (Hebrail & Berard, 2012).
