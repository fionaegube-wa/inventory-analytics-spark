# Automated Sales & Inventory Analytics Pipeline – Ashley Furniture

##  Overview
This project demonstrates an **end-to-end PySpark ETL pipeline** developed for **Ashley Furniture** to automate sales and inventory data processing.

The pipeline ingests daily raw data from **Google Cloud Storage (GCS)**, performs data **cleansing**, **enrichment**, and **currency normalization**, and writes the processed output to **BigQuery** as a **partitioned and clustered fact table** to support BI and reporting use cases.

The ETL is executed on **Dataproc (PySpark)** and orchestrated via **Cloud Composer (Apache Airflow)**.

---

##  Architecture

     +--------------------+
     |  Google Cloud      |
     |  Storage (GCS)     |
     +---------+----------+
               |
               v
     +--------------------+
     |   PySpark ETL      |
     |   (Dataproc Job)   |
     +---------+----------+
               |
               v
     +--------------------+
     |   BigQuery         |
     |   Fact Table       |
     +---------+----------+
               |
               v
     +--------------------+
     |   Looker / BI      |
     +--------------------+

 Orchestrated via Cloud Composer (Airflow)


**Flow:**  
`GCS → PySpark (Dataproc) → BigQuery`  
with **Airflow** managing job scheduling and monitoring.

---

##  Technical Summary

| Component | Description |
|------------|-------------|
| **Language** | Python (PySpark) |
| **Execution Engine** | Dataproc Cluster (Spark 3.x) |
| **Orchestration** | Cloud Composer (Apache Airflow) |
| **Storage** | Google Cloud Storage (raw input) |
| **Warehouse** | BigQuery (fact & reference tables) |
| **Data Source Types** | CSV, JSON |
| **Frequency** | Daily automated refresh |
| **Output Table** | `bq_ashley_sales.inventory_fact` |

---

##  What the Pipeline Does

1. **Extracts** raw sales and inventory files from GCS.  
2. **Validates and cleanses** data:
   - Removes nulls and malformed records  
   - Normalizes field formats and datatypes  
   - Deduplicates by latest timestamp per SKU  
3. **Enriches** data:
   - Joins with product and customer reference tables in BigQuery  
   - Applies FX-rate conversion to normalize currency values  
4. **Computes KPIs:**
   - Inventory value (local & USD)  
   - Stock coverage days  
   - Reorder flags  
5. **Loads** the curated data into BigQuery as a partitioned and clustered fact table.

---

## Example Script

The main ETL logic resides in [`inventory_etl_spark.py`](inventory_etl_spark.py).

Key Spark transformations include:

```python
# Compute KPIs
df = df.withColumn(
    "stock_coverage_days",
    F.when(F.col("avg_daily_sales") > 0, F.col("on_hand_qty") / F.col("avg_daily_sales"))
).withColumn(
    "reorder_flag",
    F.when(F.col("stock_coverage_days") < 7, F.lit(True)).otherwise(F.lit(False))
)

(out.write.format("bigquery")
    .option("temporaryGcsBucket", args.temp_gcs_bucket)
    .option("partitionField", "partition_date")
    .option("partitionType", "DAY")
    .option("clusteredFields", "region,category,sku")
    .mode("append")
    .save())
