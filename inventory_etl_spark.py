# Purpose: Supply Chain Inventory Analytics Pipeline (PySpark on Dataproc)

# Flow: GCS (raw) -> Spark (clean/enrich/KPIs) -> BigQuery (fact)
#
# Required connectors (Dataproc/Glue/EMR):
#    - spark-bigquery (e.g., gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar)
#    - GCS connector bundled on Dataproc
#
# Submit (Dataproc):
# gcloud dataproc jobs submit pyspark inventory_etl_spark.py \
#    --cluster=<DATAPROC_CLUSTER> \
#    --region=<REGION> \
#    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
#    -- \
#    --project_id prj-analytics-123 \
#    --bq_dataset supply_chain \
#    --bq_fact_table inventory_fact \
#    --bq_product_table ref.product_dim \
#    --bq_supplier_table ref.supplier_dim \
#    --bq_fx_table ref.fx_rates_daily \
#    --gcs_input gs://whirlpool-raw/inventory/date=2025-10-01/* \
#    --temp_gcs_bucket whirlpool-tmp-bq \
#    --run_date 2025-10-01

import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--project_id", required=True)
    p.add_argument("--bq_dataset", required=True)
    p.add_argument("--bq_fact_table", default="inventory_fact")
    p.add_argument("--bq_product_table", required=True)   # e.g. ref.product_dim
    p.add_argument("--bq_supplier_table", required=True)  # e.g. ref.supplier_dim
    p.add_argument("--bq_fx_table", required=True)        # e.g. ref.fx_rates_daily
    p.add_argument("--gcs_input", required=True)          # e.g. gs://bucket/raw/inventory/date=YYYY-MM-DD/*
    p.add_argument("--temp_gcs_bucket", required=True)    # temp bucket for BQ connector
    p.add_argument("--run_date", required=True)           # partition date YYYY-MM-DD
    return p.parse_args()

def build_spark(app_name="inventory-etl"):
    return (
        SparkSession.builder
        .appName(app_name)
        # Performance / shuffle tuning – adjust for your cluster size
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def read_raw_inventory(spark, input_path):
    """
    Reads mixed CSV/JSON files. You can restrict to one format by splitting paths.
    """
    inv_schema = T.StructType([
        T.StructField("warehouse_id", T.StringType(), True),
        T.StructField("sku", T.StringType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("on_hand_qty", T.StringType(), True),
        T.StructField("unit_cost", T.StringType(), True),
        T.StructField("avg_daily_sales", T.StringType(), True),
        T.StructField("snapshot_ts", T.TimestampType(), True),  # or string -> to_timestamp
        T.StructField("region", T.StringType(), True),
        T.StructField("source_file", T.StringType(), True),
    ])

    # CSV
    csv_df = (
        spark.read
        .option("header", True)
        .schema(inv_schema)
        .csv(f"{input_path}/*.csv")
    )

    # JSON (schema-on-read; coalesce with CSV columns)
    json_df = (
        spark.read
        .json(f"{input_path}/*.json")
        .select(
            F.col("warehouse_id").cast("string"),
            F.col("sku").cast("string"),
            F.col("currency").cast("string"),
            F.col("on_hand_qty").cast("string"),
            F.col("unit_cost").cast("string"),
            F.col("avg_daily_sales").cast("string"),
            F.to_timestamp("snapshot_ts").alias("snapshot_ts"),
            F.col("region").cast("string"),
            F.input_file_name().alias("source_file"),
        )
    )

    inv = csv_df.unionByName(json_df, allowMissingColumns=True)

    # Clean/standardize
    inv = (
        inv.withColumn("warehouse_id", F.trim(F.col("warehouse_id")))
           .withColumn("sku", F.upper(F.trim(F.col("sku"))))
           .withColumn("currency", F.upper(F.trim(F.col("currency"))))
           .withColumn("region", F.upper(F.trim(F.col("region"))))
           .withColumn("on_hand_qty", F.col("on_hand_qty").cast("long"))
           .withColumn("unit_cost", F.col("unit_cost").cast("double"))
           .withColumn("avg_daily_sales", F.col("avg_daily_sales").cast("double"))
           .withColumn("snapshot_ts",
                        F.when(F.col("snapshot_ts").isNull(),
                               F.current_timestamp()).otherwise(F.col("snapshot_ts")))
    )

    # Basic validation
    inv = inv.filter(
        (F.col("warehouse_id").isNotNull()) &
        (F.col("sku").isNotNull()) &
        (F.col("on_hand_qty").isNotNull()) &
        (F.col("unit_cost").isNotNull())
    )

    # Deduplicate latest record per warehouse/sku by snapshot_ts
    w = Window.partitionBy("warehouse_id", "sku").orderBy(F.col("snapshot_ts").desc())
    inv = inv.withColumn("_rk", F.row_number().over(w)).filter(F.col("_rk") == 1).drop("_rk")

    return inv

def read_bigquery_table(spark, project_id, table_fqn):
    # table_fqn like "ref.product_dim"
    return (
        spark.read.format("bigquery")
        .option("table", f"{project_id}.{table_fqn}")
        .load()
    )

def enrich(inv, product_dim, supplier_dim):
    # Minimal set of columns assumed in dims; adjust selects & join keys
    prod = product_dim.select(
        F.upper(F.col("sku")).alias("sku"),
        "product_name",
        "category",
        "uom"
    )

    sup = supplier_dim.select(
        F.col("supplier_id"),
        F.upper(F.col("sku")).alias("sku"),
        F.col("preferred_supplier").cast("boolean").alias("preferred_supplier")
    )

    inv = inv.join(prod, on="sku", how="left")
    inv = inv.join(sup, on="sku", how="left")

    return inv

def attach_fx(inv, fx_rates, run_date):
    """
    We choose the most recent rate <= run_date per currency.
    """
    fx = (
        fx_rates
        .select(
            F.upper(F.col("currency_code")).alias("currency"),
            F.col("rate_to_usd").cast("double"),
            F.col("effective_date").cast("date")
        )
        .filter(F.col("effective_date") <= F.lit(run_date))
    )

    w = Window.partitionBy("currency").orderBy(F.col("effective_date").desc())
    fx_latest = (
        fx.withColumn("_rk", F.row_number().over(w))
          .filter(F.col("_rk") == 1)
          .drop("_rk", "effective_date")
    )

    inv_fx = inv.join(F.broadcast(fx_latest), on="currency", how="left")

    inv_fx = inv_fx.withColumn(
        "inventory_value_local",
        F.col("on_hand_qty") * F.col("unit_cost")
    ).withColumn(
        "inventory_value_usd",
        F.when(F.col("rate_to_usd").isNotNull(),
                F.col("inventory_value_local") * F.col("rate_to_usd")).otherwise(F.lit(None).cast("double"))
    )

    return inv_fx

def compute_kpis(df):
    return (
        df.withColumn(
            "stock_coverage_days",
            F.when(F.col("avg_daily_sales") > 0, F.col("on_hand_qty") / F.col("avg_daily_sales"))
              .otherwise(F.lit(None).cast("double"))
        )
        .withColumn(
            "reorder_flag",
            F.when(F.col("stock_coverage_days") < F.lit(7), F.lit(True)).otherwise(F.lit(False))
        )
    )

def write_to_bigquery(df, project_id, dataset, table, temp_bucket, partition_date):
    # Partition by snapshot_date (derived), cluster by region/category/sku
    # NOTE: 'partition_date' and 'snapshot_date' columns are added here.
    out = (
        df.withColumn("snapshot_date", F.to_date("snapshot_ts"))
          .withColumn("partition_date", F.lit(partition_date).cast("date"))
    )

    (out.write.format("bigquery")
        .option("temporaryGcsBucket", temp_bucket)
        .option("writeMethod", "direct")  # or "indirect" if needed
        .option("table", f"{project_id}.{dataset}.{table}")
        .option("partitionField", "partition_date")
        .option("partitionType", "DAY")
        .option("clusteredFields", "region,category,sku")
        .mode("append")
        .save()
    )

def main():
    args = parse_args()
    spark = build_spark()

    # Read raw
    inv = read_raw_inventory(spark, args.gcs_input)

    # Refs from BigQuery
    product_dim = read_bigquery_table(spark, args.project_id, args.bq_product_table)
    supplier_dim = read_bigquery_table(spark, args.project_id, args.bq_supplier_table)
    fx_rates = read_bigquery_table(spark, args.project_id, args.bq_fx_table)

    # Enrich + FX + KPIs
    inv_enriched = enrich(inv, product_dim, supplier_dim)
    inv_fx = attach_fx(inv_enriched, fx_rates, args.run_date)
    
    # Apply KPIs and select final columns. 
    
    fact = compute_kpis(inv_fx).select(
        "snapshot_ts", "warehouse_id", "region",
        "sku", "product_name", "category", "uom",
        "supplier_id", "preferred_supplier",
        "currency", "unit_cost", "on_hand_qty", "avg_daily_sales",
        "inventory_value_local", "inventory_value_usd",
        "stock_coverage_days", "reorder_flag",
        "source_file"
    )

    write_to_bigquery(
        fact,
        args.project_id,
        args.bq_dataset,
        args.bq_fact_table,
        args.temp_gcs_bucket,
        args.run_date
    )

    spark.stop()

if __name__ == "__main__":
    main()
