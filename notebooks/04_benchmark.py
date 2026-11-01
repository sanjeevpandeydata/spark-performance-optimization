# Databricks notebook source
# ============================================================
# NOTEBOOK : 04_benchmark.py
# PURPOSE  : Run the like-for-like comparison between the
#            baseline and optimised layouts, repeatedly, and
#            record the results. This is the only notebook in
#            the repo that claims a measured result — and it
#            claims only what it measures on your cluster, on
#            your run date, at your data volume.
#
#            Each query runs once as a warm-up (excluded),
#            then `runs` times. The median elapsed time is
#            the reported figure. A single timed run is not
#            a benchmark: the first run pays for file listing,
#            caching, and cluster warm-up.
#
# NOTE: Do not copy
#            numbers from a run of this notebook into a CV
#            or README without the cluster, Spark version,
#            row count, and run date they were measured on.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
import statistics
import time
from datetime import date

import pyspark.sql.functions as F
from pyspark.sql import Row

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("base_path", "")
base_path = dbutils.widgets.get("base_path") or "/tmp/spark_perf_benchmark"

dbutils.widgets.text("runs", "")
runs = int(dbutils.widgets.get("runs") or "3")

# COMMAND ----------
# ── Helpers ───────────────────────────────────────────────────
def count_files(path):
    total_files, total_bytes = 0, 0
    stack = [path]
    while stack:
        current = stack.pop()
        for entry in dbutils.fs.ls(current):
            if entry.isDir():
                stack.append(entry.path)
            else:
                total_files += 1
                total_bytes += entry.size
    return total_files, total_bytes


def files_touched(df):
    """Files Spark would read for this filtered DataFrame.
    Returns -1 when the information is unavailable, rather
    than failing the benchmark."""
    try:
        return len(df.inputFiles())
    except Exception:
        return -1


def median_timed(query, n):
    """Warm-up once, then time n runs and return the median
    elapsed time and the row count from the last run."""
    query.count()  # warm-up, excluded from the median
    elapsed = []
    rows = 0
    for _ in range(n):
        start = time.time()
        rows = query.count()
        elapsed.append(time.time() - start)
    return rows, statistics.median(elapsed)


# COMMAND ----------
# ── Queries (identical for every layout) ──────────────────────
def build_queries(df, products_df=None):
    queries = {}

    queries["Q1_point_lookup"] = (
        df.filter((F.col("customer_id") == "CUST012345")
                  & (F.col("sale_date") >= "2024-01-01")
                  & (F.col("sale_date") < "2024-04-01"))
          .select("customer_id", "region", "sale_date", "sale_amount"))

    queries["Q2_region_month_aggregate"] = (
        df.filter((F.col("region") == "North")
                  & (F.col("sale_year") == 2024)
                  & (F.col("sale_month") == 1))
          .groupBy("channel")
          .agg(F.round(F.sum("sale_amount"), 2).alias("total_sales"),
               F.count("*").alias("transactions")))

    queries["Q3_date_range_scan"] = (
        df.filter((F.col("sale_date") >= "2024-01-01")
                  & (F.col("sale_date") < "2024-04-01"))
          .groupBy("region")
          .agg(F.round(F.sum("sale_amount"), 2).alias("total_sales")))

    if products_df is not None:
        # Q4: join with a 10-row dimension. Without a broadcast
        # hint Spark may shuffle the large fact table; with
        # broadcast() the small dimension is sent to every
        # executor instead. Only the needed columns are
        # selected, so no unused column is read or shuffled.
        queries["Q4_join_no_hint"] = (
            df.select("product_id", "sale_amount")
              .join(products_df, on="product_id", how="inner")
              .groupBy("product_name")
              .agg(F.round(F.sum("sale_amount"), 2).alias("total_sales")))

        queries["Q4_join_broadcast"] = (
            df.select("product_id", "sale_amount")
              .join(F.broadcast(products_df), on="product_id", how="inner")
              .groupBy("product_name")
              .agg(F.round(F.sum("sale_amount"), 2).alias("total_sales")))

    return queries


# COMMAND ----------
# ── Dimension table (10 rows, invented) ───────────────────────
products_df = spark.createDataFrame([
    Row(product_id="PROD101", product_name="Laptop Pro X"),
    Row(product_id="PROD102", product_name="Wireless Mouse"),
    Row(product_id="PROD103", product_name="USB-C Hub"),
    Row(product_id="PROD104", product_name="Mechanical Keyboard"),
    Row(product_id="PROD105", product_name="4K Monitor"),
    Row(product_id="PROD106", product_name="Webcam HD"),
    Row(product_id="PROD107", product_name="Headphones Pro"),
    Row(product_id="PROD108", product_name="SSD 1TB"),
    Row(product_id="PROD109", product_name="Smart Speaker"),
    Row(product_id="PROD110", product_name="Tablet 10inch"),
])

# COMMAND ----------
# ── Run the benchmark ─────────────────────────────────────────
layouts = {
    "baseline": f"{base_path}/baseline_sales",
    "optimised": f"{base_path}/optimised_sales",
}

results = []
for layout, path in layouts.items():
    df = spark.read.format("delta").load(path)
    table_files, table_bytes = count_files(path)
    queries = build_queries(df, products_df)

    for query_name, query in queries.items():
        rows, median_seconds = median_timed(query, runs)
        results.append({
            "run_date": str(date.today()),
            "layout": layout,
            "query": query_name,
            "runs": runs,
            "median_seconds": round(median_seconds, 3),
            "rows_returned": rows,
            "files_touched": files_touched(query),
            "files_in_table": table_files,
            "table_size_mb": round(table_bytes / 1024 / 1024, 1),
            "spark_version": spark.version,
        })
        print(f"{layout} {query_name}: median {median_seconds:.3f}s "
              f"over {runs} runs, {rows:,} rows")

# COMMAND ----------
# ── Record the results ────────────────────────────────────────
# Results are written where you ran them, under base_path.
# They are NOT committed to this repository with invented
# values: copy a measured results file into results/ only
# after a run, labelled with its environment and date.
# See results/README.md in this repo.

metrics_df = spark.createDataFrame(results)

metrics_path = f"{base_path}/benchmark_metrics"
metrics_df.write.format("delta").mode("overwrite").save(metrics_path)

(metrics_df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(f"{base_path}/benchmark_results_csv"))

print(f"Benchmark metrics written to: {metrics_path}")
metrics_df.orderBy("layout", "query").show(truncate=False)
