# Databricks notebook source
# ============================================================
# NOTEBOOK : 02_baseline_unoptimised.py
# PURPOSE  : Build the "before" layout and run the three
#            representative queries against it, once, so the
#            cost of the naive layout is visible before any
#            optimisation is applied.
#
#            Layout: Delta, no partitioning, no Z-ordering,
#            no OPTIMIZE. This notebook changes nothing about
#            the data itself — only how it is laid out.
#
# NOTE: Timings here
#            are illustrative for your cluster only; the
#            formal comparison (median of repeated runs) is
#            in 04_benchmark.py.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
import time

import pyspark.sql.functions as F

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("base_path", "")
base_path = dbutils.widgets.get("base_path") or "/tmp/spark_perf_benchmark"

# COMMAND ----------
# ── Helpers ───────────────────────────────────────────────────
def count_files(path):
    """Recursively count files under a Delta table path."""
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


def timed_count(df):
    """Time a single action. Only the (small) aggregate result
    is materialised on the driver; the data stays distributed."""
    start = time.time()
    rows = df.count()
    return rows, time.time() - start


# COMMAND ----------
# ── Build the baseline layout ─────────────────────────────────
raw_path = f"{base_path}/raw_sales"
baseline_path = f"{base_path}/baseline_sales"

df_raw = spark.read.format("delta").load(raw_path)

(df_raw.repartition(200)
        .write
        .format("delta")
        .mode("overwrite")
        .save(baseline_path))

files, size_bytes = count_files(baseline_path)
print(f"Baseline layout: {files:,} files, {size_bytes / 1024 / 1024:.1f} MB")

# COMMAND ----------
# ── The three representative queries ──────────────────────────
# Q1: point lookup — one customer, one quarter.
# Q2: partition-shaped aggregate — one region, one month.
# Q3: date-range scan — one quarter, all regions.
# The same three queries run, unchanged, against every
# layout in this repo, so the comparison is like-for-like.

df = spark.read.format("delta").load(baseline_path)

q1 = (df.filter((F.col("customer_id") == "CUST012345")
                & (F.col("sale_date") >= "2024-01-01")
                & (F.col("sale_date") < "2024-04-01"))
        .select("customer_id", "region", "sale_date", "sale_amount"))

q2 = (df.filter((F.col("region") == "North")
                & (F.col("sale_year") == 2024)
                & (F.col("sale_month") == 1))
        .groupBy("channel")
        .agg(F.round(F.sum("sale_amount"), 2).alias("total_sales"),
             F.count("*").alias("transactions")))

q3 = (df.filter((F.col("sale_date") >= "2024-01-01")
                & (F.col("sale_date") < "2024-04-01"))
        .groupBy("region")
        .agg(F.round(F.sum("sale_amount"), 2).alias("total_sales")))

for name, query in [("Q1 point lookup", q1),
                    ("Q2 region-month aggregate", q2),
                    ("Q3 date-range scan", q3)]:
    rows, elapsed = timed_count(query)
    print(f"Baseline {name}: {rows:,} rows in {elapsed:.2f}s")

print("Next: run 03_optimised_partition_zorder.py.")
