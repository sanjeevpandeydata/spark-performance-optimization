# Databricks notebook source
# ============================================================
# NOTEBOOK : 03_optimised_partition_zorder.py
# PURPOSE  : Build the "after" layout from the same source
#            data and run the same three queries as notebook 02.
#
#            Layout: partitioned by (region, sale_year,
#            sale_month), then OPTIMIZE with ZORDER BY
#            (customer_id) to co-locate one customer's rows
#            inside each partition.
#
#            VACUUM is optional and off by default: it deletes
#            old data files, which is only safe here because
#            every path under base_path is generated data.
#
# NOTE: See
#            04_benchmark.py for the repeated, median-based
#            comparison.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
import time

import pyspark.sql.functions as F

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("base_path", "")
base_path = dbutils.widgets.get("base_path") or "/tmp/spark_perf_benchmark"

dbutils.widgets.text("confirm_vacuum", "")
confirm_vacuum = (dbutils.widgets.get("confirm_vacuum") or "false").lower()

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
    start = time.time()
    rows = df.count()
    return rows, time.time() - start


# COMMAND ----------
# ── Build the optimised layout ────────────────────────────────
raw_path = f"{base_path}/raw_sales"
optimised_path = f"{base_path}/optimised_sales"

df_raw = spark.read.format("delta").load(raw_path)

# Partitioning matches Q2's filter shape (region + year/month)
# and Q3's date filter (via the derived year/month columns).
# It does not help Q1 on its own: that is what Z-ordering
# is for, below.
(df_raw.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("region", "sale_year", "sale_month")
        .save(optimised_path))

# ZORDER co-locates rows for the same customer_id inside
# each partition, so Q1's point lookup can skip whole files
# instead of scanning every file in the matching partitions.
# OPTIMIZE also compacts the many small files written by
# the partitioned write into fewer, larger ones.
spark.sql(f"OPTIMIZE delta.`{optimised_path}` ZORDER BY (customer_id)")

files, size_bytes = count_files(optimised_path)
print(f"Optimised layout: {files:,} files, {size_bytes / 1024 / 1024:.1f} MB")

# COMMAND ----------
# ── Optional VACUUM (off by default) ──────────────────────────
# VACUUM deletes data files that are no longer referenced by
# the Delta log. It is only safe here because optimised_sales
# is generated data under base_path. Never run VACUUM RETAIN 0
# HOURS against a production table without checking for
# concurrent readers and time-travel requirements first.
if confirm_vacuum == "true":
    spark.sql(f"VACUUM delta.`{optimised_path}` RETAIN 0 HOURS")
    print("VACUUM complete (generated data only).")
else:
    print("VACUUM skipped: set confirm_vacuum=true to run it.")

# COMMAND ----------
# ── Same three queries as notebook 02, unchanged ──────────────
df = spark.read.format("delta").load(optimised_path)

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
    print(f"Optimised {name}: {rows:,} rows in {elapsed:.2f}s")

print("Next: run 04_benchmark.py for the repeated comparison.")
