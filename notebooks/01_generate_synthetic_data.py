# Databricks notebook source
# ============================================================
# NOTEBOOK : 01_generate_synthetic_data.py
# PURPOSE  : Generate a deterministic, synthetic sales dataset
#            for the performance experiments in this repo.
#            No real customer, product, or sales data is used.
#
#            Writes one unpartitioned Delta table with many
#            small files, on purpose: that layout is the
#            "before" state the later notebooks improve on.
#
# NOTE: The data is
#            invented, the scale is configurable, and the
#            results you measure depend on your own cluster.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
import pyspark.sql.functions as F

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("base_path", "")
base_path = dbutils.widgets.get("base_path") or "/tmp/spark_perf_benchmark"

dbutils.widgets.text("num_rows", "")
num_rows = int(dbutils.widgets.get("num_rows") or "1000000")

# COMMAND ----------
# ── Synthetic data ────────────────────────────────────────────
# Deterministic seeds are fixed so two runs with the same
# num_rows produce the same dataset. Change num_rows to fit
# your cluster; 1,000,000 rows is the default, not a
# production scale claim.
#
# Columns mirror the CRM schema used in the azure-etl-pipeline
# sample data, so the queries in notebooks 02-04 read naturally.

regions = ["North", "South", "East", "West"]
channels = ["Online", "Retail", "Corporate"]
statuses = ["Completed", "Returned", "Pending"]
products = [
    ("PROD101", "Laptop Pro X"),
    ("PROD102", "Wireless Mouse"),
    ("PROD103", "USB-C Hub"),
    ("PROD104", "Mechanical Keyboard"),
    ("PROD105", "4K Monitor"),
    ("PROD106", "Webcam HD"),
    ("PROD107", "Headphones Pro"),
    ("PROD108", "SSD 1TB"),
    ("PROD109", "Smart Speaker"),
    ("PROD110", "Tablet 10inch"),
]
product_ids = [p[0] for p in products]
product_names = [p[1] for p in products]

# A per-row deterministic index, then derived columns. rand()
# with a fixed seed is used for each column so the dataset
# is reproducible for a given num_rows.
df = (
    spark.range(num_rows)
    .withColumn("customer_id",
        F.concat(F.lit("CUST"),
                 F.lpad((F.rand(42) * 50000).cast("int"), 6, "0")))
    .withColumn("region",
        F.element_at(F.array(*[F.lit(r) for r in regions]),
                     (F.rand(43) * len(regions)).cast("int") + 1))
    .withColumn("product_idx",
        (F.rand(44) * len(products)).cast("int"))
    .withColumn("product_id",
        F.element_at(F.array(*[F.lit(p) for p in product_ids]),
                     F.col("product_idx") + 1))
    .withColumn("product_name",
        F.element_at(F.array(*[F.lit(p) for p in product_names]),
                     F.col("product_idx") + 1))
    .drop("product_idx")
    .withColumn("sale_amount",
        F.round(F.rand(45) * 900 + 10, 2))
    .withColumn("return_amount",
        F.when(F.rand(46) < 0.08,
               F.round(F.col("sale_amount") * F.rand(47), 2))
         .otherwise(F.lit(0.0)))
    .withColumn("sale_date",
        F.date_add(F.lit("2023-01-01").cast("date"),
                   (F.rand(48) * 730).cast("int")))
    .withColumn("sales_rep_id",
        F.concat(F.lit("REP"),
                 F.lpad((F.rand(49) * 50).cast("int") + 1, 3, "0")))
    .withColumn("channel",
        F.element_at(F.array(*[F.lit(c) for c in channels]),
                     (F.rand(50) * len(channels)).cast("int") + 1))
    .withColumn("status",
        F.element_at(F.array(*[F.lit(s) for s in statuses]),
                     (F.rand(51) * len(statuses)).cast("int") + 1))
    .withColumn("sale_year", F.year("sale_date"))
    .withColumn("sale_month", F.month("sale_date"))
)

print(f"Rows generated: {df.count():,}")

# COMMAND ----------
# ── Write: many small files, on purpose ───────────────────────
# 200 partitions by default makes the small-file
# problem visible without needing production-scale data.
# Do not "fix" this write: notebooks 02-04 compare layouts
# built from this same source table.

raw_path = f"{base_path}/raw_sales"

(df.repartition(200)
   .write
   .format("delta")
   .mode("overwrite")
   .save(raw_path))

print(f"Wrote Delta table: {raw_path}")
print("Next: run 02_baseline_unoptimised.py, then "
      "03_optimised_partition_zorder.py, then 04_benchmark.py.")
