# Spark Performance & Cost Optimisation

![Databricks](https://img.shields.io/badge/Databricks-Spark-FF3621)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8)
![Python](https://img.shields.io/badge/Python-3.9-blue)

> This project builds a reproducible benchmark for Spark layout optimisation on a synthetic sales dataset. All data is generated inside the notebooks; no real customer or company data is used. Results are measured by running the benchmark on your own cluster — see [Results](#results).

## The problem

A Spark query that scans a whole table to answer a narrow question is paying for work it does not need: every file is listed, opened, and read, whether or not it contains a matching row. Three layout decisions control most of that waste — how the table is partitioned, how rows are co-located inside partitions, and whether a small dimension table is broadcast instead of shuffled — and they are easy to claim but hard to prove without running the same query, unchanged, against different layouts.

This repo makes the comparison runnable and honest: one synthetic dataset, three representative queries, two layouts, and a benchmark notebook that times each query repeatedly and records the median. Change the layout, re-run the same queries, and the difference — if any, on your cluster — is the result.

## What it builds

From one deterministic synthetic dataset (default 1,000,000 rows, seed-fixed, invented customers/products/regions), the notebooks build:

| Layout | How it is written | What it tests |
|---|---|---|
| `baseline_sales` | Delta, no partitioning, no Z-ordering, 200 small files | The cost of the naive layout |
| `optimised_sales` | Partitioned by `(region, sale_year, sale_month)`, then `OPTIMIZE … ZORDER BY (customer_id)` | Partition pruning + file skipping + file compaction |

The three queries, identical for both layouts:

| Query | Shape | Why it is in the benchmark |
|---|---|---|
| Q1 — Point lookup | One `customer_id`, one quarter | Tests Z-ordering / file skipping. Partitioning alone does not help this query. |
| Q2 — Region-month aggregate | One region, one month, grouped by channel | Tests partition pruning. |
| Q3 — Date-range scan | One quarter, all regions, grouped by region | Tests whether the layout helps a broad scan at all — it may help little, and that is a result too. |

A fourth query, Q4, runs only in the benchmark notebook: a join to a 10-row product dimension, with and without a broadcast hint, selecting only the columns it needs. It tests broadcast joins and column pruning separately from the layout comparison.

Full detail — flow, design decisions with trade-offs, failure modes, and the measurement method — is in [docs/architecture.md](docs/architecture.md).

## How to run

Run the notebooks in order, on Databricks, with the same `base_path` in each. No storage account, secret, or credential is needed: all paths are under the `base_path` you supply (default `/tmp/spark_perf_benchmark`), and the data is generated inside the notebooks.

1. **`01_generate_synthetic_data.py`** — generates the dataset and writes `raw_sales` (unpartitioned, 200 files, on purpose). Set `num_rows` to fit your cluster; the default is 1,000,000.
2. **`02_baseline_unoptimised.py`** — builds `baseline_sales` and runs Q1–Q3 once, to see the naive layout's cost.
3. **`03_optimised_partition_zorder.py`** — builds `optimised_sales` (partitioned + Z-ordered + optimised) and runs Q1–Q3 once. `VACUUM` is off by default; set `confirm_vacuum=true` only for this generated data.
4. **`04_benchmark.py`** — warm-up + median of 3 runs per query per layout, plus Q4 with/without broadcast. Writes the results to a Delta metrics table and a CSV under `base_path`. Set `runs` higher (for example 5) on a shared cluster where timings vary.

Requirements: a Databricks workspace with Spark and Delta Lake. The notebooks use Delta *paths*, not Unity Catalog tables, so they run without any catalog setup; adapting them to managed tables is a noted extension in the architecture document.

## Results

**There are no measured results in this repository yet, and none are claimed.** The `results/` folder contains only a template and a note explaining how to record a run. That is deliberate: a speedup measured on one cluster, at one data volume, on one date, does not transfer to another cluster, and copying such a number into a README would be misleading.

When you run `04_benchmark.py`, record alongside the numbers: Spark/Databricks Runtime version, cluster type and worker count, `num_rows`, `runs`, and the run date. Only then add a results table — labelled with all of that — to this README. The benchmark notebook already records the Spark version and run date in every metrics row to make that easy.

What to expect, qualitatively: Q2 should benefit most from partitioning, Q1 most from Z-ordering, and Q3 least from either — but "should" is a hypothesis, and the benchmark exists to test it on your cluster. If a layout shows no improvement for a query, that is a valid, reportable finding, not a failed experiment.

## Key design choices

- **Same data, same queries, only the layout changes.** The dataset is generated once and the query text is identical for both layouts, so the comparison isolates layout from data and logic.
- **Deterministic synthetic data.** Fixed seeds mean the same `num_rows` produces the same dataset, so runs are comparable. The data is invented; no real names, customers, or sales figures are in this repository.
- **Median of repeated runs, after warm-up.** A single timed run mostly measures warm-up, file listing, and caching. The benchmark discards the warm-up and reports the median.
- **Files touched, not just time.** Time varies with cluster load; the number of files Spark reads for a query is a more stable signal that pruning and skipping are actually happening. Both are recorded.
- **VACUUM behind a confirmation flag.** `VACUUM RETAIN 0 HOURS` deletes files permanently. It is off by default and only ever runs against the generated paths in this repo.

## Limitations — read openly

- **Databricks only, as written.** The notebooks use `dbutils` and Delta SQL (`OPTIMIZE`, `ZORDER`, `VACUUM`). They do not run on a plain local Spark install without adaptation.
- **Scale.** The default is 1,000,000 rows. That is enough to show file-pruning behaviour, not enough to prove anything about 50M-row production behaviour. Scale `num_rows` up deliberately, with a cluster sized for it.
- **No cost measurement.** The benchmark measures time and files read. It does not measure Databricks Units (DBUs) or money. Do not convert a time saving into a cost saving without the cluster's billing data.
- **Single-query benchmark.** The queries run one at a time, sequentially. Concurrent-query behaviour, and the effect of Photon / Adaptive Query Execution settings on your workspace, are out of scope and are noted as extensions in the architecture document.
- **Z-ordering is not free.** `OPTIMIZE` rewrites data and costs a run of its own. The benchmark times queries, not the cost of building the optimised layout. Include the `OPTIMIZE` time in any total-cost comparison.

## Project structure

```
spark-performance-optimization/
├── notebooks/
│   ├── 01_generate_synthetic_data.py      # Deterministic synthetic data, 1M rows by default
│   ├── 02_baseline_unoptimised.py         # Baseline layout + Q1-Q3, single run
│   ├── 03_optimised_partition_zorder.py   # Partitioned + Z-ordered layout + Q1-Q3, single run
│   └── 04_benchmark.py                    # Repeated median benchmark + Q4 broadcast join
├── tools/
│   └── generate_sample_sales.py           # Pure-Python generator for the 100-row sample (no Spark)
├── data/
│   └── sample_sales.csv                   # 100 synthetic rows, for schema/shape checking only
├── docs/
│   └── architecture.md                    # Flow, design decisions, failure modes, measurement method
├── results/
│   ├── README.md                          # How to record a measured run (no results claimed yet)
│   └── benchmark_results_TEMPLATE.csv     # Column headers for a measured run
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

## Tech stack

| Component | Technology |
|---|---|
| Processing | Apache Spark, PySpark |
| Table format | Delta Lake (paths, `OPTIMIZE`, `ZORDER BY`, `VACUUM`) |
| Platform | Databricks (notebooks, `dbutils`) |
| Data | Synthetic, deterministic, seed-fixed |
| Measurement | Repeated timing, median, files-touched count |

## Author

**Sanjeev Kumar Pandey** — Data Engineer → AI Engineer | Azure Databricks | PySpark | RAG | Agentic AI

[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) · [GitHub](https://github.com/sanjeevpandeydata)
