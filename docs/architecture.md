# Architecture — Spark Performance & Cost Optimisation

## Purpose

This document describes how this benchmark works, why each experiment is shaped the way it is, and how to read its results without overstating them. It is written so a reviewer can check each claim against the notebooks in `notebooks/` and reproduce the comparison on their own cluster.

This project answers one question: *for the same data and the same queries, how much does table layout change what Spark actually reads and how long a query takes?* It does not answer how fast a production pipeline runs, and it does not claim a cost saving.

> **Status of this document.** This project is built on synthetic data. There is no measured result behind it yet. The notebooks are new code written for this repository. Where a technique has a cost or a limit, this document states it (see §7).

---

## 1. System overview

```
┌─────────────────────────────────────────────────────────────────┐
│  01_generate_synthetic_data.py                                  │
│  spark.range + deterministic rand() seeds                       │
│  → 1,000,000 synthetic sales rows (default, configurable)       │
│  → Delta table: raw_sales (unpartitioned, 200 files)            │
└────────────────────────┬────────────────────────────────────────┘
                         │  same source data, read twice
            ┌────────────┴────────────┐
            ▼                         ▼
┌───────────────────────┐   ┌───────────────────────────────────┐
│ 02_baseline           │   │ 03_optimised                      │
│ baseline_sales        │   │ optimised_sales                   │
│  • no partitioning    │   │  • partitionBy(region,            │
│  • no Z-ordering      │   │    sale_year, sale_month)         │
│  • no OPTIMIZE        │   │  • OPTIMIZE ZORDER BY             │
│                       │   │    (customer_id)                  │
│                       │   │  • VACUUM: off by default          │
└───────────┬───────────┘   └───────────────┬───────────────────┘
            │  same three queries, unchanged │
            └───────────────┬────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  04_benchmark.py                                                │
│  Warm-up + median of N runs per query per layout                │
│  Records: median seconds, rows, files touched, files in table,  │
│  table size, Spark version, run date                            │
│  → Delta metrics table + CSV under base_path                    │
└─────────────────────────────────────────────────────────────────┘
```

**Reading the diagram.** There is one dataset and two layouts. Everything else — query text, data, cluster — is held constant within a run, so any difference in the benchmark is attributable to layout (plus normal run-to-run variance, which the repeated runs are there to show).

---

## 2. End-to-end flow

### 2.1 Generate — Notebook 01

1. **Parameters.** `base_path` (default `/tmp/spark_perf_benchmark`) and `num_rows` (default 1,000,000). Both are widgets, so the same notebooks run at different scales without editing code.
2. **Derive, don't loop.** Rows are generated with `spark.range()` and column expressions (`rand()` with a fixed seed per column, `element_at` over small literal arrays for region/channel/status/product). No Python loop, no UDF, no `collect()` — generation itself is distributed, and the dataset is reproducible for a given `num_rows`.
3. **Write raw.** The result is written as a Delta table at `{base_path}/raw_sales`, repartitioned to 200 partitions. The many-small-files layout is deliberate: it is the "before" state. The raw table is the single source for both experiment layouts, so they cannot drift apart in content.

**Schema.** `customer_id`, `region`, `product_id`, `product_name`, `sale_amount`, `return_amount`, `sale_date`, `sales_rep_id`, `channel`, `status`, plus derived `sale_year` and `sale_month`. The schema mirrors the CRM schema in the `azure-etl-pipeline` sample data, so the queries read as realistic sales queries rather than abstract ones.

### 2.2 Baseline — Notebook 02

1. **Build.** Reads `raw_sales` and writes `baseline_sales` with 200 partitions, no partitioning columns, no Z-ordering, no `OPTIMIZE`.
2. **Measure once.** Counts the files and total size under the table path, then runs Q1–Q3 once each, timing a single `.count()` action per query. This run is for orientation only — it is not the benchmark.
3. **Report.** Prints file count, size, and one elapsed time per query.

### 2.3 Optimised — Notebook 03

1. **Build.** Reads the same `raw_sales` and writes `optimised_sales` partitioned by `(region, sale_year, sale_month)`.
2. **Optimise.** Runs `OPTIMIZE … ZORDER BY (customer_id)`. This rewrites the data into fewer, larger files and co-locates rows for the same customer inside each partition.
3. **Vacuum (optional, off by default).** Only if `confirm_vacuum=true`, runs `VACUUM RETAIN 0 HOURS`. This deletes files no longer referenced by the Delta log. It is safe here only because every path is generated data; the flag exists so the step cannot run by accident.
4. **Measure once.** Same three queries, same timing method as Notebook 02, for a first-look comparison.

### 2.4 Benchmark — Notebook 04

1. **Layouts.** Iterates over `baseline_sales` and `optimised_sales`.
2. **Queries.** Builds Q1–Q3 identically for each layout, plus Q4 (join) in two variants: without a hint, and with `F.broadcast()` on the 10-row dimension, selecting only the columns the aggregation needs.
3. **Timing.** For each query: one warm-up run (excluded), then `runs` timed runs (default 3). The reported figure is the median. Row counts come from the last run; all runs must return the same row count for a given query across layouts, or the comparison is invalid — check that before quoting any result.
4. **Files touched.** For each query, `df.inputFiles()` is recorded where available (`-1` if unavailable, rather than failing the run). This is the pruning/skipping evidence: a query that reads fewer files is doing less I/O, even on a run where wall-clock time is noisy.
5. **Record.** Results are written to a Delta table at `{base_path}/benchmark_metrics` and as a CSV folder at `{base_path}/benchmark_results_csv`, with `run_date` and `spark.version` in every row. They are written where the benchmark ran, not committed to this repository with invented values — see `results/README.md`.

---

## 3. The experiments, and what each one isolates

| Experiment | What changes | What stays the same | What a difference proves |
|---|---|---|---|
| Q1 on baseline vs optimised | Layout: partitioning + Z-ordering | Data, query text, cluster | Whether co-locating customer rows lets Spark skip files for a point lookup. Partitioning alone should not help Q1; Z-ordering is the variable that matters. |
| Q2 on baseline vs optimised | Layout: partitioning | Data, query text, cluster | Whether partition pruning removes whole region/month directories from the read. |
| Q3 on baseline vs optimised | Layout: partitioning + Z-ordering | Data, query text, cluster | Whether the layout helps a broad scan. A small or zero difference here is a real finding: broad scans are dominated by total data read, not by skipping. |
| Q4 no-hint vs broadcast | Join strategy + column selection | Data, query text shape, cluster | Whether broadcasting a 10-row dimension avoids shuffling the fact table, and whether selecting only needed columns matters. This experiment is independent of layout and runs on the optimised layout in the benchmark notebook; re-run it against the baseline layout if you want the interaction effect. |

**What is deliberately not varied.** Cluster size, Spark configuration, and data volume are fixed within a run. Varying them at the same time as layout would make any difference uninterpretable. If you want to test cluster-size sensitivity, run the whole benchmark at two cluster sizes and report both, labelled.

---

## 4. Design decisions

### 4.1 Same data, same queries, only the layout changes

**Decision.** Generate once, read twice, and keep query text byte-identical between layouts (the query-building code is duplicated in Notebooks 02–04 rather than shared, so each notebook is self-contained on Databricks).

**Why.** Performance comparisons are only meaningful when everything except the variable under test is held constant. Duplication is the cost of notebook portability; a shared helper module would be cleaner in a package, and harder to run as standalone Databricks notebooks.

**Trade-off.** If a query is edited in one notebook and not the others, the comparison silently breaks. The row-count check in §2.4 is the guard: identical queries must return identical row counts on both layouts.

### 4.2 Deterministic synthetic data with fixed seeds

**Decision.** `rand()` with a fixed seed per column, and a fixed product/region/channel vocabulary.

**Why.** Reproducibility. Two runs with the same `num_rows` generate the same dataset, so a benchmark re-run is comparable with the first, and a reviewer can regenerate the data and check the numbers.

**Trade-off.** Synthetic data has a uniform-ish distribution by construction. Real sales data is skewed — a few customers, products, or regions dominate — and skew changes partition sizes, Z-ordering benefit, and shuffle behaviour. Results on this data do not predict results on skewed production data. The architecture document states this plainly rather than letting the scale imply otherwise.

### 4.3 Partitioning on the query's filter shape

**Decision.** Partition by `(region, sale_year, sale_month)`, matching Q2's filter and Q3's date range (via the derived year/month columns).

**Why.** Partition pruning only works when the filter matches the partition columns. Partitioning on a column nobody filters on adds directory overhead without skipping anything.

**Trade-off.** Partitioning creates many directories (4 regions × 24 months = 96 partitions at this scale, before Z-ordering compacts files). At small data volumes, partition overhead can outweigh pruning benefit — which is why Q3 and small-scale runs may show little gain. High-cardinality partitioning (for example, by `customer_id` directly) would be worse: thousands of tiny partitions, the small-file problem made worse. The chosen granularity is deliberately coarse.

### 4.4 Z-ordering on `customer_id` only

**Decision.** One Z-order column, the point-lookup key, rather than Z-ordering on several columns.

**Why.** Z-ordering co-locates rows that are queried together. Q1 filters on one customer; co-locating that customer's rows is the direct win. Z-ordering on multiple columns dilutes the co-location for each one.

**Trade-off.** Z-ordering helps queries that filter on the Z-order column and hurts nothing else much — except the cost of the `OPTIMIZE` run itself, which rewrites the table. That cost is not in the query timings; include it in any total-cost comparison (see §7).

### 4.5 Median of repeated runs, after warm-up

**Decision.** One warm-up run, then N timed runs, median reported. Default N = 3.

**Why.** A single timed Spark run measures warm-up, file listing, JIT, caching, and cluster noise as much as it measures the query. The median of repeated runs is the smallest honest summary, and the warm-up exclusion removes the largest single source of bias.

**Trade-off.** Three runs is a small sample. On a shared or autoscaling cluster, three medians can still be noisy. Raise `runs` to 5 or more when timings vary, and report the spread (min/max) alongside the median if you publish the numbers — the benchmark notebook records per-run timings only in its printed output, so note the spread by hand when you record a run.

### 4.6 Files touched as a second metric, alongside time

**Decision.** Record `inputFiles()` length per query, in addition to elapsed time.

**Why.** Wall-clock time varies with cluster load, caching, and neighbours. Files read is a structural signal: if partitioning and Z-ordering work, fewer files are read for the same query, regardless of how busy the cluster is. A time improvement *without* a files-touched improvement should be treated with suspicion; a files-touched improvement without a time improvement is still evidence the layout is doing its job.

**Trade-off.** `inputFiles()` reflects the files Spark lists for the query, which is a good proxy for I/O but not a byte count. File sizes also change after `OPTIMIZE` compaction, so files-touched and bytes-read can move in different directions. The benchmark also records total table size for context, but not bytes read per query — that would require Spark's query metrics, which are a noted extension.

### 4.7 Delta paths, not Unity Catalog tables

**Decision.** All tables are Delta tables at file paths under `base_path`. No catalog, schema, or table creation is required.

**Why.** This project runs on any Databricks workspace without Unity Catalog setup, permissions, or a metastore to clean up. The layout techniques (partitioning, Z-ordering, `OPTIMIZE`) are identical for path-based and catalog tables.

**Trade-off.** Path-based tables skip the governance, lineage, and access-control story that Unity Catalog adds in production. Adapting the notebooks to managed tables (replace `load(path)` / `save(path)` with table names, and `OPTIMIZE delta.`path`` with `OPTIMIZE table_name`) is straightforward and is listed as an extension in §8.

### 4.8 VACUUM behind a confirmation flag, off by default

**Decision.** `confirm_vacuum` defaults to `"false"`; `VACUUM RETAIN 0 HOURS` runs only when explicitly set to `"true"`.

**Why.** `VACUUM` permanently deletes files. A default-on vacuum in a copy-pasted notebook is how notebook code copied elsewhere becomes a production incident. The flag forces the operator to read the warning in Notebook 03 before enabling it.

**Trade-off.** Without `VACUUM`, old files from before `OPTIMIZE` remain on disk and the table's total size (as counted by file listing) overstates the live data size. The benchmark counts files under the table path, so run `VACUUM` (or count only current files via the Delta log) before comparing table sizes across layouts — and say which you did when you record the run.

---

## 5. Data and schema

| Column | Meaning | How it is generated |
|---|---|---|
| `customer_id` | Invented customer key, `CUST` + 6 digits | `rand(42)` over 50,000 values |
| `region` | North / South / East / West | Uniform pick from 4 values |
| `product_id`, `product_name` | 10 invented products | Uniform pick from 10 values |
| `sale_amount` | Sale value, 10–910 | Uniform, rounded to 2 decimals |
| `return_amount` | Return value, 0 or a fraction of the sale | 8% of rows, fraction of `sale_amount` |
| `sale_date` | 2023-01-01 to 2024-12-31 | Uniform day offset over 730 days |
| `sales_rep_id` | `REP001`–`REP050` | Uniform pick |
| `channel` | Online / Retail / Corporate | Uniform pick |
| `status` | Completed / Returned / Pending | Uniform pick |
| `sale_year`, `sale_month` | Derived from `sale_date` | Partition columns |

**Sample data.** `data/sample_sales.csv` (100 rows, seed 42, generated by `tools/generate_sample_sales.py`) uses the same 10 source columns. It exists only for schema and shape checking. It is far too small to say anything about performance, and its dates are confined to 2024 by the generator script, unlike the notebook data which spans 2023–2024.

**What the data does not model.** Skew, seasonality, late-arriving data, schema evolution, nulls in key columns, or duplicate events. Each of those would change the optimisation story, and none is in scope for this project. If you extend this project, add one at a time and re-benchmark — do not add several and attribute the change to all of them.

---

## 6. Configuration

| Parameter | Where | Default | Notes |
|---|---|---|---|
| `base_path` | All notebooks (widget) | `/tmp/spark_perf_benchmark` | All tables and results live under this path. Point it at a Volume or ADLS path in your own workspace if `/tmp` is not suitable. |
| `num_rows` | Notebook 01 (widget) | `1,000,000` | Scale. Raise deliberately, with a cluster sized for it. |
| `runs` | Notebook 04 (widget) | `3` | Timed runs per query per layout, after one warm-up. Raise to 5+ on a noisy cluster. |
| `confirm_vacuum` | Notebook 03 (widget) | `"false"` | Set to `"true"` only for this generated data. See §4.8. |
| Cluster / Spark config | Your workspace | Not set by this repo | The repo sets no Spark configuration. Photon, Adaptive Query Execution, and autoscaling settings are whatever your workspace uses — record them when you record a run, because they affect the numbers. |
| Secrets / credentials | — | None | No secret, token, storage account, or credential appears in this repository. None is needed. |

---

## 7. Failure modes, gaps, and limitations

Read this section before running or quoting anything from this repo. Each item follows from the code or the measurement design.

1. **No measured result exists yet.** Until someone runs Notebook 04 on a real cluster and records the run, every performance statement about this repo is a hypothesis. The README and this document claim no speedup, no file-reduction figure, and no cost saving.
2. **Time is not cost.** The benchmark measures elapsed seconds and files touched. It does not measure DBUs, instance cost, or storage cost. A 30% time saving is not a 30% cost saving unless the cluster bills by the second at a constant rate and the optimised layout costs nothing to build — neither is generally true. Include `OPTIMIZE` time and cluster billing data before making a cost claim.
3. **Uniform synthetic data flatters some techniques and hides others.** Uniform region/customer distribution means partitions are even and Z-ordering benefit is clean. Skewed real data can make one partition dominate, make Z-ordering less effective, or make a broadcast join spill. Do not extrapolate benchmark numbers to production data.
4. **Single-query, sequential benchmark.** Queries run one at a time. The benchmark says nothing about concurrency, queueing, or how the layouts behave under simultaneous load.
5. **First-run and cache effects are only partly controlled.** The warm-up run removes the largest bias, but Delta/Photon caching across runs can still make later runs faster for reasons unrelated to layout. Run order is baseline first, optimised second, in the benchmark loop — if caching favours the second layout, the comparison is biased in its favour. To check, reverse the order on a second run and compare.
6. **`inputFiles()` is a proxy, not a byte count.** It counts files listed for the query. After `OPTIMIZE`, files are larger, so fewer files can still mean similar bytes. Pair it with elapsed time and total table size, and do not quote files-touched alone as an I/O saving.
7. **VACUUM and time travel.** `VACUUM RETAIN 0 HOURS` removes the ability to time-travel to older versions of the optimised table. For this generated data that is fine; in production, a retention window that respects concurrent readers and audit requirements is mandatory.
8. **Databricks-only as written.** `dbutils`, Delta SQL, and `OPTIMIZE`/`ZORDER`/`VACUUM` require Databricks (or a Delta Lake setup that supports them). The notebooks will not run unchanged on a plain local Spark install, which also lacks the Delta integration used here.
9. **Query duplication across notebooks.** The three queries are written out in Notebooks 02, 03, and 04. Editing one without the others silently breaks the like-for-like comparison. The row-count equality check (§2.4) is the only guard — verify it on every run.
10. **No Photon / AQE control.** The repo does not set or record Photon or Adaptive Query Execution settings. Those settings change query plans and timings, and differ between workspaces. Record them with any published result, and do not compare numbers across workspaces with different settings.

---

## 8. Performance — what is known and what is not

**Known from the design.** Partitioning prunes whole directories for filters that match the partition columns. Z-ordering co-locates rows for the Z-order column so data-skipping can drop whole files within a partition. Broadcasting a small dimension avoids shuffling the large table. File compaction via `OPTIMIZE` reduces the number of files Spark must list and open. These are established Spark/Delta behaviours, and this benchmark is designed to show them.

**Not known until a run happens.** The magnitude of any effect, for any query, on any cluster, at any data volume. This document contains no benchmark table, because no benchmark has been run. The `results/` folder is intentionally empty of numbers.

**How to measure and report it.** Run Notebook 04, copy the measured CSV into `results/` with a filename that includes the date, and fill in the template columns: Spark/Databricks Runtime version, cluster type and workers, Photon/AQE settings, `num_rows`, `runs`, and the median seconds and files touched per layout per query. Report the spread across runs if timings varied. Do not average medians across different clusters or dates into a single figure.

---

## 9. How to run, step by step

- [ ] Databricks workspace with Spark and Delta Lake. No catalog, secret, or storage setup is needed.
- [ ] Choose `base_path` and `num_rows` for your cluster. Start with the defaults.
- [ ] Run Notebook 01, then 02, then 03, then 04, in order, with the same `base_path`.
- [ ] Check that Q1–Q3 return the same row counts on both layouts. If they do not, stop — the comparison is invalid.
- [ ] Check the files-touched figures as well as the times. A time win without a files-touched win needs an explanation before it is quoted.
- [ ] Record the environment (Runtime version, cluster, Photon/AQE, `num_rows`, `runs`, date) alongside the numbers, in `results/`.
- [ ] Delete the generated paths under `base_path` when finished, or leave them for a re-run — they are synthetic data and cost storage while they exist.

---

## 10. Extensions (not in this version)

- Adapt the notebooks to Unity Catalog managed tables and re-benchmark, to show the same techniques under governance.
- Add a skewed-data variant (one dominant region/customer) and compare with the uniform data.
- Record bytes read per query from Spark's query metrics, alongside files touched.
- Add a Photon on/off and AQE on/off comparison, run as separate labelled benchmarks, not mixed into one.
- Add a cost column: DBU consumption per query from cluster metrics, reported alongside time.
- Add a second Z-order column experiment (for example, `customer_id, sale_date`) and show why multi-column Z-ordering dilutes the benefit for a single-key lookup.

---

## 11. File inventory

| Path | What it is |
|---|---|
| `notebooks/01_generate_synthetic_data.py` | Synthetic data generation (Databricks notebook) |
| `notebooks/02_baseline_unoptimised.py` | Baseline layout + single-run queries |
| `notebooks/03_optimised_partition_zorder.py` | Optimised layout + single-run queries, VACUUM behind a flag |
| `notebooks/04_benchmark.py` | Repeated median benchmark + broadcast-join experiment |
| `tools/generate_sample_sales.py` | Pure-Python generator for the 100-row sample (no Spark) |
| `data/sample_sales.csv` | 100 synthetic rows, schema/shape checking only |
| `docs/architecture.md` | This document |
| `results/README.md`, `results/benchmark_results_TEMPLATE.csv` | How to record a measured run; no results claimed yet |
| `requirements.txt`, `.gitignore`, `LICENSE`, `README.md` | Project extras and documentation |

---

## Author

**Sanjeev Kumar Pandey** — Data Engineer → AI Engineer | Azure Databricks | PySpark | RAG | Agentic AI

[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) · [GitHub](https://github.com/sanjeevpandeydata)
