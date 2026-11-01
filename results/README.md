# Benchmark results

There are no measured results in this folder yet, and none are claimed anywhere in this repository.

## How to record a run

1. Run `notebooks/04_benchmark.py` on Databricks.
2. Copy the measured CSV from under `base_path` into this folder, with a filename that includes the run date, for example `benchmark_results_2026-11-01.csv`.
3. Fill in the environment columns in the template: Databricks Runtime / Spark version, cluster type and worker count, Photon and Adaptive Query Execution settings, `num_rows`, and `runs`.
4. In the pull request or commit message that adds the file, state the run date and the cluster. Do not merge results from different clusters or dates into a single table.

Do not copy a number from a single timed run, a run on a different cluster, or a run at a different data volume into the README as a result for this project. A number without its environment is not a result.
