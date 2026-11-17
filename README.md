# waypoints

NYC Yellow Taxi batch pipeline (PySpark 4.3, JDK 25).

Copy `.env.example` to `.env` and set `JAVA_HOME` to your local JDK, then:

```bash
uv run --env-file .env python main.py
```

`main.py` is the full-batch entry point: it creates one Spark session and runs each stage in order (cleaning, zone enrichment, features, analytics). Monthly datasets are written to `src/data/processed/`. Revenue, demand and operations aggregations live in `revenue.py`, `demand.py` and `operations.py`, are shared with the incremental pipeline, and write seven datasets each under `src/data/output/`: revenue, demand and operational metrics. Short trips are <= 2 miles and long trips are >= 10 miles.

For file-based incremental runs, use `python incremental.py` instead. It tracks source file size and modification time in `src/data/processed/processing_manifest.json`, reprocesses only new or changed months, and rebuilds the overall analytics from all available months.

Run the batch-versus-incremental experiment with `python experiment.py`. It uses real TLC files only: a byte-for-byte copy of an existing month as the new month and a controlled copy of another real month as the modified month. Raw files and original outputs are restored afterward. Results are written to `src/data/output/batch_vs_incremental_experiment.csv`; correctness compares every `analytics_*.parquet` output after rounding numeric values to two decimals.

Train the trip-duration models with `python train.py`. It samples up to 200,000 eligible rows (seed 42, without replacement), compares four regressors, and writes metrics plus the best PipelineModel under `src/data/output/`.

Launch the custom dashboard with `python dashboard.py`, then open `http://127.0.0.1:8000`. It uses FastAPI and DuckDB to read the generated Parquet/CSV outputs directly, with a dependency-free HTML/CSS/SVG frontend. No chart values are hardcoded or synthesized.
