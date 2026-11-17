# waypoints

NYC Yellow Taxi batch pipeline (PySpark 4.3, JDK 25).

Copy `.env.example` to `.env` and set `JAVA_HOME` to your local JDK, then:

```bash
uv run --env-file .env python main.py
```

`main.py` is the full-batch entry point: it creates one Spark session and runs each stage in order (cleaning, zone enrichment, features, analytics). Monthly datasets are written to `src/data/processed/`. Revenue, demand and operations aggregations live in `revenue.py`, `demand.py` and `operations.py`, are shared with the incremental pipeline, and write seven datasets each under `src/data/output/`: revenue, demand and operational metrics. Short trips are <= 2 miles and long trips are >= 10 miles.

For file-based incremental runs, use `python incremental.py` instead. It tracks source file size and modification time in `src/data/processed/processing_manifest.json`, reprocesses only new or changed months, and rebuilds the overall analytics from all available months.
