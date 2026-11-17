# waypoints

NYC Yellow Taxi batch pipeline (PySpark 4.3, JDK 25).

Copy `.env.example` to `.env` and set `JAVA_HOME` to your local JDK, then:

```bash
uv run --env-file .env python main.py
```

`main.py` is the entry point: it creates the Spark session and runs the processing stages in order. Cleaning writes one Parquet dataset per month to `src/data/processed/`.
