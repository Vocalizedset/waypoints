"""Custom dashboard API backed by the generated Waypoint analytics outputs."""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from waypoints.processing.cleaning import OUTPUT_DIR

STATIC_DIR = Path(__file__).parent / "static"
app = FastAPI(title="Waypoints Dashboard")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

PARQUET_FILES = {
    "revenue_by_month": "analytics_revenue_by_month.parquet",
    "revenue_by_borough": "analytics_revenue_by_pickup_borough.parquet",
    "payment_method": "analytics_revenue_by_payment_method.parquet",
    "tips_summary": "analytics_tips_summary.parquet",
    "trips_per_hour": "analytics_trips_per_hour.parquet",
    "trips_per_day": "analytics_trips_per_day.parquet",
    "peak_hours": "analytics_peak_pickup_hours.parquet",
    "busiest_zones": "analytics_busiest_pickup_zones.parquet",
    "popular_routes": "analytics_popular_routes.parquet",
    "weekday_weekend": "analytics_weekday_weekend_demand.parquet",
    "avg_duration": "analytics_average_trip_duration.parquet",
    "avg_distance": "analytics_average_trip_distance.parquet",
    "avg_speed": "analytics_average_speed.parquet",
    "fare_per_mile": "analytics_average_fare_per_mile.parquet",
    "passenger_distribution": "analytics_passenger_count_distribution.parquet",
    "model_metrics": "trip_duration_model_metrics.parquet",
}
CSV_FILES = {"experiment": "batch_vs_incremental_experiment.csv"}
ALL_FILES = {**PARQUET_FILES, **CSV_FILES}


def _path(key: str) -> Path:
    path = OUTPUT_DIR / ALL_FILES[key]
    if not path.exists():
        raise FileNotFoundError(f"Missing generated output: {path}")
    return path


def _literal(path: Path) -> str:
    return str(path).replace("'", "''")


def _json_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _query(connection, sql: str) -> list[dict]:
    cursor = connection.execute(sql)
    columns = [column[0] for column in cursor.description]
    return [
        {key: _json_value(value) for key, value in zip(columns, row)}
        for row in cursor.fetchall()
    ]


def _one(connection, sql: str) -> dict:
    rows = _query(connection, sql)
    return rows[0] if rows else {}


def _sources() -> list[dict]:
    return [
        {
            "key": key,
            "file": name,
            "modified_at": datetime.fromtimestamp(
                (OUTPUT_DIR / name).stat().st_mtime, tz=timezone.utc
            ).isoformat(),
        }
        for key, name in ALL_FILES.items()
        if (OUTPUT_DIR / name).exists()
    ]


def _load_data() -> dict:
    parquet = {key: f"read_parquet('{_literal(_path(key))}')" for key in PARQUET_FILES}
    experiment_csv = f"read_csv_auto('{_literal(_path('experiment'))}', header=true)"

    with duckdb.connect() as connection:
        return {
            "revenue": {
                "trend": _query(connection, f"""
                    SELECT printf('%04d-%02d', pickup_year, pickup_month) AS month,
                           total_revenue, gross_revenue, total_fare_amount, tip_percentage
                    FROM {parquet['revenue_by_month']}
                    ORDER BY pickup_year, pickup_month
                """),
                "by_borough": _query(connection, f"""
                    SELECT pickup_borough, total_revenue, gross_revenue, trip_count
                    FROM {parquet['revenue_by_borough']}
                    ORDER BY total_revenue DESC
                """),
                "by_payment": _query(connection, f"""
                    SELECT payment_method, total_revenue, trip_count, avg_revenue_per_trip
                    FROM {parquet['payment_method']}
                    ORDER BY total_revenue DESC
                """),
                "tips": _query(connection, f"""
                    SELECT printf('%04d-%02d', pickup_year, pickup_month) AS month,
                           total_tips, tip_percentage, trips_with_tips
                    FROM {parquet['tips_summary']}
                    ORDER BY pickup_year, pickup_month
                """),
                "summary": _one(connection, f"""
                    SELECT SUM(total_revenue) AS total_revenue,
                           SUM(total_fare_amount) AS total_fare_amount,
                           SUM(total_tips) AS total_tips,
                           SUM(trip_count) AS trip_count
                    FROM {parquet['revenue_by_month']}
                """),
            },
            "demand": {
                "hourly": _query(connection, f"""
                    SELECT pickup_hour_start, pickup_hour, trip_count
                    FROM {parquet['trips_per_hour']}
                    ORDER BY pickup_hour_start
                """),
                "daily": _query(connection, f"""
                    SELECT pickup_date, day_name, trip_count
                    FROM {parquet['trips_per_day']}
                    ORDER BY pickup_date
                """),
                "peak_hours": _query(connection, f"""
                    SELECT hour_label, trip_count
                    FROM {parquet['peak_hours']}
                    ORDER BY trip_count DESC
                """),
                "busiest_zones": _query(connection, f"""
                    SELECT pickup_borough, pickup_zone, trip_count
                    FROM {parquet['busiest_zones']}
                    ORDER BY trip_count DESC
                    LIMIT 15
                """),
                "popular_routes": _query(connection, f"""
                    SELECT pickup_borough || ': ' || pickup_zone || ' -> ' ||
                           dropoff_borough || ': ' || dropoff_zone AS route,
                           trip_count
                    FROM {parquet['popular_routes']}
                    ORDER BY trip_count DESC
                    LIMIT 15
                """),
                "weekday_weekend": _query(connection, f"""
                    SELECT day_type, trip_count, demand_share_pct
                    FROM {parquet['weekday_weekend']}
                    ORDER BY trip_count DESC
                """),
            },
            "operations": {
                "avg_duration": _one(connection, f"""
                    SELECT avg_trip_duration_minutes, valid_trip_count
                    FROM {parquet['avg_duration']}
                """),
                "avg_distance": _one(connection, f"""
                    SELECT avg_trip_distance_miles, valid_trip_count
                    FROM {parquet['avg_distance']}
                """),
                "avg_speed": _one(connection, f"""
                    SELECT avg_speed_mph, valid_trip_count
                    FROM {parquet['avg_speed']}
                """),
                "fare_per_mile": _one(connection, f"""
                    SELECT avg_fare_per_mile, valid_trip_count
                    FROM {parquet['fare_per_mile']}
                """),
                "passengers": _query(connection, f"""
                    SELECT passenger_count_bucket, trip_count, trip_share_pct
                    FROM {parquet['passenger_distribution']}
                    ORDER BY trip_count DESC
                """),
            },
            "ml": _query(connection, f"""
                SELECT model_name, rmse, mae, r2, is_best
                FROM {parquet['model_metrics']}
                ORDER BY rmse ASC
            """),
            "experiment": _query(connection, f"""
                SELECT scenario, approach, source_months, source_records_processed,
                       execution_seconds, throughput_records_per_second,
                       monthly_output_records, analytics_output_records,
                       outputs_compared,
                       COALESCE(mismatched_outputs, '') AS mismatched_outputs,
                       CAST(correctness_match AS BOOLEAN) AS correctness_match
                FROM {experiment_csv}
                ORDER BY CASE scenario
                             WHEN 'initial_run' THEN 1
                             WHEN 'new_month' THEN 2
                             WHEN 'modified_month' THEN 3
                             ELSE 4 END,
                         CASE approach WHEN 'batch' THEN 1 ELSE 2 END
            """),
            "sources": _sources(),
        }


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/dashboard")
def dashboard_data() -> dict:
    try:
        return _load_data()
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
