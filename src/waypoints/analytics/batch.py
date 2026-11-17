"""Batch analytics over all available feature-engineered months.

Reads the cleaned, zone-enriched and feature-engineered monthly datasets,
combines them, and writes three datasets to ``src/data/output/``:

- analytics_revenue: revenue by month, borough and payment type.
- analytics_demand: trips and passengers by pickup time and zone.
- analytics_operations: speed, distance and duration by borough and vendor.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from waypoints.processing.cleaning import PROCESSED_DIR, ROOT

OUTPUT_DIR = ROOT / "src" / "data" / "output"
PREFIX = "yellow_tripdata_"


def _months_with_suffix(suffix: str) -> set[str]:
    """Return months that have a processed dataset with this suffix."""
    return {
        path.stem.removeprefix(PREFIX).removesuffix(f"_{suffix}")
        for path in PROCESSED_DIR.glob(f"{PREFIX}*_{suffix}.parquet")
    }


def discover_months() -> tuple[str, ...]:
    """Return months having both a cleaned and a feature-engineered dataset."""
    return tuple(sorted(_months_with_suffix("cleaned") & _months_with_suffix("features")))


def revenue_analytics(trips: DataFrame) -> DataFrame:
    """Net revenue, including reversals (negative amounts), by payment type."""
    return trips.groupBy(
        F.year("tpep_pickup_datetime").alias("pickup_year"),
        "pickup_month",
        "pickup_borough",
        "payment_type",
    ).agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.sum("tip_amount"), 2).alias("total_tips"),
        F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
    )


def demand_analytics(trips: DataFrame) -> DataFrame:
    """Trip demand by pickup date/hour and pickup zone."""
    return trips.groupBy(
        F.year("tpep_pickup_datetime").alias("pickup_year"),
        "pickup_month",
        "pickup_day",
        "pickup_day_of_week",
        "pickup_hour",
        "is_weekend",
        "pickup_borough",
        "pickup_zone",
    ).agg(
        F.count("*").alias("trips"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
    )


def operational_analytics(trips: DataFrame) -> DataFrame:
    """Operational metrics using the engineered speed and cost features."""
    return trips.groupBy(
        F.year("tpep_pickup_datetime").alias("pickup_year"),
        "pickup_month",
        "pickup_borough",
        "VendorID",
    ).agg(
        F.count("*").alias("trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.avg("average_speed_mph"), 2).alias("avg_speed_mph"),
        F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_percentage"),
    )


def run_batch_analytics(spark: SparkSession) -> None:
    """Discover every available month, combine them and write the analytics."""
    months = discover_months()
    print(f"Analytics: processing {len(months)} months ({', '.join(months)})")
    if not months:
        return

    trips = spark.read.parquet(
        *(str(PROCESSED_DIR / f"{PREFIX}{month}_features.parquet") for month in months)
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    outputs = (
        ("analytics_revenue.parquet", revenue_analytics(trips)),
        ("analytics_demand.parquet", demand_analytics(trips)),
        ("analytics_operations.parquet", operational_analytics(trips)),
    )
    for name, data in outputs:
        data.write.mode("overwrite").parquet(str(OUTPUT_DIR / name))
