"""Batch analytics over all available feature-engineered months.

Reads the cleaned, zone-enriched and feature-engineered monthly datasets,
combines them, and writes revenue, demand and operations datasets to
``src/data/output/``. Revenue, demand and operations aggregations live in
their own modules and are shared with the incremental pipeline.
"""

from pyspark.sql import SparkSession

from waypoints.analytics.demand import write_demand_datasets
from waypoints.analytics.operations import write_operations_datasets
from waypoints.analytics.revenue import write_revenue_datasets
from waypoints.processing.cleaning import PROCESSED_DIR

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


def run_batch_analytics(spark: SparkSession) -> None:
    """Discover every available month, combine them and write the analytics."""
    months = discover_months()
    print(f"Analytics: processing {len(months)} months ({', '.join(months)})")
    if not months:
        return

    trips = spark.read.parquet(
        *(str(PROCESSED_DIR / f"{PREFIX}{month}_features.parquet") for month in months)
    )
    write_revenue_datasets(trips)
    write_demand_datasets(trips)
    write_operations_datasets(trips)
