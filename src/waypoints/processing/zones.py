"""Enrich cleaned trips with taxi zone names."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from waypoints.processing.cleaning import MONTHS, PROCESSED_DIR, RAW_DIR

ZONE_CSV = RAW_DIR / "taxi_zone_lookup.csv"


def enrich_trips(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pickup = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    dropoff = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )
    joined = (
        trips.join(pickup, on="PULocationID", how="left")
        .join(dropoff, on="DOLocationID", how="left")
    )
    return joined.select(
        *(trips.columns + ["pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone"])
    )


def run_zone_enrichment(
    spark: SparkSession, months: tuple[str, ...] | None = None
) -> None:
    target_months = MONTHS if months is None else months
    zones = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(ZONE_CSV))
    )
    for month in target_months:
        trips = spark.read.parquet(
            str(PROCESSED_DIR / f"yellow_tripdata_{month}_cleaned.parquet")
        )
        enriched = enrich_trips(trips, zones)
        enriched.write.mode("overwrite").parquet(
            str(PROCESSED_DIR / f"yellow_tripdata_{month}_enriched.parquet")
        )
