"""Add derived features to the enriched monthly trip datasets."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from waypoints.processing.cleaning import MONTHS, PROCESSED_DIR


def add_features(df: DataFrame) -> DataFrame:
    # trip_duration_minutes is already derived from the timestamps in cleaning.
    return (
        df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("is_weekend", F.col("pickup_day_of_week").isin(1, 7))
        .withColumn(
            "average_speed_mph",
            F.when(
                (F.col("trip_duration_minutes") > 0) & (F.col("trip_distance") > 0),
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "fare_per_mile",
            F.when(
                (F.col("trip_distance") > 0) & (F.col("fare_amount") >= 0),
                F.col("fare_amount") / F.col("trip_distance"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                F.col("tip_amount") / F.col("fare_amount") * 100,
            ).otherwise(F.lit(None).cast("double")),
        )
    )


def run_feature_engineering(
    spark: SparkSession, months: tuple[str, ...] | None = None
) -> None:
    target_months = MONTHS if months is None else months
    for month in target_months:
        trips = spark.read.parquet(
            str(PROCESSED_DIR / f"yellow_tripdata_{month}_enriched.parquet")
        )
        add_features(trips).write.mode("overwrite").parquet(
            str(PROCESSED_DIR / f"yellow_tripdata_{month}_features.parquet")
        )
