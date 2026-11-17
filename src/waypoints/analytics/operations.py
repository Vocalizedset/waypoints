"""Reusable operational analytics for the combined Yellow Taxi dataset.

Uses the derived columns from feature engineering. Averages ignore null, NaN
and non-positive values; the offending rows are counted separately.

Trip length thresholds (miles): short <= 2, long >= 10. Profiling showed a
median near 1.8 miles and a 95th percentile near 12.5 miles, so 2 miles
captures typical short hops and 10 miles approximates the top decile.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from waypoints.processing.cleaning import OUTPUT_DIR

SHORT_TRIP_MAX_MILES = 2.0
LONG_TRIP_MIN_MILES = 10.0


def _valid(name: str):
    column = F.col(name)
    return F.when(
        column.isNull() | F.isnan(column) | (column <= 0),
        F.lit(None).cast("double"),
    ).otherwise(column)


def _share_pct():
    total = F.sum("trip_count").over(Window.partitionBy())
    return F.round(100 * F.col("trip_count") / total, 2).alias("trip_share_pct")


def average_trip_duration(trips: DataFrame) -> DataFrame:
    value = _valid("trip_duration_minutes")
    return trips.agg(
        F.round(F.avg(value), 2).alias("avg_trip_duration_minutes"),
        F.count(value).alias("valid_trip_count"),
        F.sum(F.col("trip_duration_minutes").isNull().cast("int")).alias("null_duration_trips"),
        F.sum((F.col("trip_duration_minutes") <= 0).cast("int")).alias("nonpositive_duration_trips"),
    )


def average_trip_distance(trips: DataFrame) -> DataFrame:
    value = _valid("trip_distance")
    return trips.agg(
        F.round(F.avg(value), 2).alias("avg_trip_distance_miles"),
        F.count(value).alias("valid_trip_count"),
        F.sum(F.col("trip_distance").isNull().cast("int")).alias("null_distance_trips"),
        F.sum((F.col("trip_distance") <= 0).cast("int")).alias("nonpositive_distance_trips"),
    )


def average_speed(trips: DataFrame) -> DataFrame:
    value = _valid("average_speed_mph")
    return trips.agg(
        F.round(F.avg(value), 2).alias("avg_speed_mph"),
        F.count(value).alias("valid_trip_count"),
        F.sum(F.col("average_speed_mph").isNull().cast("int")).alias("invalid_speed_trips"),
    )


def average_fare_per_mile(trips: DataFrame) -> DataFrame:
    value = _valid("fare_per_mile")
    return trips.agg(
        F.round(F.avg(value), 2).alias("avg_fare_per_mile"),
        F.count(value).alias("valid_trip_count"),
        F.sum(F.col("fare_per_mile").isNull().cast("int")).alias("invalid_fare_per_mile_trips"),
    )


def trip_duration_by_hour(trips: DataFrame) -> DataFrame:
    return trips.groupBy("pickup_hour") \
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg(_valid("trip_duration_minutes")), 2).alias("avg_trip_duration_minutes"),
            F.count(_valid("trip_duration_minutes")).alias("valid_duration_trips"),
        ) \
        .orderBy("pickup_hour")


def trip_length_distribution(trips: DataFrame) -> DataFrame:
    distance = F.col("trip_distance")
    category = F.when(
        distance.isNull() | F.isnan(distance) | (distance <= 0), F.lit("Invalid")
    ).when(distance <= SHORT_TRIP_MAX_MILES, F.lit("Short (<= 2 mi)")) \
        .when(distance >= LONG_TRIP_MIN_MILES, F.lit("Long (>= 10 mi)")) \
        .otherwise(F.lit("Medium (2-10 mi)")) \
        .alias("trip_length_category")
    return trips.groupBy(category) \
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg(_valid("trip_distance")), 2).alias("avg_distance_miles"),
            F.round(F.avg(_valid("trip_duration_minutes")), 2).alias("avg_duration_minutes"),
        ) \
        .withColumn("trip_share_pct", _share_pct()) \
        .orderBy(F.desc("trip_count"))


def passenger_count_distribution(trips: DataFrame) -> DataFrame:
    passengers = F.col("passenger_count")
    bucket = F.when(passengers.isNull(), F.lit("Unknown")) \
        .when(passengers == 0, F.lit("0")) \
        .when(passengers >= 7, F.lit("7+")) \
        .otherwise(passengers.cast("string")) \
        .alias("passenger_count_bucket")
    return trips.groupBy(bucket) \
        .agg(F.count("*").alias("trip_count")) \
        .withColumn("trip_share_pct", _share_pct()) \
        .orderBy(F.desc("trip_count"))


def operations_datasets(trips: DataFrame) -> tuple[tuple[str, DataFrame], ...]:
    """Return all operational outputs as (file name, DataFrame) pairs."""
    return (
        ("analytics_average_trip_duration.parquet", average_trip_duration(trips)),
        ("analytics_average_trip_distance.parquet", average_trip_distance(trips)),
        ("analytics_average_speed.parquet", average_speed(trips)),
        ("analytics_average_fare_per_mile.parquet", average_fare_per_mile(trips)),
        ("analytics_trip_duration_by_hour.parquet", trip_duration_by_hour(trips)),
        ("analytics_trip_length_distribution.parquet", trip_length_distribution(trips)),
        ("analytics_passenger_count_distribution.parquet", passenger_count_distribution(trips)),
    )


def write_operations_datasets(trips: DataFrame, output_dir=OUTPUT_DIR) -> None:
    """Write the operational datasets; shared by batch and incremental runs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, data in operations_datasets(trips):
        data.write.mode("overwrite").parquet(str(output_dir / name))
