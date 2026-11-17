"""Reusable demand analytics for the combined Yellow Taxi dataset.

Time-based demand uses pickup timestamps. Zone and borough labels come from
the existing zone-lookup join, including N/A and Unknown categories.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from waypoints.processing.cleaning import OUTPUT_DIR


def _share_of_trips(column_name: str = "trip_count"):
    total = F.sum(column_name).over(Window.partitionBy())
    return F.round(100 * F.col(column_name) / total, 2).alias("demand_share_pct")


def trips_per_hour(trips: DataFrame) -> DataFrame:
    return trips.groupBy(
        F.date_trunc("hour", "tpep_pickup_datetime").alias("pickup_hour_start"),
        F.hour("tpep_pickup_datetime").alias("pickup_hour"),
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
    ).agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
    ).orderBy("pickup_hour_start")


def trips_per_day(trips: DataFrame) -> DataFrame:
    return trips.groupBy(
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        F.date_format("tpep_pickup_datetime", "EEEE").alias("day_name"),
        F.month("tpep_pickup_datetime").alias("pickup_month"),
        "is_weekend",
    ).agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
    ).orderBy("pickup_date")


def peak_pickup_hours(trips: DataFrame) -> DataFrame:
    return trips.groupBy(F.hour("tpep_pickup_datetime").alias("pickup_hour")) \
        .agg(F.count("*").alias("trip_count")) \
        .withColumn(
            "hour_label",
            F.concat(F.lpad(F.col("pickup_hour").cast("string"), 2, "0"), F.lit(":00")),
        ) \
        .orderBy(F.desc("trip_count"))


def busiest_pickup_zones(trips: DataFrame) -> DataFrame:
    return trips.groupBy("pickup_borough", "pickup_zone") \
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
        ) \
        .orderBy(F.desc("trip_count"))


def popular_routes(trips: DataFrame) -> DataFrame:
    return trips.groupBy(
        "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone",
    ).agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
    ).orderBy(F.desc("trip_count"))


def weekday_weekend_demand(trips: DataFrame) -> DataFrame:
    return trips.groupBy(
        "is_weekend",
        F.when(F.col("is_weekend"), F.lit("Weekend")).otherwise(F.lit("Weekday")).alias("day_type"),
    ).agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
    ).withColumn("demand_share_pct", _share_of_trips()) \
        .orderBy(F.desc("trip_count"))


def demand_by_borough(trips: DataFrame) -> DataFrame:
    return trips.groupBy("pickup_borough") \
        .agg(
            F.count("*").alias("trip_count"),
            F.countDistinct("pickup_zone").alias("pickup_zone_count"),
            F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
        ) \
        .withColumn("demand_share_pct", _share_of_trips()) \
        .orderBy(F.desc("trip_count"))


def demand_datasets(trips: DataFrame) -> tuple[tuple[str, DataFrame], ...]:
    """Return all demand outputs as (file name, DataFrame) pairs."""
    return (
        ("analytics_trips_per_hour.parquet", trips_per_hour(trips)),
        ("analytics_trips_per_day.parquet", trips_per_day(trips)),
        ("analytics_peak_pickup_hours.parquet", peak_pickup_hours(trips)),
        ("analytics_busiest_pickup_zones.parquet", busiest_pickup_zones(trips)),
        ("analytics_popular_routes.parquet", popular_routes(trips)),
        ("analytics_weekday_weekend_demand.parquet", weekday_weekend_demand(trips)),
        ("analytics_demand_by_borough.parquet", demand_by_borough(trips)),
    )


def write_demand_datasets(trips: DataFrame, output_dir=OUTPUT_DIR) -> None:
    """Write the demand datasets; shared by batch and incremental runs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, data in demand_datasets(trips):
        data.write.mode("overwrite").parquet(str(output_dir / name))
