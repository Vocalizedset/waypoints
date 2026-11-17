"""Reusable revenue analytics for the combined Yellow Taxi dataset.

Revenue is the net ``total_amount`` (reversals remain negative), while
``gross_revenue`` and ``reversal_amount`` show the two components separately.
Null/NaN amounts are treated as zero and counted in ``invalid_*_trips``.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from waypoints.processing.cleaning import OUTPUT_DIR


def _safe(name: str):
    column = F.col(name)
    return F.when(column.isNull() | F.isnan(column), F.lit(0.0)).otherwise(column)


def _invalid(name: str):
    column = F.col(name)
    return column.isNull() | F.isnan(column)


def _revenue_metrics() -> list:
    total = _safe("total_amount")
    fare = _safe("fare_amount")
    return [
        F.count("*").alias("trip_count"),
        F.round(F.sum(total), 2).alias("total_revenue"),
        F.round(F.sum(F.when(total > 0, total).otherwise(0.0)), 2).alias("gross_revenue"),
        F.round(F.sum(F.when(total < 0, total).otherwise(0.0)), 2).alias("reversal_amount"),
        F.round(F.sum(fare), 2).alias("total_fare_amount"),
        F.round(F.avg(total), 2).alias("avg_revenue_per_trip"),
        F.sum(_invalid("total_amount").cast("int")).alias("invalid_total_amount_trips"),
        F.sum((total < 0).cast("int")).alias("reversal_trips"),
    ]


def _tip_metrics() -> list:
    tip = _safe("tip_amount")
    fare = _safe("fare_amount")
    positive_fare = F.sum(F.when(fare > 0, fare).otherwise(0.0))
    return [
        F.round(F.sum(tip), 2).alias("total_tips"),
        F.when(
            positive_fare > 0,
            F.round(100 * F.sum(tip) / positive_fare, 2),
        ).otherwise(F.lit(None).cast("double")).alias("tip_percentage"),
        F.sum((tip > 0).cast("int")).alias("trips_with_tips"),
    ]


def _payment_method():
    return F.when(F.col("payment_type") == 1, F.lit("Credit card")) \
        .when(F.col("payment_type") == 2, F.lit("Cash")) \
        .when(F.col("payment_type") == 3, F.lit("No charge")) \
        .when(F.col("payment_type") == 4, F.lit("Dispute")) \
        .when(F.col("payment_type") == 5, F.lit("Unknown")) \
        .when(F.col("payment_type") == 6, F.lit("Voided trip")) \
        .otherwise(F.lit("Unknown / not recorded")) \
        .alias("payment_method")


def revenue_by_month(trips: DataFrame) -> DataFrame:
    return trips.groupBy(
        F.year("tpep_pickup_datetime").alias("pickup_year"),
        F.month("tpep_pickup_datetime").alias("pickup_month"),
    ).agg(*(_revenue_metrics() + _tip_metrics()))


def revenue_by_day(trips: DataFrame) -> DataFrame:
    return trips.groupBy(
        F.year("tpep_pickup_datetime").alias("pickup_year"),
        F.month("tpep_pickup_datetime").alias("pickup_month"),
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
    ).agg(*_revenue_metrics())


def average_fare_per_trip(trips: DataFrame) -> DataFrame:
    fare = _safe("fare_amount")
    return trips.agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg(fare), 2).alias("avg_fare_per_trip"),
        F.round(
            F.avg(F.when(fare > 0, fare).otherwise(F.lit(None).cast("double"))),
            2,
        ).alias("avg_fare_per_paid_trip"),
        F.round(F.avg(_safe("total_amount")), 2).alias("avg_revenue_per_trip"),
        F.sum((fare == 0).cast("int")).alias("zero_fare_trips"),
        F.sum((fare < 0).cast("int")).alias("negative_fare_trips"),
        F.sum(_invalid("fare_amount").cast("int")).alias("invalid_fare_trips"),
    )


def revenue_by_pickup_borough(trips: DataFrame) -> DataFrame:
    return trips.groupBy("pickup_borough") \
        .agg(*_revenue_metrics()) \
        .orderBy(F.desc("total_revenue"))


def tips_summary(trips: DataFrame) -> DataFrame:
    return trips.groupBy(
        F.year("tpep_pickup_datetime").alias("pickup_year"),
        F.month("tpep_pickup_datetime").alias("pickup_month"),
    ).agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum(_safe("fare_amount")), 2).alias("total_fare_amount"),
        *_tip_metrics(),
    )


def revenue_by_payment_method(trips: DataFrame) -> DataFrame:
    return trips.groupBy("payment_type", _payment_method()) \
        .agg(*_revenue_metrics()) \
        .orderBy(F.desc("total_revenue"))


def hourly_revenue_trends(trips: DataFrame) -> DataFrame:
    return trips.groupBy(
        F.date_trunc("hour", "tpep_pickup_datetime").alias("pickup_hour_start"),
        F.hour("tpep_pickup_datetime").alias("pickup_hour"),
        F.month("tpep_pickup_datetime").alias("pickup_month"),
    ).agg(*_revenue_metrics())


def revenue_datasets(trips: DataFrame) -> tuple[tuple[str, DataFrame], ...]:
    """Return all revenue outputs as (file name, DataFrame) pairs."""
    return (
        ("analytics_revenue_by_month.parquet", revenue_by_month(trips)),
        ("analytics_revenue_by_day.parquet", revenue_by_day(trips)),
        ("analytics_average_fare.parquet", average_fare_per_trip(trips)),
        ("analytics_revenue_by_pickup_borough.parquet", revenue_by_pickup_borough(trips)),
        ("analytics_tips_summary.parquet", tips_summary(trips)),
        ("analytics_revenue_by_payment_method.parquet", revenue_by_payment_method(trips)),
        ("analytics_hourly_revenue.parquet", hourly_revenue_trends(trips)),
    )


def write_revenue_datasets(trips: DataFrame, output_dir=OUTPUT_DIR) -> None:
    """Write the revenue datasets; shared by batch and incremental runs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, data in revenue_datasets(trips):
        data.write.mode("overwrite").parquet(str(output_dir / name))
