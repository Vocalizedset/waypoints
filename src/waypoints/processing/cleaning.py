"""Clean the monthly NYC Yellow Taxi trip datasets."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = ROOT / "src" / "data" / "raw"
PROCESSED_DIR = ROOT / "src" / "data" / "processed"
MONTHS = tuple(
    sorted(
        path.stem.removeprefix("yellow_tripdata_")
        for path in RAW_DIR.glob("yellow_tripdata_*.parquet")
    )
)

# Profiling found 111 trips above 500 miles (up to 269,097 miles on an 8-minute
# $12 ride), while 100-500 mile trips have fares consistent with long journeys.
MAX_DISTANCE_MILES = 500.0
# 35 trips lasted over 24 hours; shorter long trips can be genuine.
MAX_DURATION_MINUTES = 24 * 60.0


def clean_trips(df: DataFrame) -> DataFrame:
    # Keep the five null fields tied to payment_type = 0, negative amounts
    # (39,425 of 39,463 negative fares are exact reversals of positive fares),
    # uncommon VendorIDs/RatecodeID 99, equal pickup/dropoff zones, and
    # N/A zones. total_amount is left untouched: component sums disagree on
    # 38.5% of January rows because of vendor-specific fee treatment.
    return (
        df.withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60.0,
        )
        .filter(
            (F.col("trip_distance") > 0)
            & (F.col("trip_distance") <= MAX_DISTANCE_MILES)
            & (F.col("trip_duration_minutes") > 0)
            & (F.col("trip_duration_minutes") <= MAX_DURATION_MINUTES)
        )
    )


def run_cleaning_pipeline(
    spark: SparkSession, months: tuple[str, ...] | None = None
) -> None:
    target_months = MONTHS if months is None else months
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    print(
        "Rules: drop non-positive or >24h durations (also catches null/inverted "
        "timestamps), non-positive or >500-mile distances; keep correlated nulls, "
        "negative amounts, uncommon codes, equal zones and N/A zones; "
        "leave total_amount unchanged."
    )
    for month in target_months:
        source = RAW_DIR / f"yellow_tripdata_{month}.parquet"
        trips = spark.read.parquet(str(source))
        original_count = trips.count()

        cleaned = clean_trips(trips).cache()
        cleaned_count = cleaned.count()
        removed = original_count - cleaned_count

        output = PROCESSED_DIR / f"yellow_tripdata_{month}_cleaned.parquet"
        cleaned.write.mode("overwrite").parquet(str(output))
        cleaned.unpersist()

        print(
            f"{month}: original={original_count:,} cleaned={cleaned_count:,} "
            f"removed={removed:,} retained={cleaned_count / original_count:.2%}"
        )
