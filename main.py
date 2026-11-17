"""Entry point for the waypoints data pipeline."""

from pyspark.sql import SparkSession

from waypoints.processing.cleaning import run_cleaning_pipeline


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("waypoints-pipeline")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_cleaning_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
