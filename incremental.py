"""Entry point for incremental Waypoints processing."""

from pyspark.sql import SparkSession

from waypoints.incremental import run_incremental_pipeline


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("waypoints-incremental")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_incremental_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
