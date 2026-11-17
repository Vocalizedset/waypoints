"""Entry point for trip duration model training."""

from pyspark.sql import SparkSession

from waypoints.ml.duration import run_duration_training


def main() -> None:
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("waypoints-duration-training")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_duration_training(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
