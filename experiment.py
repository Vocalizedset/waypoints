"""Entry point for the batch versus incremental processing experiment."""

from pyspark.sql import SparkSession

from waypoints.experiments import run_experiment


def main() -> None:
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("waypoints-batch-vs-incremental")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_experiment(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
