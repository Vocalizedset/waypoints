"""Entry point for the waypoints data pipeline."""

from pyspark.sql import SparkSession

from waypoints.analytics.batch import run_batch_analytics
from waypoints.processing.cleaning import run_cleaning_pipeline
from waypoints.processing.features import run_feature_engineering
from waypoints.processing.zones import run_zone_enrichment


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
        run_zone_enrichment(spark)
        run_feature_engineering(spark)
        run_batch_analytics(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
