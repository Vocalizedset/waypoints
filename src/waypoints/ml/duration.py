"""Trip duration prediction with PySpark MLlib.

The label is ``trip_duration_minutes``. Input features are pickup/drop-off
location, trip distance, pickup hour, pickup day of week and passenger count;
drop-off timestamps and the label itself are never used as inputs.

All feature-engineered months are combined, then a deterministic random
sample without replacement (seed 42) targeting 200,000 eligible rows is used
to keep training laptop-friendly. If fewer than 200,000 rows are eligible,
all of them are used.

Categorical fields are string-indexed and one-hot encoded; numeric fields are
median-imputed and standardized. Unseen/invalid categories are kept as an
extra bucket by the indexer and encoder.
"""

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import (
    DecisionTreeRegressor,
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from waypoints.analytics.batch import PREFIX, discover_months
from waypoints.processing.cleaning import OUTPUT_DIR, PROCESSED_DIR

LABEL = "trip_duration_minutes"
CATEGORICAL = ("PULocationID", "DOLocationID", "pickup_hour", "pickup_day_of_week")
NUMERIC = ("trip_distance", "passenger_count")
SEED = 42
SAMPLE_ROWS = 200_000
METRICS_PATH = OUTPUT_DIR / "trip_duration_model_metrics.parquet"
CSV_PATH = OUTPUT_DIR / "trip_duration_model_metrics.csv"
MODEL_PATH = OUTPUT_DIR / "trip_duration_best_model"


def prepare_data(spark: SparkSession) -> DataFrame:
    """Select eligible rows using only the allowed features and the label."""
    months = discover_months()
    paths = [str(PROCESSED_DIR / f"{PREFIX}{month}_features.parquet") for month in months]
    trips = spark.read.parquet(*paths)

    return trips.select(
        F.col(LABEL).alias(LABEL),
        F.coalesce(F.col("PULocationID").cast("string"), F.lit("Unknown")).alias("PULocationID"),
        F.coalesce(F.col("DOLocationID").cast("string"), F.lit("Unknown")).alias("DOLocationID"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("pickup_hour").cast("string").alias("pickup_hour"),
        F.col("pickup_day_of_week").cast("string").alias("pickup_day_of_week"),
        F.when(F.col("passenger_count") < 0, F.lit(None).cast("double"))
        .otherwise(F.col("passenger_count").cast("double"))
        .alias("passenger_count"),
    ).filter(
        F.col(LABEL).isNotNull()
        & (F.col(LABEL) > 0)
        & (F.col(LABEL) <= 24 * 60)
        & F.col("trip_distance").isNotNull()
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= 500)
    )


def sample_data(data: DataFrame) -> DataFrame:
    """Deterministic sample without replacement, capped at SAMPLE_ROWS."""
    total = data.count()
    fraction = min(1.0, SAMPLE_ROWS / total) if total else 0.0
    return data.sample(withReplacement=False, fraction=fraction, seed=SEED).limit(SAMPLE_ROWS)


def preprocessing_stages() -> list:
    indexers = [
        StringIndexer(inputCol=column, outputCol=f"{column}_idx", handleInvalid="keep")
        for column in CATEGORICAL
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{column}_idx" for column in CATEGORICAL],
        outputCols=[f"{column}_vec" for column in CATEGORICAL],
        handleInvalid="keep",
    )
    imputed = [f"{column}_imp" for column in NUMERIC]
    imputer = Imputer(inputCols=list(NUMERIC), outputCols=imputed, strategy="median")
    numeric_assembler = VectorAssembler(inputCols=imputed, outputCol="numeric_features")
    scaler = StandardScaler(
        inputCol="numeric_features", outputCol="numeric_scaled", withMean=True, withStd=True
    )
    assembler = VectorAssembler(
        inputCols=[f"{column}_vec" for column in CATEGORICAL] + ["numeric_scaled"],
        outputCol="features",
    )
    return indexers + [encoder, imputer, numeric_assembler, scaler, assembler]


def model_estimators() -> dict[str, object]:
    return {
        "LinearRegression": LinearRegression(
            featuresCol="features", labelCol=LABEL, maxIter=50, regParam=0.1
        ),
        "DecisionTreeRegressor": DecisionTreeRegressor(
            featuresCol="features", labelCol=LABEL, maxDepth=8,
            minInstancesPerNode=50, seed=SEED,
        ),
        "RandomForestRegressor": RandomForestRegressor(
            featuresCol="features", labelCol=LABEL, numTrees=15, maxDepth=8,
            minInstancesPerNode=50, seed=SEED,
        ),
        "GBTRegressor": GBTRegressor(
            featuresCol="features", labelCol=LABEL, maxIter=15, maxDepth=6,
            stepSize=0.1, seed=SEED,
        ),
    }


def run_duration_training(spark: SparkSession) -> None:
    eligible = prepare_data(spark)
    sample = sample_data(eligible).cache()
    sample_count = sample.count()
    print(
        f"ML sample: {sample_count:,} rows (target {SAMPLE_ROWS:,}, "
        f"seed {SEED}, sample without replacement)"
    )

    train, test = sample.randomSplit([0.8, 0.2], seed=SEED)
    train.cache()
    test.cache()

    preprocessing = preprocessing_stages()
    evaluator = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction")
    results = []
    best_name, best_model, best_rmse = None, None, float("inf")

    for name, estimator in model_estimators().items():
        model = Pipeline(stages=preprocessing + [estimator]).fit(train)
        predictions = model.transform(test)
        rmse = evaluator.setMetricName("rmse").evaluate(predictions)
        mae = evaluator.setMetricName("mae").evaluate(predictions)
        r2 = evaluator.setMetricName("r2").evaluate(predictions)
        print(f"{name}: RMSE={rmse:.3f} MAE={mae:.3f} R2={r2:.3f}")
        results.append((name, rmse, mae, r2))
        if rmse < best_rmse:
            best_name, best_model, best_rmse = name, model, rmse

    schema = StructType([
        StructField("model_name", StringType(), False),
        StructField("rmse", DoubleType(), False),
        StructField("mae", DoubleType(), False),
        StructField("r2", DoubleType(), False),
        StructField("is_best", BooleanType(), False),
    ])
    metrics = spark.createDataFrame(
        [(name, rmse, mae, r2, name == best_name) for name, rmse, mae, r2 in results],
        schema,
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    metrics.write.mode("overwrite").parquet(str(METRICS_PATH))
    metrics.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(CSV_PATH))
    best_model.write().overwrite().save(str(MODEL_PATH))
    print(f"Best model: {best_name} (RMSE={best_rmse:.3f}) saved to {MODEL_PATH}")

    train.unpersist()
    test.unpersist()
    sample.unpersist()
