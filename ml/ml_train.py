"""
PySpark ML script for training a model using data from an Apache Iceberg table.
Performs feature engineering, model training, and model registration with MLflow.
"""
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import mlflow
import mlflow.spark

ICEBERG_CATALOG = "local_catalog"
ICEBERG_DB = "logs_db"
ICEBERG_TABLE = "user_sessions"
MLFLOW_TRACKING_URI = "./mlruns"  # Local tracking, can be S3 or remote
MODEL_NAME = "UserSessionModel"


def create_spark():
    return SparkSession.builder \
        .appName("IcebergML") \
        .config("spark.sql.catalog." + ICEBERG_CATALOG, "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog." + ICEBERG_CATALOG + ".type", "hadoop") \
        .config("spark.sql.catalog." + ICEBERG_CATALOG + ".warehouse", "./iceberg_warehouse") \
        .getOrCreate()

def main():
    spark = create_spark()
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("UserSessionML")

    # Read data from Iceberg table
    df = spark.read.format("iceberg").load(f"{ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE}")
    # Feature engineering: Predict if session_duration > threshold (e.g., 600s)
    threshold = 600
    df = df.withColumn("long_session", (df.session_duration > threshold).cast("int"))
    # Index categorical features
    indexer = StringIndexer(inputCol="event_type", outputCol="event_type_idx")
    assembler = VectorAssembler(
        inputCols=["event_type_idx", "session_duration"],
        outputCol="features"
    )
    lr = LogisticRegression(featuresCol="features", labelCol="long_session")
    pipeline = Pipeline(stages=[indexer, assembler, lr])

    # Train/test split
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    with mlflow.start_run() as run:
        model = pipeline.fit(train)
        predictions = model.transform(test)
        accuracy = predictions.filter(predictions.long_session == predictions.prediction).count() / float(predictions.count())
        mlflow.log_metric("accuracy", accuracy)
        mlflow.spark.log_model(model, MODEL_NAME)
        mlflow.log_param("threshold", threshold)
        print(f"Model accuracy: {accuracy}")
        print(f"Model saved to MLflow run: {run.info.run_id}")
    spark.stop()

if __name__ == "__main__":
    main()
