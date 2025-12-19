# Data Engineering & CI/CD Pipeline (Iceberg/MLOps Focus)

## Overview
This project demonstrates an end-to-end data pipeline using Apache Iceberg, PySpark, and MLflow, managed by a CI/CD pipeline (GitHub Actions). It ingests new log data, merges it into an Iceberg table, trains a machine learning model, and registers the model artifact.

### Architecture
- **Data Ingestion & ETL:**
  - New log data is ingested and cleansed using PySpark.
  - A derived feature (`session_duration`) is added.
  - Data is merged/upserted into a partitioned Apache Iceberg table using transactional SQL (`MERGE INTO`).
- **ML Pipeline:**
  - Reads directly from the Iceberg table.
  - Performs feature engineering and trains a logistic regression model to predict long sessions.
  - Model is tracked and registered with MLflow.
- **CI/CD:**
  - GitHub Actions runs unit tests, schema checks, ETL, and ML scripts on push/PR.
  - Model artifacts are uploaded as build artifacts.

## Setup Instructions

### 1. Iceberg Metastore Setup
- Uses local Hadoop catalog for demonstration (see `etl/etl_iceberg.py`).
- For production, configure AWS Glue, GCP BigLake, or REST catalog as needed.

### 2. Running Locally
- Install dependencies:
  ```bash
  pip install pyspark mlflow apache-iceberg pytest
  ```
- Run ETL:
  ```bash
  python etl/etl_iceberg.py
  ```
- Run ML training:
  ```bash
  python ml/ml_train.py
  ```
- Run tests:
  ```bash
  pytest tests/
  ```

### 3. CI/CD Pipeline
- On push/PR to `main`, GitHub Actions will:
  - Run unit tests and schema checks
  - Run ETL and ML scripts
  - Upload model artifacts

### 4. Querying Iceberg Table
- Use Spark SQL or PySpark:
  ```python
  spark.read.format("iceberg").load("local_catalog.logs_db.user_sessions").show()
  ```

### 5. Viewing Registered Model
- Start MLflow UI:
  ```bash
  mlflow ui --backend-store-uri ./mlruns
  ```
- Open http://localhost:5000 to view model runs and artifacts.

## Iceberg Merge/Upsert Strategy
- Uses Spark SQL `MERGE INTO` for transactional upserts.
- Handles schema evolution and time travel (see Iceberg docs).

## Cloud Deployment
- For AWS/GCP/Azure, update the Spark and Iceberg configs in the scripts to use the appropriate metastore and storage.
- (Bonus) Use the provided Airflow DAG and Terraform template for orchestration and IaC.

## Sample Data
- See `sample_data/new_logs.csv` for example log data.

---

## Bonus: Orchestration & IaC
- See `infra/airflow_dag.py` and `infra/terraform/` for Airflow and Terraform examples (to be added).
