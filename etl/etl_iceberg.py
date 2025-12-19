"""
PySpark ETL script for ingesting, transforming, and merging data into an Apache Iceberg table.
Assumes local Hadoop or REST catalog for Iceberg metastore.
"""
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, (max as spark_max)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# Configurations (edit as needed)
ICEBERG_CATALOG = "local_catalog"
ICEBERG_WAREHOUSE = "./iceberg_warehouse"
ICEBERG_DB = "logs_db"
ICEBERG_TABLE = "user_sessions"
RAW_DATA_PATH = "../sample_data/new_logs.csv.gz"

# Define schema for raw data
def get_raw_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("login_time", StringType(), True),
        StructField("logout_time", StringType(), True),
        StructField("event_type", StringType(), True)
    ])

def create_spark():
    return SparkSession.builder \
        .appName("IcebergETL") \
        .config("spark.sql.catalog." + ICEBERG_CATALOG, "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog." + ICEBERG_CATALOG + ".type", "hadoop") \
        .config("spark.sql.catalog." + ICEBERG_CATALOG + ".warehouse", ICEBERG_WAREHOUSE) \
        .getOrCreate()

def cleanse_and_transform(df):
    # Convert login/logout to timestamps, drop nulls, add session_duration
    df = df.withColumn("login_time", col("login_time").cast(TimestampType())) \
           .withColumn("logout_time", col("logout_time").cast(TimestampType()))
    df = df.dropna(subset=["user_id", "session_id", "login_time", "logout_time"])
    df = df.withColumn("session_duration", (unix_timestamp(col("logout_time")) - unix_timestamp(col("login_time"))).cast(FloatType()))
    return df

def validate_schema(df, expected_schema):
    # Simple schema check: field names and types
    actual = [(f.name, f.dataType) for f in df.schema.fields]
    expected = [(f.name, f.dataType) for f in expected_schema.fields]
    return actual == expected

def main():
    spark = create_spark()
    raw_schema = get_raw_schema()
    # Read new data
    df = spark.read.csv(RAW_DATA_PATH, header=True, schema=raw_schema)
    df = cleanse_and_transform(df)
    # Define expected schema for Iceberg table
    iceberg_schema = StructType(raw_schema.fields + [StructField("session_duration", FloatType(), True)])
    if not validate_schema(df, iceberg_schema):
        print("Schema validation failed.")
        sys.exit(1)
    # Create Iceberg table if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE} (
            user_id STRING,
            session_id STRING,
            login_time TIMESTAMP,
            logout_time TIMESTAMP,
            event_type STRING,
            session_duration FLOAT
        )
        PARTITIONED BY (event_type)
    """)
    # Merge/Upsert logic: Use Iceberg's MERGE INTO (requires Spark 3.2+)
    df.createOrReplaceTempView("staging")
    merge_sql = f"""
        MERGE INTO {ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE} t
        USING staging s
        ON t.session_id = s.session_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    print("Data merged into Iceberg table.")
    spark.stop()

if __name__ == "__main__":
    main()
