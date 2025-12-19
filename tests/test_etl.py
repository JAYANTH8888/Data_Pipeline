import pytest
from pyspark.sql import SparkSession
from etl.etl_iceberg import cleanse_and_transform, get_raw_schema
from pyspark.sql.types import FloatType

def spark_session():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_cleanse_and_transform():
    spark = spark_session()
    data = [("u1", "s1", "2025-12-13 10:00:00", "2025-12-13 10:20:00", "login")]
    df = spark.createDataFrame(data, schema=get_raw_schema())
    result = cleanse_and_transform(df)
    assert "session_duration" in result.columns
    assert result.schema["session_duration"].dataType == FloatType()
    row = result.collect()[0]
    assert abs(row.session_duration - 1200.0) < 1
    spark.stop()
