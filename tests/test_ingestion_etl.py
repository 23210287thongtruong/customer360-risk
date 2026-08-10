import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from chispa.dataframe_comparer import assert_df_equality
from spark_jobs.ingestion_etl import SparkIngestionETL
import datetime

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("pytest-pyspark-local-testing")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

@pytest.fixture
def etl_job():
    # Use dummy postgres configs since we'll mock the write
    return SparkIngestionETL(
        postgres_url="jdbc:postgresql://localhost:5432/dummy",
        postgres_properties={"user": "test", "password": "test", "driver": "org.postgresql.Driver"}
    )

def test_validate_dataframe_success(spark, etl_job):
    # Arrange
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ])
    
    data = [
        ("CUST-1", "John Doe", "john@example.com"),
        ("CUST-2", "Jane Smith", "jane@example.com")
    ]
    df = spark.createDataFrame(data, schema)
    
    # Act
    result_df = etl_job.validate_dataframe(df, "test_table", ["customer_id", "email"])
    
    # Assert - should return the dataframe unmodified
    assert_df_equality(result_df, df)

def test_validate_dataframe_missing_column(spark, etl_job):
    # Arrange
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True)
    ])
    
    data = [
        ("CUST-1", "John Doe"),
    ]
    df = spark.createDataFrame(data, schema)
    
    # Act & Assert
    with pytest.raises(ValueError, match="Missing required columns in test_table"):
        etl_job.validate_dataframe(df, "test_table", ["customer_id", "email"])
