from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, col, sum as spark_sum,
    trim, lower
)
from pyspark.sql.types import *
import logging
import sys

logger = logging.getLogger(__name__)


class SparkIngestionETL:
    def __init__(self, postgres_url, postgres_properties):
        self.spark = (
            SparkSession.builder
            .appName("Customer360-Spark-Ingestion")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        )
        self.postgres_url = postgres_url
        self.postgres_properties = postgres_properties
        self.spark.sparkContext.setLogLevel("WARN")

    def validate_dataframe(self, df, table_name, required_columns):
        logger.info(f"Validating {table_name} data quality...")
        
        # Check for required columns
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns in {table_name}: {missing_cols}")
        
        # Check for null values in critical columns
        null_counts = df.select([
            spark_sum(col(c).isNull().cast("int")).alias(c) 
            for c in required_columns
        ]).collect()[0].asDict()
        
        critical_nulls = {k: v for k, v in null_counts.items() if v > 0}
        if critical_nulls:
            logger.warning(f"Null values found in {table_name}: {critical_nulls}")
        
        # Calculate data quality metrics
        total_rows = df.count()
        logger.info(f"{table_name}: {total_rows} rows ingested")
        
        return df

    def ingest_customers(self, csv_path):
        """
        Ingest customer data from CSV using Spark with comprehensive validation.
        """
        logger.info(f"Ingesting customers from {csv_path}")
        
        # Read CSV with schema inference and options for data quality
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "DROPMALFORMED") \
            .option("encoding", "UTF-8") \
            .csv(csv_path)
        
        # Data cleansing transformations
        df_clean = df.select(
            col("customer_id"),
            trim(col("name")).alias("name"),
            col("date_of_birth").cast(DateType()),
            trim(col("address")).alias("address"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            col("zip_code"),
            col("phone"),
            lower(trim(col("email"))).alias("email"),
            col("annual_income").cast(DoubleType()),
            trim(col("job_title")).alias("job_title"),
            col("employment_status"),
            col("marital_status"),
            col("created_date").cast(DateType())
        )
        
        # Add audit columns (best practice for data lineage)
        df_audited = df_clean \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(csv_path)) \
            .withColumn("data_source", lit("synthetic_generator"))
        
        # Validate data quality
        required_columns = ["customer_id", "name", "email"]
        df_validated = self.validate_dataframe(df_audited, "customers", required_columns)
        
        # Write to PostgreSQL staging with truncate (handles deterministic IDs)
        df_validated.write \
            .format("jdbc") \
            .option("url", self.postgres_url) \
            .option("dbtable", "staging.customers") \
            .option("user", self.postgres_properties["user"]) \
            .option("password", self.postgres_properties["password"]) \
            .option("driver", self.postgres_properties["driver"]) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()
        
        logger.info(f"Successfully ingested {df_validated.count()} customers")
        return df_validated

    def ingest_transactions(self, csv_path):
        """
        Ingest transaction data with advanced Spark transformations.
        """
        logger.info(f"Ingesting transactions from {csv_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "DROPMALFORMED") \
            .csv(csv_path)
        
        # Data cleansing and type casting
        df_clean = df.select(
            col("transaction_id"),
            col("customer_id"),
            col("transaction_type"),
            col("amount").cast(DoubleType()),
            col("timestamp").cast(TimestampType()),
            trim(col("merchant_name")).alias("merchant_name"),
            col("merchant_category"),
            trim(col("location_city")).alias("location_city"),
            trim(col("location_state")).alias("location_state"),
            col("is_weekend").cast(BooleanType()),
            col("hour").cast(IntegerType()),
            col("is_online").cast(BooleanType()),
            col("is_fraud").cast(BooleanType())
        )
        
        # Add audit columns
        df_audited = df_clean \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(csv_path))
        
        # Validate
        required_columns = ["transaction_id", "customer_id", "amount"]
        df_validated = self.validate_dataframe(df_audited, "transactions", required_columns)
        
        # Write to staging
        df_validated.write \
            .format("jdbc") \
            .option("url", self.postgres_url) \
            .option("dbtable", "staging.transactions") \
            .option("user", self.postgres_properties["user"]) \
            .option("password", self.postgres_properties["password"]) \
            .option("driver", self.postgres_properties["driver"]) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()
        
        logger.info(f"Successfully ingested {df_validated.count()} transactions")
        return df_validated

    def ingest_credit_scores(self, csv_path):
        """
        Ingest credit score data with validation.
        """
        logger.info(f"Ingesting credit scores from {csv_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "DROPMALFORMED") \
            .csv(csv_path)
        
        # Data cleansing
        df_clean = df.select(
            col("customer_id"),
            col("credit_score").cast(IntegerType()),
            col("score_date").cast(DateType()),
            col("credit_history_length").cast(IntegerType()),
            col("number_of_accounts").cast(IntegerType()),
            col("total_debt").cast(DoubleType()),
            col("credit_utilization").cast(DoubleType())
        )
        
        # Add business rule validation for credit scores
        df_validated_scores = df_clean.filter(
            (col("credit_score").between(300, 850)) | col("credit_score").isNull()
        )
        
        invalid_count = df_clean.count() - df_validated_scores.count()
        if invalid_count > 0:
            logger.warning(f"Filtered out {invalid_count} records with invalid credit scores")
        
        # Add audit columns
        df_audited = df_validated_scores \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", lit(csv_path))
        
        # Validate
        required_columns = ["customer_id", "credit_score"]
        df_final = self.validate_dataframe(df_audited, "credit_scores", required_columns)
        
        # Write to staging
        df_final.write \
            .format("jdbc") \
            .option("url", self.postgres_url) \
            .option("dbtable", "staging.credit_scores") \
            .option("user", self.postgres_properties["user"]) \
            .option("password", self.postgres_properties["password"]) \
            .option("driver", self.postgres_properties["driver"]) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()
        
        logger.info(f"Successfully ingested {df_final.count()} credit scores")
        return df_final

    def run_ingestion(self, data_dir="/opt/airflow/data/raw"):
        """
        Run complete ingestion pipeline for all data sources.
        """
        try:
            logger.info("Starting Spark-based data ingestion...")
            
            # Ingest all data sources
            customers_df = self.ingest_customers(f"{data_dir}/customers.csv")
            transactions_df = self.ingest_transactions(f"{data_dir}/transactions.csv")
            credit_df = self.ingest_credit_scores(f"{data_dir}/credit_scores.csv")
            
            # Print summary statistics
            logger.info("=" * 50)
            logger.info("Ingestion Summary:")
            logger.info(f"  Customers: {customers_df.count()}")
            logger.info(f"  Transactions: {transactions_df.count()}")
            logger.info(f"  Credit Scores: {credit_df.count()}")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Ingestion failed: {str(e)}")
            raise
        finally:
            self.spark.stop()


def main():
    # PostgreSQL connection details
    postgres_url = "jdbc:postgresql://postgres:5432/customer360_dw"
    postgres_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    }
    
    # Get data directory from command line or use default
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/data/raw"
    
    # Run ingestion
    etl = SparkIngestionETL(postgres_url, postgres_properties)
    etl.run_ingestion(data_dir)


if __name__ == "__main__":
    main()
