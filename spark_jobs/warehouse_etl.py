from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


class Customer360ETL:
    def __init__(self, postgres_url, postgres_properties):
        self.spark = (
            SparkSession.builder.appName("Customer360-Warehouse-ETL")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        self.postgres_url = postgres_url
        self.postgres_properties = postgres_properties
        self.spark.sparkContext.setLogLevel("WARN")

    def read_staging_table(self, table_name):
        return self.spark.read.jdbc(
            url=self.postgres_url,
            table=f"staging.{table_name}",
            properties=self.postgres_properties,
        )

    def write_warehouse_table(self, df, table_name, mode="append"):
        properties = self.postgres_properties.copy()
        properties["truncate"] = "false"
        df.write.jdbc(
            url=self.postgres_url,
            table=f"warehouse.{table_name}",
            mode=mode,
            properties=properties,
        )

    def transform_customers(self):
        customers_df = self.read_staging_table("customers")

        customers_transformed = customers_df.select(
            col("customer_id"),
            col("name"),
            col("date_of_birth"),
            floor(datediff(current_date(), col("date_of_birth")) / 365.25).alias("age"),
            concat_ws(
                ", ", col("address"), col("city"), col("state"), col("zip_code")
            ).alias("full_address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("phone"),
            col("email"),
            col("annual_income"),
            when(col("annual_income") < 30000, "Low Income")
            .when(col("annual_income") < 50000, "Lower Middle")
            .when(col("annual_income") < 75000, "Middle")
            .when(col("annual_income") < 100000, "Upper Middle")
            .otherwise("High Income")
            .alias("income_bracket"),
            col("job_title"),
            col("employment_status"),
            col("marital_status"),
            col("created_date").alias("customer_since"),
            lit(True).alias("is_active"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )

        self.write_warehouse_table(customers_transformed, "dim_customer")
        return customers_transformed

    def transform_transactions(self, customers_df):
        transactions_df = self.read_staging_table("transactions")
        customer_keys = customers_df.select("customer_id", "customer_key")

        # Join with customer keys and transform
        transactions_transformed = transactions_df.join(
            customer_keys, "customer_id", "left"
        ).select(
            col("transaction_id"),
            col("customer_key"),
            col("customer_id"),
            col("transaction_type"),
            col("amount"),
            to_date(col("timestamp")).alias("transaction_date"),
            col("timestamp").alias("transaction_timestamp"),
            col("merchant_name"),
            col("merchant_category"),
            col("location_city"),
            col("location_state"),
            col("is_weekend"),
            col("hour").alias("transaction_hour"),
            col("is_online"),
            col("is_fraud"),
            current_timestamp().alias("created_at"),
        )

        self.write_warehouse_table(transactions_transformed, "fact_transactions")
        return transactions_transformed

    def transform_credit_scores(self, customers_df):
        credit_df = self.read_staging_table("credit_scores")
        customer_keys = customers_df.select(
            "customer_id", "customer_key", "annual_income"
        )

        credit_transformed = credit_df.join(
            customer_keys, "customer_id", "left"
        ).select(
            col("customer_key"),
            col("customer_id"),
            col("credit_score"),
            when(col("credit_score") >= 800, "Excellent")
            .when(col("credit_score") >= 740, "Very Good")
            .when(col("credit_score") >= 670, "Good")
            .when(col("credit_score") >= 580, "Fair")
            .otherwise("Poor")
            .alias("credit_rating"),
            col("score_date"),
            col("credit_history_length"),
            col("number_of_accounts"),
            col("total_debt"),
            col("credit_utilization"),
            (col("total_debt") / col("annual_income")).alias("debt_to_income_ratio"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )

        self.write_warehouse_table(credit_transformed, "dim_credit")
        return credit_transformed

    def run_etl(self):
        try:
            customers_df = self.transform_customers()

            customers_with_keys = self.spark.read.jdbc(
                url=self.postgres_url,
                table="warehouse.dim_customer",
                properties=self.postgres_properties,
            )

            self.transform_transactions(customers_with_keys)
            self.transform_credit_scores(customers_with_keys)

        except Exception as e:
            logger.error(f"ETL failed: {str(e)}")
            raise
        finally:
            self.spark.stop()


def main():
    postgres_url = "jdbc:postgresql://postgres:5432/customer360_dw"
    postgres_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    }

    etl = Customer360ETL(postgres_url, postgres_properties)
    etl.run_etl()


if __name__ == "__main__":
    main()
