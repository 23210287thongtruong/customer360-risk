from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class Customer360ETL:
    def __init__(self, postgres_url, postgres_properties):
        self.spark = (
            SparkSession.builder
            .appName("Customer360-Warehouse-ETL")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
            .config("spark.sql.shuffle.partitions", "20")
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

    def write_warehouse_table(self, df, table_name, mode="append", unique_key=None):
        properties = self.postgres_properties.copy()
        properties["truncate"] = "false"
        if unique_key and mode == "upsert":
            temp_table = f"temp_{table_name}_{int(datetime.now().timestamp())}"
            df.write.jdbc(
                url=self.postgres_url,
                table=f"warehouse.{temp_table}",
                mode="overwrite",
                properties=properties,
            )
            conn = self.spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
                self.postgres_url,
                self.postgres_properties["user"],
                self.postgres_properties["password"],
            )
            stmt = conn.createStatement()
            columns = df.columns
            column_list = ", ".join(columns)
            update_list = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in columns if col != unique_key]
            )
            upsert_sql = f"""
                INSERT INTO warehouse.{table_name} ({column_list})
                SELECT {column_list} FROM warehouse.{temp_table}
                ON CONFLICT ({unique_key}) DO UPDATE SET {update_list}
            """
            stmt.execute(upsert_sql)
            stmt.execute(f"DROP TABLE warehouse.{temp_table}")
            stmt.close()
            conn.close()
        else:
            df.write.jdbc(
                url=self.postgres_url,
                table=f"warehouse.{table_name}",
                mode=mode,
                properties=properties,
            )

    def transform_customers(self):
        logger.info("Transforming customers dimension...")
        
        customers_df = self.read_staging_table("customers")

        customers_repartitioned = customers_df.repartition(10, "customer_id")
        window_by_email = Window.partitionBy("email")
        
        customers_transformed = customers_repartitioned.select(
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
            datediff(current_date(), col("created_date")).alias("customer_tenure_days"),
            when(
                (col("email").isNotNull()) & 
                (col("name").isNotNull()) & 
                (col("customer_id").isNotNull()),
                lit(True)
            ).otherwise(lit(False)).alias("is_complete_record"),
            lit(True).alias("is_active"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
        
        # Cache for reuse
        customers_transformed.cache()
        
        logger.info(f"Transformed {customers_transformed.count()} customers")
        
        self.write_warehouse_table(
            customers_transformed,
            "dim_customer",
            mode="upsert",
            unique_key="customer_id",
        )
        return customers_transformed

    def transform_transactions(self, customers_df):
        logger.info("Transforming transactions fact table...")
        
        transactions_df = self.read_staging_table("transactions")

        customer_keys = customers_df.select("customer_id", "customer_key")

        try:
            existing_transactions = self.spark.read.jdbc(
                url=self.postgres_url,
                table="warehouse.fact_transactions",
                properties=self.postgres_properties,
            ).select("transaction_id")
            new_transactions = transactions_df.join(
                existing_transactions, "transaction_id", "left_anti"
            )
            logger.info(f"Found {new_transactions.count()} new transactions to process")
        except:
            logger.info("First run - processing all transactions")
            new_transactions = transactions_df
        
        # Advanced window functions for transaction analytics
        window_by_customer = Window.partitionBy("customer_id").orderBy("timestamp")
        window_by_customer_unbounded = Window.partitionBy("customer_id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        # Enrich transactions with advanced metrics
        transactions_enriched = new_transactions \
            .withColumn("transaction_date", to_date(col("timestamp"))) \
            .withColumn("transaction_timestamp", col("timestamp")) \
            .withColumn("transaction_sequence", row_number().over(window_by_customer)) \
            .withColumn("running_total", sum("amount").over(window_by_customer_unbounded)) \
            .withColumn("days_since_last_transaction", 
                       datediff(col("timestamp"), lag("timestamp").over(window_by_customer))) \
            .withColumn("amount_vs_avg", 
                       col("amount") - avg("amount").over(Window.partitionBy("customer_id"))) \
            .withColumn("is_high_value", 
                       when(col("amount") > 500, lit(True)).otherwise(lit(False)))
        
        # Broadcast join with customer dimension (optimization for small table)
        transactions_transformed = transactions_enriched.join(
            broadcast(customer_keys), "customer_id", "left"
        ).select(
            col("transaction_id"),
            col("customer_key"),
            col("customer_id"),
            col("transaction_type"),
            col("amount"),
            col("transaction_date"),
            col("transaction_timestamp"),
            col("merchant_name"),
            col("merchant_category"),
            col("location_city"),
            col("location_state"),
            col("is_weekend"),
            col("hour").alias("transaction_hour"),
            col("is_online"),
            col("is_fraud"),
            col("transaction_sequence"),
            col("running_total"),
            col("days_since_last_transaction"),
            col("amount_vs_avg"),
            col("is_high_value"),
            current_timestamp().alias("created_at"),
        )
        
        # Repartition by customer for optimal write performance
        transactions_partitioned = transactions_transformed.repartition(20, "customer_id")
        
        logger.info(f"Writing {transactions_partitioned.count()} transactions to warehouse")
        
        self.write_warehouse_table(
            transactions_partitioned, "fact_transactions", mode="append"
        )
        return transactions_partitioned

    def transform_credit_scores(self, customers_df):
        logger.info("Transforming credit dimension...")
        
        credit_df = self.read_staging_table("credit_scores")
        customer_keys = customers_df.select(
            "customer_id", "customer_key", "annual_income"
        )
        
        # Broadcast join for small dimension
        credit_transformed = credit_df.join(
            broadcast(customer_keys), "customer_id", "left"
        ).select(
            col("customer_key"),
            col("customer_id"),
            col("credit_score"),
            # Enhanced credit rating with more granular categories
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
            # Add risk indicators
            when(col("credit_utilization") > 0.8, lit("High"))
            .when(col("credit_utilization") > 0.5, lit("Medium"))
            .otherwise(lit("Low"))
            .alias("utilization_risk_level"),
            when((col("total_debt") / col("annual_income")) > 0.4, lit(True))
            .otherwise(lit(False))
            .alias("high_debt_burden"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at"),
        )
        
        logger.info(f"Transformed {credit_transformed.count()} credit records")
        
        self.write_warehouse_table(
            credit_transformed, "dim_credit", mode="upsert", unique_key="customer_id"
        )
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
