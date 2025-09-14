from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)


class RiskScoringETL:
    def __init__(self, postgres_url, postgres_properties):
        self.spark = (
            SparkSession.builder.appName("Customer360-Risk-Scoring")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        self.postgres_url = postgres_url
        self.postgres_properties = postgres_properties
        self.spark.sparkContext.setLogLevel("WARN")

    def read_warehouse_table(self, table_name):
        return self.spark.read.jdbc(
            url=self.postgres_url,
            table=f"warehouse.{table_name}",
            properties=self.postgres_properties,
        )

    def write_analytics_table(self, df, table_name, mode="overwrite"):
        if table_name == "customer_360":
            conn = self.spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
                self.postgres_url,
                self.postgres_properties["user"],
                self.postgres_properties["password"],
            )
            stmt = conn.createStatement()
            stmt.execute(f"TRUNCATE TABLE analytics.{table_name};")
            stmt.close()
            conn.close()
            mode = "append"
        df.write.jdbc(
            url=self.postgres_url,
            table=f"analytics.{table_name}",
            mode=mode,
            properties=self.postgres_properties,
        )

    def calculate_transaction_metrics(self, customer_df, transactions_df):
        transaction_metrics = transactions_df.groupBy("customer_id").agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").alias("total_spent"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.max("amount").alias("max_transaction_amount"),
            F.min("transaction_date").alias("first_transaction_date"),
            F.max("transaction_date").alias("last_transaction_date"),
            F.datediff(F.current_date(), F.max("transaction_date")).alias(
                "days_since_last_transaction"
            ),
            F.first(F.col("merchant_category")).alias("favorite_merchant_category"),
            (
                F.sum(F.when(F.col("is_online") == True, 1).otherwise(0)) / F.count("*")
            ).alias("online_transaction_pct"),
            # Calculate weekend transaction percentage
            (
                F.sum(F.when(F.col("is_weekend") == True, 1).otherwise(0))
                / F.count("*")
            ).alias("weekend_transaction_pct"),
        )

        return transaction_metrics

    def calculate_risk_score(self, customer_360_df):
        """Calculate risk score based on business rules"""
        logger.info("Calculating risk scores...")

        risk_scored_df = customer_360_df.select(
            "*",
            (
                F.when(F.col("credit_score") < 580, 30)
                .when(F.col("credit_score") < 620, 20)
                .when(F.col("credit_score") < 670, 10)
                .otherwise(0)
                + F.when(F.col("credit_utilization") > 0.8, 20)
                .when(F.col("credit_utilization") > 0.6, 15)
                .when(F.col("credit_utilization") > 0.4, 10)
                .otherwise(0)
                + F.when(F.col("debt_to_income_ratio") > 0.4, 25)
                .when(F.col("debt_to_income_ratio") > 0.3, 15)
                .when(F.col("debt_to_income_ratio") > 0.2, 10)
                .otherwise(0)
                + F.when(F.col("total_spent") > 10000, 10)
                .when(F.col("total_spent") > 5000, 5)
                .otherwise(0)
                + F.when(F.col("annual_income") < 30000, 10)
                .when(F.col("annual_income") < 50000, 5)
                .otherwise(0)
                + F.when(F.col("days_since_last_transaction") > 90, 5)
                .when(F.col("days_since_last_transaction") > 30, 2)
                .otherwise(0)
            ).alias("risk_score"),
            F.array_remove(
                F.array(
                    F.when(F.col("credit_score") < 600, F.lit("Low Credit Score")),
                    F.when(
                        F.col("credit_utilization") > 0.8,
                        F.lit("High Credit Utilization"),
                    ),
                    F.when(
                        F.col("debt_to_income_ratio") > 0.4,
                        F.lit("High Debt to Income"),
                    ),
                    F.when(F.col("total_spent") > 10000, F.lit("High Spending Volume")),
                    F.when(F.col("annual_income") < 30000, F.lit("Low Income")),
                    F.when(
                        F.col("days_since_last_transaction") > 90,
                        F.lit("Inactive Customer"),
                    ),
                ),
                None,  # remove nulls from array
            ).alias("risk_factors"),
        )

        risk_categorized_df = risk_scored_df.select(
            "*",
            F.when(F.col("risk_score") >= 60, "High")
            .when(F.col("risk_score") >= 30, "Medium")
            .otherwise("Low")
            .alias("risk_category"),
        )

        return risk_categorized_df

    def create_customer_360_view(self):
        customers_df = self.read_warehouse_table("dim_customer")
        transactions_df = self.read_warehouse_table("fact_transactions")
        credit_df = self.read_warehouse_table("dim_credit")

        transaction_metrics = self.calculate_transaction_metrics(
            customers_df, transactions_df
        )

        # Join all data sources
        customer_360_base = (
            customers_df.join(credit_df, "customer_key", "left")
            .join(transaction_metrics, "customer_id", "left")
            .select(
                customers_df["customer_key"],
                customers_df["customer_id"],
                customers_df["name"],
                customers_df["date_of_birth"],
                customers_df["age"],
                customers_df["city"],
                customers_df["state"],
                customers_df["annual_income"],
                customers_df["income_bracket"],
                customers_df["job_title"],
                customers_df["employment_status"],
                customers_df["marital_status"],
                F.coalesce(F.col("total_transactions"), F.lit(0)).alias(
                    "total_transactions"
                ),
                F.coalesce(F.col("total_spent"), F.lit(0.0)).alias("total_spent"),
                F.coalesce(F.col("avg_transaction_amount"), F.lit(0.0)).alias(
                    "avg_transaction_amount"
                ),
                F.coalesce(F.col("max_transaction_amount"), F.lit(0.0)).alias(
                    "max_transaction_amount"
                ),
                F.col("first_transaction_date"),
                F.col("last_transaction_date"),
                F.coalesce(F.col("days_since_last_transaction"), F.lit(9999)).alias(
                    "days_since_last_transaction"
                ),
                F.col("favorite_merchant_category"),
                F.coalesce(F.col("online_transaction_pct"), F.lit(0.0)).alias(
                    "online_transaction_pct"
                ),
                F.coalesce(F.col("weekend_transaction_pct"), F.lit(0.0)).alias(
                    "weekend_transaction_pct"
                ),
                F.coalesce(credit_df["credit_score"], F.lit(300)).alias("credit_score"),
                F.coalesce(credit_df["credit_rating"], F.lit("Poor")).alias(
                    "credit_rating"
                ),
                F.coalesce(credit_df["credit_history_length"], F.lit(0)).alias(
                    "credit_history_length"
                ),
                F.coalesce(credit_df["total_debt"], F.lit(0.0)).alias("total_debt"),
                F.coalesce(credit_df["credit_utilization"], F.lit(0.0)).alias(
                    "credit_utilization"
                ),
                F.coalesce(credit_df["debt_to_income_ratio"], F.lit(0.0)).alias(
                    "debt_to_income_ratio"
                ),
            )
        )

        customer_360_final = self.calculate_risk_score(customer_360_base).select(
            "*", F.current_timestamp().alias("last_updated")
        )

        self.write_analytics_table(customer_360_final, "customer_360")
        return customer_360_final

    def create_risk_history(self, customer_360_df):
        risk_history_df = customer_360_df.select(
            F.col("customer_id"),
            F.col("risk_score"),
            F.col("risk_category"),
            F.col("risk_factors"),
            F.current_date().alias("assessment_date"),
            F.current_timestamp().alias("created_at"),
        )

        self.write_analytics_table(
            risk_history_df, "customer_risk_history", mode="append"
        )
        return risk_history_df

    def run_etl(self):
        try:
            customer_360_df = self.create_customer_360_view()
            self.create_risk_history(customer_360_df)
            self.print_summary_stats(customer_360_df)
        except Exception as e:
            logger.error(f"ETL failed: {str(e)}")
            raise
        finally:
            self.spark.stop()

    def print_summary_stats(self, customer_360_df):
        total_customers = customer_360_df.count()
        risk_distribution = customer_360_df.groupBy("risk_category").count().collect()
        avg_stats = customer_360_df.agg(
            F.avg("credit_score").alias("avg_credit_score"),
            F.avg("annual_income").alias("avg_income"),
            F.avg("total_spent").alias("avg_spent"),
            F.avg("risk_score").alias("avg_risk_score"),
        ).collect()[0]


def main():
    postgres_url = "jdbc:postgresql://postgres:5432/customer360_dw"
    postgres_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    }

    etl = RiskScoringETL(postgres_url, postgres_properties)
    etl.run_etl()


if __name__ == "__main__":
    main()
