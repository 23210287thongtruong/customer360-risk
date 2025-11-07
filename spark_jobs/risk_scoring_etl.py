from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, Bucketizer
from pyspark.ml import Pipeline
import logging

logger = logging.getLogger(__name__)


class RiskScoringETL:
    def __init__(self, postgres_url, postgres_properties):
        self.spark = (
            SparkSession.builder
            .appName("Customer360-Risk-Scoring-ML")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.shuffle.partitions", "20")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
        logger.info("Calculating transaction metrics with advanced aggregations...")
        
        # Window for transaction analytics
        window_by_customer = Window.partitionBy("customer_id")
        
        transaction_metrics = transactions_df.groupBy("customer_id").agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").alias("total_spent"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.max("amount").alias("max_transaction_amount"),
            F.min("amount").alias("min_transaction_amount"),
            F.stddev("amount").alias("transaction_amount_stddev"),  # Volatility measure
            F.min("transaction_date").alias("first_transaction_date"),
            F.max("transaction_date").alias("last_transaction_date"),
            F.datediff(F.current_date(), F.max("transaction_date")).alias(
                "days_since_last_transaction"
            ),
            # Advanced analytics
            F.first(F.col("merchant_category")).alias("favorite_merchant_category"),
            F.countDistinct("merchant_category").alias("unique_merchant_categories"),
            F.countDistinct("merchant_name").alias("unique_merchants"),
            (
                F.sum(F.when(F.col("is_online") == True, 1).otherwise(0)) / F.count("*")
            ).alias("online_transaction_pct"),
            (
                F.sum(F.when(F.col("is_weekend") == True, 1).otherwise(0))
                / F.count("*")
            ).alias("weekend_transaction_pct"),
            # Fraud indicators
            F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count"),
            (
                F.sum(F.when(F.col("is_fraud") == True, F.col("amount")).otherwise(0))
            ).alias("fraud_amount"),
            # Time-based patterns
            F.avg("transaction_hour").alias("avg_transaction_hour"),
            # High-value transaction analysis
            F.sum(F.when(F.col("amount") > 500, 1).otherwise(0)).alias("high_value_transaction_count"),
        )
        
        logger.info(f"Calculated metrics for {transaction_metrics.count()} customers")
        return transaction_metrics

    def calculate_risk_score(self, customer_360_df):
        feature_cols = [
            "credit_score", "credit_utilization", "debt_to_income_ratio",
            "total_spent", "annual_income", "days_since_last_transaction",
            "transaction_amount_stddev", "total_transactions"
        ]

        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features",
            handleInvalid="keep"
        )

        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )

        bucketizer = Bucketizer(
            splits=[0.0, 20.0, 40.0, 60.0, 80.0, 100.0],
            inputCol="raw_risk_score",
            outputCol="risk_bucket"
        )

        pipeline = Pipeline(stages=[assembler, scaler])

        @F.udf(DoubleType())
        def calculate_raw_risk(credit_score, credit_util, debt_income,
                               total_spent, income, days_inactive, volatility):
            credit_score = float(credit_score) if credit_score else 600.0
            credit_util = float(credit_util) if credit_util else 0.5
            debt_income = float(debt_income) if debt_income else 0.2
            total_spent = float(total_spent) if total_spent else 0.0
            income = float(income) if income else 50000.0
            days_inactive = float(days_inactive) if days_inactive else 0.0
            volatility = float(volatility) if volatility else 100.0

            # Weighted risk components
            credit_risk = (850 - credit_score) / 850 * 40      # 40% weight
            util_risk = min(credit_util, 1.0) * 20             # 20% weight
            debt_risk = min(debt_income / 0.5, 1.0) * 25       # 25% weight
            volatility_risk = min(volatility / 1000, 1.0) * 10 # 10% weight
            inactivity_risk = min(days_inactive / 180, 1.0) * 5 # 5% weight

            total_risk = credit_risk + util_risk + debt_risk + volatility_risk + inactivity_risk
            return min(max(total_risk, 0.0), 100.0)

        # Add raw risk score column
        df_with_raw_risk = customer_360_df.withColumn(
            "raw_risk_score",
            calculate_raw_risk(
                F.col("credit_score"), F.col("credit_utilization"),
                F.col("debt_to_income_ratio"), F.col("total_spent"),
                F.col("annual_income"), F.col("days_since_last_transaction"),
                F.col("transaction_amount_stddev")
            )
        )

        pipeline_model = pipeline.fit(df_with_raw_risk)
        df_with_features = pipeline_model.transform(df_with_raw_risk)

        bucketed_df = bucketizer.transform(df_with_features)

        final_df = bucketed_df.select(
            "*",
            # Risk category based on bucket
            F.when(F.col("risk_bucket") == 0.0, "Very Low")
            .when(F.col("risk_bucket") == 1.0, "Low")
            .when(F.col("risk_bucket") == 2.0, "Medium")
            .when(F.col("risk_bucket") == 3.0, "High")
            .otherwise("Very High")
            .alias("risk_category"),

            # Primary risk score
            F.col("raw_risk_score").alias("risk_score"),

            # Risk factors array (ML-enhanced identification)
            F.array_remove(
                F.array(
                    F.when(F.col("credit_score") < 600, F.lit("Low Credit Score")),
                    F.when(F.col("credit_utilization") > 0.8, F.lit("High Credit Utilization")),
                    F.when(F.col("debt_to_income_ratio") > 0.4, F.lit("High Debt to Income")),
                    F.when(F.col("total_spent") > 10000, F.lit("High Spending Volume")),
                    F.when(F.col("annual_income") < 30000, F.lit("Low Income")),
                    F.when(F.col("days_since_last_transaction") > 90, F.lit("Inactive Customer")),
                    F.when(F.col("fraud_count") > 0, F.lit("Fraud History")),
                    F.when(F.col("transaction_amount_stddev") > 500, F.lit("High Transaction Volatility")),
                ),
                None,
            ).alias("risk_factors"),

            # ML pipeline metadata
            F.current_timestamp().alias("ml_scored_at"),
            F.lit("Spark ML Pipeline v1.0").alias("scoring_model_version")
        )

        logger.info(f"Spark ML risk scoring completed for {final_df.count()} customers")
        return final_df

    def create_customer_360_view(self):
        """
        Create comprehensive customer 360 view with advanced Spark optimizations.
        """
        logger.info("Creating Customer 360 view with advanced optimizations...")
        
        # Read warehouse tables
        customers_df = self.read_warehouse_table("dim_customer")
        transactions_df = self.read_warehouse_table("fact_transactions")
        credit_df = self.read_warehouse_table("dim_credit")
        
        # Cache frequently accessed dimension table
        customers_df.cache()
        credit_df.cache()
        
        logger.info(f"Loaded {customers_df.count()} customers")
        logger.info(f"Loaded {transactions_df.count()} transactions")
        logger.info(f"Loaded {credit_df.count()} credit records")
        
        # Calculate transaction metrics
        transaction_metrics = self.calculate_transaction_metrics(
            customers_df, transactions_df
        )
        
        # Cache transaction metrics for reuse
        transaction_metrics.cache()
        
        # Broadcast small dimension tables for efficient joins
        customer_360_base = (
            customers_df
            .join(F.broadcast(credit_df), "customer_key", "left")
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
                customers_df["customer_tenure_days"],
                # Transaction metrics
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
                F.coalesce(F.col("min_transaction_amount"), F.lit(0.0)).alias(
                    "min_transaction_amount"
                ),
                F.coalesce(F.col("transaction_amount_stddev"), F.lit(0.0)).alias(
                    "transaction_amount_stddev"
                ),
                F.col("first_transaction_date"),
                F.col("last_transaction_date"),
                F.coalesce(F.col("days_since_last_transaction"), F.lit(9999)).alias(
                    "days_since_last_transaction"
                ),
                F.col("favorite_merchant_category"),
                F.coalesce(F.col("unique_merchant_categories"), F.lit(0)).alias(
                    "unique_merchant_categories"
                ),
                F.coalesce(F.col("unique_merchants"), F.lit(0)).alias(
                    "unique_merchants"
                ),
                F.coalesce(F.col("online_transaction_pct"), F.lit(0.0)).alias(
                    "online_transaction_pct"
                ),
                F.coalesce(F.col("weekend_transaction_pct"), F.lit(0.0)).alias(
                    "weekend_transaction_pct"
                ),
                F.coalesce(F.col("fraud_count"), F.lit(0)).alias("fraud_count"),
                F.coalesce(F.col("fraud_amount"), F.lit(0.0)).alias("fraud_amount"),
                F.coalesce(F.col("high_value_transaction_count"), F.lit(0)).alias(
                    "high_value_transaction_count"
                ),
                # Credit metrics
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
        
        # Apply risk scoring with ML features
        customer_360_final = self.calculate_risk_score(customer_360_base).select(
            "*", F.current_timestamp().alias("last_updated")
        )
        
        # Partition for optimal write performance
        customer_360_partitioned = customer_360_final.repartition(10)
        
        # Cache final result before writing
        customer_360_partitioned.cache()
        
        logger.info(f"Writing {customer_360_partitioned.count()} customers to analytics layer")
        
        self.write_analytics_table(customer_360_partitioned, "customer_360")
        
        # Unpersist cached DataFrames to free memory
        customers_df.unpersist()
        credit_df.unpersist()
        transaction_metrics.unpersist()
        
        return customer_360_partitioned

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
