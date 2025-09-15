from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import logging
import random

logger = logging.getLogger(__name__)

default_args = {
    "owner": "IE405.E31.CN2 - Group 22",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "customer360_risk_pipeline",
    default_args=default_args,
    description="Customer 360 Risk Scoring Data Pipeline",
    schedule_interval="@daily",
    max_active_runs=1,
    tags=["customer360", "risk-scoring", "etl"],
)


def generate_synthetic_data(**context):
    import subprocess
    import os
    import glob

    scripts_dir = "/opt/airflow/scripts"
    data_dir = "/opt/airflow/data/raw"

    # Clean up existing CSV files before generating new ones
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    for csv_file in csv_files:
        try:
            os.remove(csv_file)
            logger.info(f"Removed old file: {csv_file}")
        except OSError as e:
            logger.warning(f"Could not remove {csv_file}: {e}")

    # Make the numbers random within a reasonable range
    num_customers = random.randint(800, 1200)
    num_transactions = random.randint(15, 25)

    cmd = [
        "python",
        os.path.join(scripts_dir, "generate_data.py"),
        "--customers",
        str(num_customers),
        "--transactions",
        str(num_transactions),
        "--output",
        data_dir,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"Data generation completed with {num_customers} customers and {num_transactions} transactions per customer")
    except subprocess.CalledProcessError as e:
        logger.error(f"Data generation failed: {e.stderr}")
        raise


def check_data_quality(**context):
    import pandas as pd
    import os

    data_dir = "/opt/airflow/data/raw"

    customers_file = os.path.join(data_dir, "customers.csv")
    if os.path.exists(customers_file):
        customers_df = pd.read_csv(customers_file)
        logger.info(f"Customers: {len(customers_df)} records")

        null_ids = customers_df["customer_id"].isnull().sum()
        if null_ids > 0:
            raise ValueError(f"Found {null_ids} null customer IDs")

        duplicate_ids = customers_df["customer_id"].duplicated().sum()
        if duplicate_ids > 0:
            raise ValueError(f"Found {duplicate_ids} duplicate customer IDs")
    else:
        raise FileNotFoundError("Customers file not found")

    transactions_file = os.path.join(data_dir, "transactions.csv")
    if os.path.exists(transactions_file):
        transactions_df = pd.read_csv(transactions_file)
        logger.info(f"Transactions: {len(transactions_df)} records")
    else:
        raise FileNotFoundError("Transactions file not found")

    credit_file = os.path.join(data_dir, "credit_scores.csv")
    if os.path.exists(credit_file):
        credit_df = pd.read_csv(credit_file)
        logger.info(f"Credit scores: {len(credit_df)} records")

        invalid_scores = (
            (credit_df["credit_score"] < 300) | (credit_df["credit_score"] > 850)
        ).sum()
        if invalid_scores > 0:
            raise ValueError(f"Found {invalid_scores} invalid credit scores")
    else:
        raise FileNotFoundError("Credit scores file not found")

    logger.info("Data quality checks passed")


def run_seatunnel_ingestion(config_file, **context):
    import pandas as pd
    from sqlalchemy import create_engine
    import os

    config_mapping = {
        "customers_ingestion.conf": {
            "csv_file": "/opt/airflow/data/raw/customers.csv",
            "table": "staging.customers",
        },
        "transactions_ingestion.conf": {
            "csv_file": "/opt/airflow/data/raw/transactions.csv",
            "table": "staging.transactions",
        },
        "credit_scores_ingestion.conf": {
            "csv_file": "/opt/airflow/data/raw/credit_scores.csv",
            "table": "staging.credit_scores",
        },
    }

    if config_file not in config_mapping:
        raise ValueError(f"Unknown config file: {config_file}")

    mapping = config_mapping[config_file]
    csv_file = mapping["csv_file"]
    table = mapping["table"]

    try:
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        df = pd.read_csv(csv_file)
        logger.info(f"Loaded {len(df)} records from {csv_file}")

        engine = create_engine(
            "postgresql+psycopg2://postgres:postgres@postgres:5432/customer360_dw"
        )

        df.to_sql(
            table.split(".")[1],
            engine,
            schema=table.split(".")[0],
            if_exists='append',
            index=False
        )

        logger.info(f"Successfully loaded {len(df)} records to {table}")

    except Exception as e:
        logger.error(f"Ingestion failed for {config_file}: {str(e)}")
        raise


def validate_pipeline_results(**context):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")

    tables_to_check = [
        ("staging.customers", "customer_id"),
        ("staging.transactions", "transaction_id"),
        ("staging.credit_scores", "customer_id"),
        ("warehouse.dim_customer", "customer_key"),
        ("warehouse.fact_transactions", "transaction_key"),
        ("warehouse.dim_credit", "credit_key"),
        ("analytics.customer_360", "customer_key"),
    ]

    for table, key_column in tables_to_check:
        count_query = f"SELECT COUNT(*) as record_count FROM {table}"
        result = postgres_hook.get_first(count_query)
        record_count = result[0] if result else 0
        logger.info(f"{table}: {record_count:,} records")

    risk_dist_query = """
        SELECT risk_category, COUNT(*) as count, 
               ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER() * 100, 1) as percentage
        FROM analytics.customer_360 
        GROUP BY risk_category 
        ORDER BY risk_category
    """

    risk_distribution = postgres_hook.get_records(risk_dist_query)
    logger.info("Risk distribution:")
    for row in risk_distribution:
        logger.info(f"  {row[0]}: {row[1]:,} customers ({row[2]}%)")


# Tasks
generate_data_task = PythonOperator(
    task_id="generate_synthetic_data", python_callable=generate_synthetic_data, dag=dag
)

data_quality_task = PythonOperator(
    task_id="check_data_quality", python_callable=check_data_quality, dag=dag
)

clear_staging_task = PostgresOperator(
    task_id="clear_staging_tables",
    postgres_conn_id="postgres_default",
    sql="""
        TRUNCATE TABLE staging.customers CASCADE;
        TRUNCATE TABLE staging.transactions CASCADE;
        TRUNCATE TABLE staging.credit_scores CASCADE;
    """,
    dag=dag,
)

ingest_customers_task = PythonOperator(
    task_id="ingest_customers",
    python_callable=run_seatunnel_ingestion,
    op_kwargs={"config_file": "customers_ingestion.conf"},
    dag=dag,
)

ingest_transactions_task = PythonOperator(
    task_id="ingest_transactions",
    python_callable=run_seatunnel_ingestion,
    op_kwargs={"config_file": "transactions_ingestion.conf"},
    dag=dag,
)

ingest_credit_task = PythonOperator(
    task_id="ingest_credit_scores",
    python_callable=run_seatunnel_ingestion,
    op_kwargs={"config_file": "credit_scores_ingestion.conf"},
    dag=dag,
)

warehouse_etl_task = SparkSubmitOperator(
    task_id="run_warehouse_etl",
    application="/opt/airflow/spark_jobs/warehouse_etl.py",
    conn_id="spark_default",
    conf={
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.executor.cores": "2",
    },
    packages="org.postgresql:postgresql:42.7.1",
    dag=dag,
)

risk_scoring_task = SparkSubmitOperator(
    task_id="run_risk_scoring",
    application="/opt/airflow/spark_jobs/risk_scoring_etl.py",
    conn_id="spark_default",
    conf={
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.executor.cores": "2",
    },
    packages="org.postgresql:postgresql:42.7.1",
    dag=dag,
)

validation_task = PythonOperator(
    task_id="validate_pipeline_results",
    python_callable=validate_pipeline_results,
    dag=dag,
)

update_lineage_task = PostgresOperator(
    task_id="update_data_lineage",
    postgres_conn_id="postgres_default",
    sql="""
        INSERT INTO analytics.pipeline_runs 
        (run_date, pipeline_name, status, records_processed, run_duration, created_at)
        VALUES 
        (CURRENT_DATE, 'customer360_risk_pipeline', 'SUCCESS', 
         (SELECT COUNT(*) FROM analytics.customer_360), 
         EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - '{{ ds }}'::timestamp)),
         CURRENT_TIMESTAMP)
        ON CONFLICT (run_date, pipeline_name) DO UPDATE SET
        status = EXCLUDED.status,
        records_processed = EXCLUDED.records_processed,
        run_duration = EXCLUDED.run_duration,
        created_at = EXCLUDED.created_at;
    """,
    dag=dag,
)

# Dependencies
generate_data_task >> data_quality_task >> clear_staging_task
clear_staging_task >> [
    ingest_customers_task,
    ingest_transactions_task,
    ingest_credit_task,
]
[
    ingest_customers_task,
    ingest_transactions_task,
    ingest_credit_task,
] >> warehouse_etl_task
warehouse_etl_task >> risk_scoring_task >> validation_task >> update_lineage_task
