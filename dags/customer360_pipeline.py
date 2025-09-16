from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import random
import os
import pandas as pd


def generate_synthetic_data(**context):
    import subprocess

    scripts_dir = "/opt/airflow/scripts"
    data_dir = "/opt/airflow/data/raw"

    # Check if this is the first run (no existing customer data)
    customers_file = os.path.join(data_dir, "customers.csv")
    is_first_run = not os.path.exists(customers_file)

    if is_first_run:
        # First run: Generate initial customer base
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
    else:
        # Subsequent runs: Incremental simulation
        num_customers = random.randint(50, 200)  # Fewer new customers
        num_transactions = random.randint(
            10, 20
        )  # Additional transactions per customer
        cmd = [
            "python",
            os.path.join(scripts_dir, "generate_data.py"),
            "--customers",
            str(num_customers),
            "--transactions",
            str(num_transactions),
            "--output",
            data_dir,
            "--incremental",
        ]
    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Data generation failed: {e.stderr}")


def check_data_quality(**context):
    data_dir = "/opt/airflow/data/raw"
    customers_file = os.path.join(data_dir, "customers.csv")
    if os.path.exists(customers_file):
        customers_df = pd.read_csv(customers_file)
        unique_emails = customers_df["email"].nunique()
        total_customers = len(customers_df)
        if unique_emails != total_customers:
            raise ValueError(
                f"Email uniqueness violation: {total_customers} customers but only {unique_emails} unique emails"
            )
        null_emails = customers_df["email"].isnull().sum()
        null_customer_ids = customers_df["customer_id"].isnull().sum()
        duplicate_customer_ids = customers_df["customer_id"].duplicated().sum()
        if null_emails > 0:
            raise ValueError(f"Found {null_emails} customers with null emails")
        if null_customer_ids > 0:
            raise ValueError(
                f"Found {null_customer_ids} customers with null customer_ids"
            )
        if duplicate_customer_ids > 0:
            raise ValueError(
                f"Found {duplicate_customer_ids} customers with duplicate customer_ids"
            )
    else:
        raise FileNotFoundError(f"Customers file not found: {customers_file}")
    transactions_file = os.path.join(data_dir, "transactions.csv")
    if os.path.exists(transactions_file):
        transactions_df = pd.read_csv(transactions_file)
        customer_ids_in_customers = set(customers_df["customer_id"])
        customer_ids_in_transactions = set(transactions_df["customer_id"])
        orphaned_transactions = customer_ids_in_transactions - customer_ids_in_customers
        if orphaned_transactions:
            raise ValueError(
                f"Referential integrity violation: {len(orphaned_transactions)} transactions reference non-existent customers"
            )
        unique_transaction_ids = transactions_df["transaction_id"].nunique()
        total_transactions = len(transactions_df)
        if unique_transaction_ids != total_transactions:
            raise ValueError(
                f"Transaction ID uniqueness violation: {total_transactions} transactions but only {unique_transaction_ids} unique IDs"
            )
    else:
        raise FileNotFoundError(f"Transactions file not found: {transactions_file}")
    credit_file = os.path.join(data_dir, "credit_scores.csv")
    if os.path.exists(credit_file):
        credit_df = pd.read_csv(credit_file)
        unique_customers_in_credit = credit_df["customer_id"].nunique()
        total_credit_records = len(credit_df)
        if unique_customers_in_credit != total_credit_records:
            raise ValueError(
                f"Credit score business rule violation: {total_credit_records} records but only {unique_customers_in_credit} unique customers"
            )
        customer_ids_in_credit = set(credit_df["customer_id"])
        orphaned_credit_scores = customer_ids_in_credit - customer_ids_in_customers
        if orphaned_credit_scores:
            raise ValueError(
                f"Referential integrity violation: {len(orphaned_credit_scores)} credit scores reference non-existent customers"
            )
        invalid_scores = (
            (credit_df["credit_score"] < 300) | (credit_df["credit_score"] > 850)
        ).sum()
        if invalid_scores > 0:
            raise ValueError(
                f"Found {invalid_scores} credit scores outside valid range (300-850)"
            )
    else:
        raise FileNotFoundError(f"Credit scores file not found: {credit_file}")


def run_seatunnel_ingestion(config_file, **context):
    from sqlalchemy import create_engine

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
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")
    df = pd.read_csv(csv_file)
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres:5432/customer360_dw"
    )
    df.to_sql(
        table.split(".")[1],
        engine,
        schema=table.split(".")[0],
        if_exists="append",
        index=False,
    )


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
    risk_dist_query = """
        SELECT risk_category, COUNT(*) as count, 
               ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER() * 100, 1) as percentage
        FROM analytics.customer_360 
        GROUP BY risk_category 
        ORDER BY risk_category
    """
    postgres_hook.get_records(risk_dist_query)


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
generate_data_task = PythonOperator(
    task_id="generate_synthetic_data", python_callable=generate_synthetic_data, dag=dag
)
data_quality_task = PythonOperator(
    task_id="check_data_quality", python_callable=check_data_quality, dag=dag
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
(
    generate_data_task
    >> data_quality_task
    >> [
        ingest_customers_task,
        ingest_transactions_task,
        ingest_credit_task,
    ]
)
[
    ingest_customers_task,
    ingest_transactions_task,
    ingest_credit_task,
] >> warehouse_etl_task
warehouse_etl_task >> risk_scoring_task >> validation_task >> update_lineage_task
