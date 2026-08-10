import os
import random
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup


def generate_synthetic_data(**context):
    import subprocess

    scripts_dir = os.environ.get('AIRFLOW_SCRIPTS_DIR', '/opt/airflow/scripts')
    data_dir = os.environ.get('AIRFLOW_DATA_DIR', '/opt/airflow/data/raw')

    # Check if this is the first run (no existing customer data)
    customers_file = os.path.join(data_dir, "customers.csv")
    is_first_run = not os.path.exists(customers_file)

    if is_first_run:
        # First run: Generate a substantial initial customer base
        num_customers = random.randint(10000, 15000)
        num_transactions = random.randint(15, 25)
        cmd = [
            "python",
            os.path.join(scripts_dir, "generate_data.py"),
            "--customers", str(num_customers),
            "--transactions", str(num_transactions),
            "--output", data_dir,
        ]
    else:
        # Subsequent runs: Incremental simulation
        num_customers = random.randint(500, 1000)
        num_transactions = random.randint(10, 20)
        cmd = [
            "python",
            os.path.join(scripts_dir, "generate_data.py"),
            "--customers", str(num_customers),
            "--transactions", str(num_transactions),
            "--output", data_dir,
            "--incremental",
        ]
    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Data generation failed: {e.stderr}")

default_args = {
    "owner": "Data Engineering Team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tzinfo=UTC),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

with DAG(
    "customer360_risk_pipeline",
    default_args=default_args,
    description="Customer 360 ELT Pipeline (Spark + dbt)",
    schedule_interval="@daily",
    max_active_runs=1,
    tags=["customer360", "risk-scoring", "elt", "dbt"],
) as dag:

    # 1. Data Generation (Simulating incoming data)
    with TaskGroup("data_generation") as data_gen_group:
        generate_data_task = PythonOperator(
            task_id="generate_synthetic_data",
            python_callable=generate_synthetic_data,
        )

    # 2. Extract and Load (EL) using PySpark
    with TaskGroup("extract_and_load") as el_group:
        spark_ingestion_task = SparkSubmitOperator(
            task_id="spark_data_ingestion",
            application="/opt/airflow/spark_jobs/ingestion_etl.py",
            conn_id="spark_default",
            conf={
                "spark.executor.memory": "2g",
                "spark.driver.memory": "1g",
                "spark.executor.cores": "2",
                "spark.sql.adaptive.enabled": "true",
            },
            packages="org.postgresql:postgresql:42.7.1",
            application_args=["/opt/airflow/data/raw"],
        )

    # 3. Transform (T) and Testing using dbt
    with TaskGroup("transform_and_test") as transform_group:
        dbt_run_task = BashOperator(
            task_id="dbt_run",
            bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
        )
        
        dbt_test_task = BashOperator(
            task_id="dbt_test",
            bash_command="dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
        )
        
        dbt_run_task >> dbt_test_task

    # 4. Metadata updates
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
    )

    # Set dependencies
    data_gen_group >> el_group >> transform_group >> update_lineage_task
