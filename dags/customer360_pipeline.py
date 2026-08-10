import os
import random
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount


def generate_synthetic_data(**context):
    import subprocess

    scripts_dir = os.environ.get("AIRFLOW_SCRIPTS_DIR", "/opt/airflow/scripts")
    data_dir = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data/raw")

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
            "--customers",
            str(num_customers),
            "--transactions",
            str(num_transactions),
            "--output",
            data_dir,
        ]
    else:
        # Subsequent runs: Incremental simulation
        num_customers = random.randint(500, 1000)
        num_transactions = random.randint(10, 20)
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
    description="Customer 360 ELT Pipeline (SeaTunnel + dbt)",
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

    # 2. Extract and Load (EL) using Apache SeaTunnel via Docker
    with TaskGroup("extract_and_load") as el_group:
        seatunnel_customers_task = DockerOperator(
            task_id="seatunnel_ingest_customers",
            image="apache/seatunnel:2.3.13",
            api_version="auto",
            auto_remove="force",
            command="seatunnel.sh --config /opt/airflow/seatunnel/jobs/customers_ingestion.conf -e local",
            docker_url="unix://var/run/docker.sock",
            network_mode="customer360-network",
            mounts=[
                Mount(
                    source="/opt/airflow/seatunnel",
                    target="/opt/airflow/seatunnel",
                    type="bind",
                ),
                Mount(
                    source="/opt/airflow/data", target="/opt/airflow/data", type="bind"
                ),
            ],
            mount_tmp_dir=False,
        )

        seatunnel_transactions_task = DockerOperator(
            task_id="seatunnel_ingest_transactions",
            image="apache/seatunnel:2.3.13",
            api_version="auto",
            auto_remove="force",
            command="seatunnel.sh --config /opt/airflow/seatunnel/jobs/transactions_ingestion.conf -e local",
            docker_url="unix://var/run/docker.sock",
            network_mode="customer360-network",
            mounts=[
                Mount(
                    source="/opt/airflow/seatunnel",
                    target="/opt/airflow/seatunnel",
                    type="bind",
                ),
                Mount(
                    source="/opt/airflow/data", target="/opt/airflow/data", type="bind"
                ),
            ],
            mount_tmp_dir=False,
        )

        seatunnel_credit_scores_task = DockerOperator(
            task_id="seatunnel_ingest_credit_scores",
            image="apache/seatunnel:2.3.13",
            api_version="auto",
            auto_remove="force",
            command="seatunnel.sh --config /opt/airflow/seatunnel/jobs/credit_scores_ingestion.conf -e local",
            docker_url="unix://var/run/docker.sock",
            network_mode="customer360-network",
            mounts=[
                Mount(
                    source="/opt/airflow/seatunnel",
                    target="/opt/airflow/seatunnel",
                    type="bind",
                ),
                Mount(
                    source="/opt/airflow/data", target="/opt/airflow/data", type="bind"
                ),
            ],
            mount_tmp_dir=False,
        )

    # 3. Transform (T) and Testing using dbt via Docker
    with TaskGroup("transform_and_test") as transform_group:
        dbt_run_task = DockerOperator(
            task_id="dbt_run",
            image="ghcr.io/dbt-labs/dbt-postgres:1.7.latest",
            api_version="auto",
            auto_remove="force",
            command="run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt",
            docker_url="unix://var/run/docker.sock",
            network_mode="customer360-network",
            mounts=[
                Mount(source="/opt/airflow/dbt", target="/usr/app/dbt", type="bind")
            ],
            mount_tmp_dir=False,
        )
        dbt_test_task = DockerOperator(
            task_id="dbt_test",
            image="ghcr.io/dbt-labs/dbt-postgres:1.7.latest",
            api_version="auto",
            auto_remove="force",
            command="test --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt",
            docker_url="unix://var/run/docker.sock",
            network_mode="customer360-network",
            mounts=[
                Mount(source="/opt/airflow/dbt", target="/usr/app/dbt", type="bind")
            ],
            mount_tmp_dir=False,
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
