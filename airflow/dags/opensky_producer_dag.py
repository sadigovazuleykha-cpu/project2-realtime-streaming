"""
Airflow DAG — Step 4.

Runs the OpenSky producer (Step 2) on a schedule instead of by hand.
Every 2 minutes keeps us comfortably inside OpenSky's daily credit budget
(see the NOTE in opensky_producer.py) while still giving the dashboards
fresh-ish data.

Nothing here needs KAFKA_BOOTSTRAP_SERVERS or OPENSKY_CREDENTIALS_FILE set —
the producer script's own defaults (kafka:29092, credentials.json next to
the script) already match this container's setup.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "matrix-data-eng",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="opensky_producer",
    default_args=default_args,
    description="Poll OpenSky and publish aircraft state to Kafka",
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["project2", "producer"],
) as dag:

    run_producer = BashOperator(
        task_id="run_opensky_producer",
        bash_command="python3 /opt/airflow/producer/opensky_producer.py",
    )