from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import TestBehavior
from datetime import datetime, timedelta
import os
import sys

# Ajout du chemin pour trouver ton script upload
sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'include', 'scripts'))

# import de la fonction main du script upload_to_s3
from upload_to_s3 import main as run_upload_s3

# --- CONFIGURATIONS ---
DBT_PROJECT_PATH = "/usr/local/airflow/dags/olist_dbt_project" 
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt" 

# Config  Cosmos pour snowflake
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_access",
        profile_args={"database": "OLIST_DB", "schema": "RAW"},
    )
)

# --- DEFINITION DU DAG ---
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="olist_ingestion_pipeline",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
) as dag:

    # Arrivée des données : Upload vers S3
    task_upload_s3 = PythonOperator(
        task_id="upload_batch_to_s3",
        python_callable=run_upload_s3
    )

    # On attend que Snowpipe avale les données 
    task_wait_for_snowpipe = TimeDeltaSensor(
        task_id="wait_for_snowpipe",
        delta=timedelta(seconds=30),
        mode="reschedule"
    )

    # Lancer dbt via Cosmos
    task_dbt_transformation = DbtTaskGroup(
        group_id="dbt_processing",           
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_ALL,
        ), 
        operator_args={
            "install_deps": True,
        }
    )

    # --- ORDONNANCEMENT ---
    task_upload_s3 >> task_wait_for_snowpipe >> task_dbt_transformation