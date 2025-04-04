import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)
dbt_path = "/usr/local/airflow/dags/dbt/data_pipeline"
print(f"Checking path: {dbt_path}")
print(f"Directory exists: {os.path.exists(dbt_path)}")
print(f"dbt_project.yml exists: {os.path.exists(os.path.join(dbt_path, 'dbt_project.yml'))}")

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(dbt_path,),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval="@daily",
    start_date=datetime(2025, 4, 4),
    catchup=False,
    dag_id="dbt_dag",
)

