import os
import json
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

# Standalone dbt project dir (sibling dbt/ folder), resolved relative to the file.
DBT_PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dbt"))
VENV_PATH = "/opt/airflow/dbt_venv/bin/activate"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

# Connection injected from an Airflow Variable (same pattern as fv_dwh ecopay DAGs).
profile_args_dbt = json.loads(Variable.get("profile_args_dwh_dbt", "{}"))

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    start_date=datetime(2026, 5, 29),
    max_active_runs=1,
    schedule=CronTriggerTimetable("0 6 * * *", timezone="Asia/Ho_Chi_Minh"),
    default_args=default_args,
    catchup=False,
    concurrency=10,
    tags=["ELT", "EL", "DBT", "mart", "ecopay"],
    params={
        "full_refresh": Param(
            default=False,
            type="boolean",
            title="Full Refresh",
            description="Run full refresh of the models",
        )
    },
)
def mart_ecopay_transaction_detail_dbt_bash():

    threads = 2

    # BigQuery connection injected via --vars (consumed by the `prod` target in profiles.yml).
    # The Airflow Variable `profile_args_dwh_dbt` holds: keyfile / project / dataset / location.
    var_string = (
        f'{{"DBT_KEYFILE":"{profile_args_dbt["keyfile"]}","DBT_PROJECT":"{profile_args_dbt["project"]}",'
        f'"DBT_DATASET":"{profile_args_dbt["dataset"]}","DBT_LOCATION":"{profile_args_dbt["location"]}"}}'
    )
    profiles_path = DBT_PROJECT_PATH

    bash_cmd_test = (
        f"source {VENV_PATH} && dbt test --project-dir {DBT_PROJECT_PATH} "
        f"--select +models/mart/ecopay/ --vars '{var_string}' "
        f"--profiles-dir {profiles_path} --target prod"
    )
    dbt_run_mart_test = BashOperator(
        task_id="dbt_run_mart_test_ecopay",
        bash_command=bash_cmd_test,
    )

    bash_cmd_run = (
        f"source {VENV_PATH} && dbt run --project-dir {DBT_PROJECT_PATH} "
        f"--select +models/mart/ecopay/ --vars '{var_string}' "
        f"--profiles-dir {profiles_path} --target prod --threads {threads}"
        "{{ ' --full-refresh' if params.full_refresh else '' }}"
    )
    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart_ecopay",
        bash_command=bash_cmd_run,
    )

    dbt_run_mart_test
    dbt_run_mart


mart_ecopay_transaction_detail_dbt_bash()
