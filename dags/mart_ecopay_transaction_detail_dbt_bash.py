from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

from dbt_helpers import DBT_TARGET, DBT_VARS, resolve_dbt_project_dir

# dbt project dir, resolved for both the Composer (data/dbt) and local layouts.
DBT_PROJECT_PATH = resolve_dbt_project_dir(__file__)

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

    # BigQuery connection injected via --vars (consumed by the `prod` target in
    # profiles.yml). Target is `prod` on Composer, `dev` on local Airflow (via
    # the `dbt_target` Variable). Auth: Composer SA (ADC) or a mounted keyfile.
    bash_cmd_test = (
        f"dbt test --project-dir {DBT_PROJECT_PATH} "
        f"--select +models/mart/ecopay/ --vars '{DBT_VARS}' "
        f"--profiles-dir {DBT_PROJECT_PATH} --target {DBT_TARGET}"
    )
    dbt_run_mart_test = BashOperator(
        task_id="dbt_run_mart_test_ecopay",
        bash_command=bash_cmd_test,
    )

    bash_cmd_run = (
        f"dbt run --project-dir {DBT_PROJECT_PATH} "
        f"--select +models/mart/ecopay/ --vars '{DBT_VARS}' "
        f"--profiles-dir {DBT_PROJECT_PATH} --target {DBT_TARGET} --threads {threads}"
        "{{ ' --full-refresh' if params.full_refresh else '' }}"
    )
    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart_ecopay",
        bash_command=bash_cmd_run,
    )

    # Preserved from the original DAG: the two tasks are independent (no >>).
    dbt_run_mart_test
    dbt_run_mart


mart_ecopay_transaction_detail_dbt_bash()
