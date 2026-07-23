from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from dbt_helpers import notify_telegram_on_failure

# Throwaway DAG to verify the on_failure_callback -> Telegram wiring.
# Trigger it manually; the single task always raises, firing the alert.
default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_telegram_on_failure,
}


def _always_fail():
    raise RuntimeError("Intentional failure to test the Telegram alert")


@dag(
    start_date=datetime(2026, 5, 29),
    schedule=None,          # manual trigger only
    catchup=False,
    default_args=default_args,
    tags=["test", "telegram", "alert"],
)
def test_telegram_alert_dbt_bash():
    PythonOperator(
        task_id="always_fail",
        python_callable=_always_fail,
    )


test_telegram_alert_dbt_bash()
