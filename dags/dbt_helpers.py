import logging
import os
import urllib.parse
import urllib.request

# Composer mounts its GCS bucket's data/ folder here on every worker
_COMPOSER_DATA_DBT = "/home/airflow/gcs/data/dbt"

DBT_TARGET = "{{ var.value.get('dbt_target', 'prod') }}"

DBT_VARS = (
    "{"
    '"DBT_PROJECT":"{{ var.json.profile_args_dwh_dbt.project }}",'
    '"DBT_DATASET":"{{ var.json.profile_args_dwh_dbt.dataset }}",'
    '"DBT_LOCATION":"{{ var.json.profile_args_dwh_dbt.location }}"'
    "}"
)
def resolve_dbt_project_dir(dag_file: str) -> str:
    """Locate the dbt project across the Composer and local/repo layouts.

    Order: explicit ``DBT_PROJECT_DIR`` env → ``/home/airflow/gcs/data/dbt``
    (Composer) → ``<dags>/dbt`` → ``<dags>/../dbt`` (local docker mount / repo).
    """
    explicit = os.environ.get("DBT_PROJECT_DIR")
    if explicit:
        return explicit
    dag_dir = os.path.dirname(os.path.abspath(dag_file))
    for candidate in (
        _COMPOSER_DATA_DBT,                   # Composer: dbt/ synced to data/
        os.path.join(dag_dir, "dbt"),         # dbt/ alongside the DAGs
        os.path.join(dag_dir, "..", "dbt"),   # local docker mount / repo layout
    ):
        if os.path.isdir(candidate):
            return os.path.abspath(candidate)
    return _COMPOSER_DATA_DBT


def notify_telegram_on_failure(context):
    from airflow.models import Variable
    try:
        token = Variable.get("telegram_bot_token", default_var="")
        chat_id = Variable.get("telegram_chat_id", default_var="")
        if not token or not chat_id:
            logging.warning(
                "Telegram alert skipped: telegram_bot_token / telegram_chat_id not set"
            )
            return

        ti = context.get("task_instance")
        text = (
            "🔴 Airflow task FAILED\n"
            f"DAG:   {ti.dag_id}\n"
            f"Task:  {ti.task_id}\n"
            f"Run:   {context.get('run_id')}\n"
            f"Try:   {ti.try_number}\n"
            f"Error: {str(context.get('exception'))[:400]}\n"
            f"Log:   {ti.log_url}"
        )
        payload = urllib.parse.urlencode(
            {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}
        ).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=payload
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except Exception as exc:  # never let the alert break the task
        logging.warning("Telegram alert failed: %s", exc)
