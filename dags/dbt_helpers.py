"""Shared helpers for running the dbt project from Airflow.

The DAGs run in two environments off the *same* code:

- **Local Airflow** (docker-compose, day-to-day dev) — dbt is installed in the
  Airflow image, BigQuery auth uses a mounted keyfile, and the Airflow Variable
  ``dbt_target`` is set to ``dev`` so runs land in the dev datasets.
- **Cloud Composer** (prod, when needed) — dbt is a PyPI package on the
  environment, auth is the environment's service account (ADC), and
  ``dbt_target`` is unset so it defaults to ``prod``.

Design notes:

- The dbt project is located at run time across layouts: Composer's
  ``/home/airflow/gcs/data/dbt`` and the repo/local ``../dbt``.
- All Airflow Variable lookups are deferred to *task run time* via Jinja
  templating — never ``Variable.get`` at DAG-parse time — so parsing stays cheap.
"""

import os

# Composer mounts its GCS bucket's data/ folder here on every worker.
_COMPOSER_DATA_DBT = "/home/airflow/gcs/data/dbt"

# dbt --target. Defaults to `prod` (Cloud Composer). Local Airflow sets the
# Airflow Variable `dbt_target` to `dev` (see local/docker-compose.yaml) so it
# writes to the dev datasets instead of production.
DBT_TARGET = "{{ var.value.get('dbt_target', 'prod') }}"

# --vars payload consumed by the `prod` target in dbt/profiles.yml. project /
# dataset / location come from the `profile_args_dwh_dbt` Airflow Variable. (The
# `dev` target reads DBT_* env vars instead, so these vars are simply ignored
# when running with --target dev.)
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
