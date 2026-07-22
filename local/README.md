# Local Airflow (dev)

Day-to-day orchestration runs on a **local Airflow** (Docker Compose,
LocalExecutor + Postgres — no Celery/Redis, so it's light and free). It runs the
**same DAGs** that deploy to Cloud Composer; only the runtime differs:

| | Local Airflow (here) | Cloud Composer (prod) |
|---|---|---|
| dbt install | `_PIP_ADDITIONAL_REQUIREMENTS` | PyPI packages on the env |
| BigQuery auth | gcloud ADC, keyless | environment service account (ADC) |
| dbt target | `dev` (dev datasets) | `prod` (clean datasets) |
| Cost | free (your machine) | always-on GKE (\$\$\$) |

The switch is driven entirely by the `dbt_target` Airflow Variable — the DAG
code is identical. See [`../dags/dbt_helpers.py`](../dags/dbt_helpers.py).

## Prerequisites

- Docker + Docker Compose
- gcloud CLI, authenticated with access to the dev datasets (no key file needed)

## Run

```bash
# 1. One-time: create Application Default Credentials on the host
gcloud auth application-default login

# 2. Configure and start
cd local
cp .env.example .env
# edit .env: set GCP_PROJECT and AIRFLOW_UID (run `id -u`)

docker compose up -d
# first start builds the image + pip-installs dbt (a few minutes)
```

Open http://localhost:8080 (login `admin` / `admin`), un-pause a DAG, trigger it.

Stop / reset:
```bash
docker compose down          # stop
docker compose down -v        # stop + wipe the Airflow metadata DB
```

## How it wires up

- `../dags` → `/opt/airflow/dags`, `../dbt` → `/opt/airflow/dbt`. The DAGs'
  `resolve_dbt_project_dir()` finds the project via the `../dbt` fallback.
- Your host `~/.config/gcloud` is mounted read-only into the container;
  `GOOGLE_APPLICATION_CREDENTIALS` points at the ADC file inside it, and the
  `dev` target uses `method: oauth` — no service-account key involved.
- `AIRFLOW_VAR_DBT_TARGET=dev` and `AIRFLOW_VAR_PROFILE_ARGS_DWH_DBT=...` inject
  the Airflow Variables the DAGs read — no manual Admin → Variables step needed.

## Notes

- **`dev` vs `prod` datasets.** With `dbt_target=dev`, dbt writes to
  `dwh_staging` / `dwh_core` / `dwh_mart` (see `get_custom_schema.sql`), so local
  runs never touch the production datasets.
- **First run is slow.** `_PIP_ADDITIONAL_REQUIREMENTS` installs dbt on every
  container start. If that gets annoying, bake a small Dockerfile
  (`FROM apache/airflow:2.10.4` + `RUN pip install dbt-bigquery==1.12.0`) and set
  `image:`/`build:` instead — same as the Composer PyPI list.
- **Write permissions.** Set `AIRFLOW_UID` to your host UID (`id -u`) so dbt can
  write `target/` and `dbt_packages/` into the mounted `../dbt`.
- Deploying to prod later: see [`../composer/README.md`](../composer/README.md).
  Nothing here needs to change.
