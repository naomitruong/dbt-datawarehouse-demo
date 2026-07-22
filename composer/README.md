# Deploying this dbt project on Google Cloud Composer

Cloud Composer is GCP's managed Apache Airflow. Because it runs inside GCP, the
BigQuery auth story is trivial — no keyfile, no Workload Identity Federation, no
secrets. The Composer environment's own service account authenticates dbt.

## How it works

- **dbt is a PyPI package on the environment.** [`requirements.txt`](requirements.txt)
  installs `dbt-bigquery` into Composer, so `dbt` is on PATH inside every
  BashOperator — no venv needed.
- **Auth is native.** dbt's `prod` target uses `method: oauth`
  ([`../dbt/profiles.yml`](../dbt/profiles.yml)); Application Default Credentials
  resolve to the Composer environment's service account. Grant that SA BigQuery
  roles and you're done.
- **The dbt project lives in the bucket's `data/` folder** (mounted at
  `/home/airflow/gcs/data`), which keeps it out of the DAG parser's scan path.
- **Airflow Variable lookups are deferred to run time** via Jinja templating.

## Composer bucket layout

Composer auto-creates a GCS bucket (`gs://<region>-<env>-<hash>-bucket/`). It
syncs `dags/` → `/home/airflow/gcs/dags` and `data/` → `/home/airflow/gcs/data`:

```
gs://<composer-bucket>/
├── dags/
│   ├── dbt_helpers.py                                # shared helper
│   ├── prod_mart_ecopay_transaction_detail_dbt_bash.py
│   └── mart_ecopay_transaction_detail_dbt_bash.py
└── data/
    └── dbt/                                          # entire dbt project
        ├── dbt_project.yml
        ├── profiles.yml
        ├── models/ … macros/ … tests/ …
        └── (no venv/, no target/, no logs/)
```

The DAGs auto-detect the project at `/home/airflow/gcs/data/dbt` (Composer) or
`../dbt` (this repo), so no code change is needed between local and Composer.

## One-time setup

1. **Service account BigQuery access.** Find the environment's service account
   (Composer console → environment → *Configuration* → Service account), then:
   ```bash
   SA_EMAIL=<composer-env-service-account>
   gcloud projects add-iam-policy-binding "$PROJECT_ID" \
     --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.dataEditor"
   gcloud projects add-iam-policy-binding "$PROJECT_ID" \
     --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.jobUser"
   ```

2. **Install dbt** into the environment:
   ```bash
   gcloud composer environments update <ENV_NAME> \
     --location <REGION> \
     --update-pypi-packages-from-file composer/requirements.txt
   ```
   > This restarts/rebuilds the environment and can take 10–20 min. If it fails
   > on a dependency conflict with Composer's pinned Airflow, see *Gotchas*.

3. **Airflow Variable** (Airflow UI → Admin → Variables) — the BigQuery target:
   ```json
   {"project": "my-gcp-project", "dataset": "dwh", "location": "asia-southeast1"}
   ```
   stored under the key `profile_args_dwh_dbt`.

## Deploy

```bash
BUCKET=gs://<composer-bucket>

# DAGs + helper
gcloud storage rsync ./dags "$BUCKET/dags" -r -x ".*__pycache__.*"

# dbt project INTO data/dbt (exclude local-only artifacts)
gcloud storage rsync ./dbt "$BUCKET/data/dbt" -r \
  -x "venv/.*|target/.*|logs/.*|dbt_packages/.*|.*__pycache__.*"
```

`dbt deps` runs inside the prod DAG, so `dbt_packages/` is excluded and fetched
at run time. To vendor packages instead, drop that exclusion and remove
`dbt deps` from the DAG.

## Gotchas

- **PyPI conflict with Composer's Airflow.** Composer pins its Airflow
  dependencies; `dbt-bigquery` can clash (protobuf, google-cloud-*). If the
  `environments update` fails, either relax the pin, or switch to running dbt in
  a `KubernetesPodOperator` with a dedicated image (full isolation).
- **Env update is slow.** Changing PyPI packages rebuilds the environment
  (10–20 min) and briefly pauses scheduling. Batch package changes.
- **Local dev** runs on local Airflow instead — see
  [`../local/README.md`](../local/README.md). The `dev` target uses keyless
  gcloud ADC; nothing in this Composer setup changes.
