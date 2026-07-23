# dbt Data Warehouse Demo: Ecopay Transaction Detail (BigQuery)

A small warehouse built on BigQuery that takes Debezium/Mongo style CDC documents and pushes them
through four layers (source, staging, core, mart) with dbt Core, scheduled by Airflow.

The model everything else exists for is `dwh_ecopay_transaction_detail`: one row per transaction with
GMV and service fees, joined to the merchant/store and to the salesman's management chain.
`rpt_ecopay_transaction_detail` sits on top of it as a thin view for BI.

## Tech stack

| Layer | Choice | Notes |
|---|---|---|
| Transformation | dbt Core (`dbt-bigquery`) | Layered models, a custom materialization, JSON macros |
| Warehouse | Google BigQuery | One dataset per layer, `JSON` landing column |
| Ingestion (landing) | Debezium/Mongo CDC into `json_raw.*` | Simulated locally by a seed script |
| Orchestration | Apache Airflow | Timezone aware DAGs, connection from an Airflow Variable |
| CI | GitHub Actions | SQLFluff, `dbt parse`, keyless auth to BigQuery via WIF |
| BI | Reads the `mart` dataset directly | Looker Studio on `rpt_ecopay_transaction_detail` |

## Architecture

Models are grouped by layer first, then by source system, and each layer maps to its own BigQuery
dataset:

![Incremental data flow from the json_raw CDC source through the staging, core and mart datasets into Looker Studio](DWH%20Incremental%20Data%20Flow-architecture.png)

What each layer does:

```
json_raw (landing, JSON)
      |
      v
staging/   stg_*   incremental append, JSON parsed with the extract_json* macros
      |
      v
core/      dim_*   materialized_deduplicate (latest row per id by ts_ms, MERGE + QUALIFY)
      |
      v
mart/      dwh_* / dm_*   incremental merge, plus rpt_* views for BI
```

Lineage of the mart:

```
json_raw.ecopay_ecopay_transactions -> stg_ecopay_transactions -> dim_ecopay_transactions -+
json_raw.ecopay_ecopay_stores       -> stg_ecopay_stores       -> dim_ecopay_stores       -+-> dwh_ecopay_transaction_detail -> rpt_ecopay_transaction_detail
json_raw.dms1_users -> stg_dms1_users -> dim_dms1_users -> dm_dms1_user_manager_phones ----+
```

`dm_dms1_user_manager_phones` walks each user's manager chain with a recursive CTE. The mart joins it
on the ecopay store's `sale_info.phone` to attach salesman and agent names and phones.

## Repository layout

```
.
├── dbt/                                  # dbt Core project (details in dbt/README.md)
│   ├── profiles.yml                      # BigQuery, method: oauth. dev = gcloud ADC + env_var, prod = --vars
│   ├── dbt_project.yml                   # layer to dataset mapping
│   ├── packages.yml                      # dbt_utils, dbt_expectations (dbt_date comes along)
│   ├── macros/
│   │   ├── extract_json_data.sql         # JSON_VALUE / JSON_QUERY helpers
│   │   ├── materialized_deduplicate.sql  # custom materialization, MERGE + QUALIFY dedup
│   │   └── get_custom_schema.sql         # dataset naming per target
│   ├── models/
│   │   ├── staging/{ecopay,dms1}/        # stg_*, incremental append
│   │   ├── core/{ecopay,dms1}/           # dim_*, materialized_deduplicate
│   │   └── mart/{ecopay,dms1}/           # dwh_* / dm_* incremental, rpt_* views
│   ├── tests/data_test/ecopay/           # singular tests for transformation invariants
│   └── seeds_local/
│       └── 00_json_raw_seed_bigquery.sql # one off bootstrap of the json_raw landing dataset
├── dags/                                 # Airflow DAGs (dev + prod) and shared helpers
├── local/                                # Docker Compose Airflow for local runs
├── .github/workflows/                    # CI: lint and BigQuery connectivity check
├── required.txt                          # assignment requirements
└── README.md
```

## Prerequisites

- A GCP project with the BigQuery API enabled.
- The gcloud CLI, authenticated locally with `gcloud auth application-default login`. No key file is
  needed. Your account needs BigQuery Data Editor and Job User on the project.
- Python 3.12 and `dbt-bigquery` (`pip install dbt-bigquery`).
- Optional: the `bq` CLI to run the seed script.

## Setup and run (local `dev` target)

```bash
cd dbt

# 1. install the adapter and dbt packages
pip install dbt-bigquery
dbt deps

# 2. authenticate (keyless, gcloud ADC) and point dbt at your project
gcloud auth application-default login
export DBT_PROJECT=your-gcp-project    # the dev target falls back to these env vars
export DBT_DATASET=dwh
export DBT_LOCATION=asia-southeast1

# 3. bootstrap the json_raw landing dataset (stands in for the Debezium ingest)
bq query --use_legacy_sql=false --project_id="$DBT_PROJECT" --location="$DBT_LOCATION" \
  < seeds_local/00_json_raw_seed_bigquery.sql

# 4. build everything into dwh_staging / dwh_core / dwh_mart
dbt build --profiles-dir .
```

To run the same pipeline through a local Airflow in Docker instead of the dbt CLI, see
[`local/README.md`](local/README.md). Same keyless ADC auth.

### Connection configuration

The `dev` target runs from your machine and reads `DBT_PROJECT`, `DBT_DATASET` and `DBT_LOCATION`
from the environment (or the defaults in `profiles.yml`), with gcloud ADC for auth. The `prod` target
runs under Airflow and gets the same values from the Airflow Variable `profile_args_dwh_dbt` (a JSON
blob), which the DAG passes down as `--vars`.

A dbt "schema" is a BigQuery dataset here. `macros/get_custom_schema.sql` names them per target:
`prod` gets the clean names (`staging`, `core`, `mart`), `dev` gets `{dataset}_{layer}`, for example
`dwh_staging`.

## Data quality tests

Run with `dbt test`, or as part of `dbt build`.

Schema tests live next to the models in `*.yml`: `not_null` and `unique` on
`dwh_ecopay_transaction_detail.transid`, `not_null` on `gmv` and the load timestamps,
`dbt_utils.expression_is_true` for `commission = 0`, `not_null` and `unique` on
`dm_dms1_user_manager_phones.user_phone`, and `not_null` plus `accepted_values` on the CDC `op`
column in ecopay staging.

Two singular tests in `dbt/tests/data_test/ecopay/` cover invariants the schema tests cannot express:
a store belongs to exactly one merchant, and every transaction carries its store's `merchant_code`.

## Orchestration (Airflow)

The DAGs in [`dags/`](dags/) build the mart and everything upstream of it through the
`+models/mart/ecopay/` selector:

- `mart_ecopay_transaction_detail_dbt_bash.py`, dev: `dbt test` then `dbt run`, daily at 06:00
  Asia/Ho_Chi_Minh.
- `prod_mart_ecopay_transaction_detail_dbt_bash.py`, prod: `dbt deps && dbt run`, daily at 05:00
  Asia/Ho_Chi_Minh.

Both read the BigQuery connection from the Airflow Variable `profile_args_dwh_dbt` and pass it as
`--vars`, so no secrets sit in the repo. A `full_refresh` DAG param toggles `--full-refresh`, and
failures go to Telegram through `notify_telegram_on_failure` in `dags/dbt_helpers.py`.

## CI/CD

Two GitHub Actions workflows run on pull requests to `main` and on push:

- `ci-lint.yml`: SQLFluff over the project, `dbt deps` and `dbt parse` to catch broken refs and Jinja,
  and `py_compile` on the DAG files.
- `ci-dbt.yml`: authenticates to GCP with Workload Identity Federation (no service account keys),
  then checks BigQuery connectivity and runs `dbt deps` / `dbt parse`.

Next step would be a `ci` target building into a scratch dataset on each pull request, so schema and
data tests run against real data before merge.

## Mapping to `required.txt`

| Requirement | How it is met |
|---|---|
| Transformation with dbt Core or Cloud | dbt Core with the `dbt-bigquery` adapter |
| Cloud data warehouse | Google BigQuery |
| BI tool connected directly to warehouse models | Looker Studio on `rpt_ecopay_transaction_detail` in the `mart` dataset |
| At least one incremental model | `dwh_ecopay_transaction_detail` and `dm_dms1_user_manager_phones` (incremental merge), plus every `stg_*` (incremental append) |
| At least one dbt test | Schema tests in `*.yml` plus 2 singular data tests |
| Working data ingestion | `json_raw` landing tables in Debezium/Mongo shape, seeded locally by the bootstrap script |
| CI/CD and release process | GitHub Actions: lint, `dbt parse`, keyless WIF auth to BigQuery |
