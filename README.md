# dbt Data Warehouse Demo — Ecopay Transaction Detail (BigQuery)

An end-to-end analytics-engineering demo that ingests Debezium/Mongo-style CDC documents and
transforms them through a layered **source → staging → core → mart** pipeline on **Google BigQuery**,
orchestrated with **Apache Airflow** and modeled with **dbt Core**.

The flagship deliverable is the `dwh_ecopay_transaction_detail` mart: per-transaction GMV and service
fees enriched with the merchant/store and the salesman's management hierarchy.

---

## Tech stack

| Layer | Choice | Notes |
|---|---|---|
| **Transformation** | dbt Core (`dbt-bigquery`) | Layered models, custom materialization, macros |
| **Cloud data warehouse** | Google BigQuery | Datasets per layer; `JSON` landing column |
| **Ingestion (landing)** | Debezium/Mongo CDC → `json_raw.*` | Simulated locally by a seed script |
| **Orchestration** | Apache Airflow | Timezone-aware DAGs, connection via Airflow Variable |
| **BI / Analytics** | Connect directly to the `mart` dataset | e.g. Looker Studio / Metabase on `dwh_ecopay_transaction_detail` |

---

## Architecture

Layers are organized `source → staging → core → mart`, grouped by source system, **one BigQuery
dataset per layer**:

```
json_raw (landing, JSON)
      │
      ▼
staging/   stg_*   incremental append, JSON parsed with extract_json* macros
      │
      ▼
core/      dim_*   materialized_deduplicate (latest row per id by ts_ms, via MERGE + QUALIFY)
      │
      ▼
mart/      dwh_* / dm_*   incremental merge
```

### Lineage of the mart

```
json_raw.ecopay_ecopay_transactions → stg_ecopay_transactions → dim_ecopay_transactions ─┐
json_raw.ecopay_ecopay_stores       → stg_ecopay_stores       → dim_ecopay_stores       ─┼─► dwh_ecopay_transaction_detail
json_raw.dms1_users → stg_dms1_users → dim_dms1_users → dm_dms1_user_manager_phones ─────┘
```

`dm_dms1_user_manager_phones` resolves each user's full manager phone chain with a **recursive CTE**;
the mart joins it to the ecopay store's `sale_info.phone` to attach salesman/agent names and phones.

---

## Repository layout

```
.
├── dbt/                                  # dbt Core project  (see dbt/README.md for details)
│   ├── profiles.yml                      # BigQuery (method: oauth); dev via gcloud ADC + env_var, prod via --vars
│   ├── dbt_project.yml                   # layer → dataset mapping (staging/core/mart)
│   ├── packages.yml                      # dbt_utils, dbt_expectations (+ dbt_date transitive)
│   ├── macros/
│   │   ├── extract_json_data.sql         # BigQuery JSON extraction (JSON_VALUE / JSON_QUERY)
│   │   ├── materialized_deduplicate.sql  # custom materialization (MERGE + QUALIFY dedup)
│   │   └── get_custom_schema.sql         # dataset resolution per target (prod vs dev)
│   ├── models/
│   │   ├── staging/{ecopay,dms1}/        # stg_*  (incremental append)
│   │   ├── core/{ecopay,dms1}/           # dim_*  (materialized_deduplicate)
│   │   └── mart/{ecopay,dms1}/           # dwh_* / dm_*  (incremental)
│   ├── tests/data_test/ecopay/           # singular data tests (transformation invariants)
│   └── seeds_local/
│       └── 00_json_raw_seed_bigquery.sql # one-off bootstrap of the json_raw landing dataset
├── dags/                                 # Airflow DAGs (dev + prod) for the ecopay mart
├── required.txt                          # assignment requirements (stack constraints)
└── README.md                             # you are here
```

---

## Prerequisites

- A **GCP project** with the BigQuery API enabled.
- The **gcloud CLI**, authenticated for local dev with Application Default
  Credentials: `gcloud auth application-default login` — **no key file needed**.
  Your account needs BigQuery Data Editor + Job User on the project.
- Python 3.12 and `dbt-bigquery` (`pip install dbt-bigquery`).
- (Optional) `bq` CLI from the Google Cloud SDK to run the seed script.

---

## Setup & run (local `dev` target)

```bash
cd dbt

# 1. install adapter + dbt packages
pip install dbt-bigquery
dbt deps

# 2. authenticate (keyless, via gcloud ADC) + point dbt at your project
gcloud auth application-default login
export DBT_PROJECT=your-gcp-project    # the dev target reads these env_var fallbacks
export DBT_DATASET=dwh
export DBT_LOCATION=asia-southeast1

# 3. bootstrap the json_raw landing dataset (stands in for the Debezium ingest)
bq query --use_legacy_sql=false --project_id="$DBT_PROJECT" --location="$DBT_LOCATION" \
  < seeds_local/00_json_raw_seed_bigquery.sql

# 4. build the whole lineage → datasets dwh_staging / dwh_core / dwh_mart
dbt build --profiles-dir .
```

> To run the same pipeline through a **local Airflow** (Docker) instead of the
> dbt CLI, see [`local/README.md`](local/README.md) — same keyless ADC auth.

### Connection configuration

| Target | Runs where | How the 4 vars are supplied |
|---|---|---|
| `dev` | your machine / CLI | keyless gcloud ADC + `export DBT_PROJECT / DBT_DATASET / DBT_LOCATION` (or the defaults in `profiles.yml`) |
| `prod` | Airflow | Airflow Variable `profile_args_dwh_dbt` (JSON) → injected by the DAG as `--vars` |

On BigQuery a dbt "schema" is a **dataset**. `macros/get_custom_schema.sql` resolves them per target:
`prod` → clean names (`staging` / `core` / `mart`); `dev` → `{dataset}_{layer}` (e.g. `dwh_staging`).

---

## Data quality tests

Run with `dbt test` (included in `dbt build`).

- **Schema tests** (in `*.yml`): `not_null` / `unique` on `dwh_ecopay_transaction_detail.transid`,
  `not_null` on `gmv` and the load timestamps, `dbt_utils.expression_is_true` on `commission = 0`;
  `not_null` / `unique` on `dm_dms1_user_manager_phones.user_phone`; `not_null` + `accepted_values`
  on the CDC `op` column of the ecopay staging model.
- **Singular data tests** (`dbt/tests/data_test/ecopay/`): enforce transformation invariants —
  one merchant per store, and transactions must carry the store's `merchant_code`.

---

## Orchestration (Airflow)

DAGs in [`dags/`](dags/) build the mart plus everything upstream via the `+models/mart/ecopay/` selector:

- `mart_ecopay_transaction_detail_dbt_bash.py` — dev: `dbt test` + `dbt run`, `0 6` Asia/Ho_Chi_Minh.
- `prod_mart_ecopay_transaction_detail_dbt_bash.py` — prod: `dbt deps && dbt run`, `0 5` Asia/Ho_Chi_Minh.

Both inject the BigQuery connection from the Airflow Variable `profile_args_dwh_dbt` (JSON with
`keyfile` / `project` / `dataset` / `location`) as `--vars` — no secrets in the repo. A `full_refresh`
DAG param toggles `--full-refresh`.

---

## Mapping to `required.txt`

| Requirement | How it is met |
|---|---|
| Transformation = **dbt Core or Cloud** | dbt Core with the `dbt-bigquery` adapter |
| **Cloud data warehouse** | Google BigQuery |
| **BI tool connected directly** to warehouse models | Point Looker Studio / Metabase at the `mart` dataset (`dwh_ecopay_transaction_detail`) |
| ≥ 1 **incremental** model | `dwh_ecopay_transaction_detail`, `dm_dms1_user_manager_phones` (incremental merge) + all `stg_*` (incremental append) |
| ≥ 1 **dbt test** | Schema tests in `*.yml` + 2 singular data tests |
| Data ingestion working | `json_raw` landing tables (Debezium/Mongo shape), seeded locally by the bootstrap script |

> **CI/CD & release process:** not yet wired up in this repo. Recommended next step: a GitHub Actions
> workflow running `dbt build --target ci` against a scratch dataset on pull requests, with a
> branch-per-feature → main promotion flow.
