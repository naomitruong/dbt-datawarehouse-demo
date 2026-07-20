# dwh_ecopay_transaction_detail (standalone)

Standalone dbt project that reproduces the `dwh_ecopay_transaction_detail` mart flow using the
**same architecture and conventions as the main `fv_dwh` project** (`dags/src/dbt/fv_dwh`).

> **Warehouse: BigQuery.** This project targets Google BigQuery (`dbt-bigquery` adapter). A dbt
> "schema" is a BigQuery **dataset**; the landing `data` column is the BigQuery **JSON** type, and
> JSON is parsed with the `extract_json*` macros (`JSON_VALUE` / `JSON_QUERY`). The custom
> `materialized_deduplicate` materialization uses BigQuery `MERGE` + `QUALIFY` (no temp tables /
> indexes / grants).

## Architecture (mirrors fv_dwh)

Layers are organized `source -> staging -> core -> mart`, grouped by source system, one schema per layer:

```
models/
  staging/                 (+schema: staging)   incremental append, JSON parsed with extract_json_* macros
    _sources.yml           json_raw Debezium/Mongo landing tables
    ecopay/
      stg_ecopay_transactions.sql
      stg_ecopay_stores.sql
    dms1/
      stg_dms1_users.sql
  core/                    (+schema: core)      materialized_deduplicate (latest row per id by ts_ms)
    ecopay/
      dim_ecopay_transactions.sql
      dim_ecopay_stores.sql
    dms1/
      dim_dms1_users.sql
  mart/                    (+schema: mart)       incremental
    dms1/
      dm_dms1_user_manager_phones.sql            recursive manager hierarchy per phone
      dm_dms1_user_manager_phones.yml            not_null + unique on user_phone
    ecopay/
      dwh_ecopay_transaction_detail.sql          the target mart
      dwh_ecopay_transaction_detail.yml
tests/
  data_test/ecopay/                              singular data tests (transformation invariants)
    test_dim_ecopay_transactions_merchant_matches_store.sql
    test_dim_ecopay_stores_one_merchant_per_store.sql
```

## Data quality / transformation tests (copied from fv_dwh)

- **Schema tests** — staging: `not_null` on id/ts_ms/json_inserted_at/inserted_at/op + `accepted_values`
  on the CDC `op` column; mart: `not_null`/`unique` on `transid`, `not_null` on gmv/fees, and
  `dbt_utils.expression_is_true` on `commission = 0`; `dm_dms1_user_manager_phones`: `not_null`/`unique`
  on `user_phone`.
- **Singular data tests** — enforce transformation invariants across the ecopay lineage: one merchant per
  store, and transactions must carry the store's merchant_code.

Naming follows fv_dwh: `stg_*` (staging), `dim_*` (core), `dwh_*` / `dm_*` (mart). Custom
materializations and JSON macros are copied from fv_dwh (`macros/`).

Also carried over from fv_dwh to keep modeling/architecture identical:

- **Schema resolution** — `macros/get_custom_schema.sql` overrides `generate_schema_name`:
  prod target -> clean schema names (`staging` / `core` / `mart`) via `generate_schema_name_for_env`;
  dev target -> `{default_schema}_{custom}` (e.g. `dwh_staging`, `dwh_core`, `dwh_mart`).
- **Packages** — `packages.yml` declares `dbt_utils` and `dbt_expectations` (same versions as fv_dwh);
  the mart's `commission = 0` check uses `dbt_utils.expression_is_true`.
- **Per-model staging yml** — each `stg_*` model has its own `.yml` declaring its `json_raw` source and
  description, matching the fv_dwh staging convention (1 yml per model).
- **profiles.yml** — both targets use BigQuery with a **service-account keyfile**. `prod` injects the
  connection via `--vars` (`DBT_KEYFILE` / `DBT_PROJECT` / `DBT_DATASET` / `DBT_LOCATION`); `dev` reads
  the same values from `env_var` fallbacks so it runs from a shell.

## Lineage of the mart

```
json_raw.ecopay_ecopay_transactions -> stg_ecopay_transactions -> dim_ecopay_transactions -\
json_raw.ecopay_ecopay_stores       -> stg_ecopay_stores       -> dim_ecopay_stores       --> dwh_ecopay_transaction_detail
json_raw.dms1_users -> stg_dms1_users -> dim_dms1_users -> dm_dms1_user_manager_phones ----/
```

## Scope note

Staging, core, mart, and `dm_dms1_user_manager_phones` models are **copied verbatim from fv_dwh** (full
column sets, real JSON paths, identical logic). Only the models on this flow's lineage are included, and
the Debezium delete-tracking config (`debezium_topic` / `debezium_deleted_records`) is intentionally
omitted so the project stands alone.

## Run (BigQuery)

Prerequisites: a GCP project, a BigQuery-enabled service account, and its JSON keyfile.

```bash
# 0. install the BigQuery adapter + deps
pip install dbt-bigquery
dbt deps                                    # install dbt_utils / dbt_expectations / dbt_date

# 1. point dbt at your project (dev target reads these env_var fallbacks)
export DBT_KEYFILE=/abs/path/to/keyfile.json
export DBT_PROJECT=my-gcp-project
export DBT_DATASET=dwh
export DBT_LOCATION=asia-southeast1

# 2. bootstrap the json_raw landing dataset (one-off; mirrors the prod ingest)
bq query --use_legacy_sql=false --project_id="$DBT_PROJECT" --location="$DBT_LOCATION" \
  < seeds_local/00_json_raw_seed_bigquery.sql

# 3. build the lineage (dev target -> datasets dwh_staging / dwh_core / dwh_mart)
dbt build --profiles-dir .
```

The models read the `json_raw` landing tables (`json_raw.ecopay_ecopay_transactions`,
`json_raw.ecopay_ecopay_stores`, `json_raw.dms1_users`) from the same project. In production these are
filled by the Kafka/Debezium ingest; locally the seed script above stands in for them.

## DataOps / orchestration

Airflow DAGs live in `dags/` and mirror the fv_dwh ecopay pattern (`prod_mart_ecopay_transaction_dbt_bash.py`):

- `mart_ecopay_transaction_detail_dbt_bash.py` — dev DAG: `dbt test` + `dbt run` over
  `+models/mart/ecopay/`, scheduled `0 6` Asia/Ho_Chi_Minh.
- `prod_mart_ecopay_transaction_detail_dbt_bash.py` — prod DAG: `dbt deps && dbt run` over
  `+models/mart/ecopay/`, scheduled `0 5` Asia/Ho_Chi_Minh.

Both follow the fv_dwh DataOps conventions:

- **Connection injected via Airflow Variable** `profile_args_dwh_dbt` → `--vars DBT_KEYFILE/DBT_PROJECT/DBT_DATASET/DBT_LOCATION`
  consumed by the `prod` target in `profiles.yml` (no secrets in the repo). The keyfile must be
  reachable from the worker (or use a GCP-attached service account).
- **Timezone-aware scheduling** via `CronTriggerTimetable(..., timezone="Asia/Ho_Chi_Minh")`.
- **Single-invocation build** of the mart plus all upstream through the `+models/mart/ecopay/` selector,
  `--threads 2`, `--full-refresh` toggle via a DAG param, `dbt deps` before run.

`DBT_PROJECT_PATH` is resolved relative to the DAG file, so the DAGs are self-contained wherever deployed.
There is no CI/CD, source-freshness, or on-run hook config — matching fv_dwh, which has none.
