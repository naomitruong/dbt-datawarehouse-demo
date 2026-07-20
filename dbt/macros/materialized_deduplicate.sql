{#-
  BigQuery port of the `materialized_deduplicate` materialization.

  Keeps the latest row per `unique_key` (ordered by `priority_sort_columns` DESC).
  Interface is unchanged from the Postgres version so model configs need no edits, but the
  BigQuery-only mechanics differ:
    - full refresh / first run -> CREATE OR REPLACE TABLE with a QUALIFY dedup (no temp tables)
    - incremental run          -> MERGE, deduplicating the incoming slice with QUALIFY
    - no CREATE INDEX / GRANT / BEGIN..COMMIT (not applicable on BigQuery)
  Config keys that only make sense on Postgres (`index_columns`, `debezium_topic`, ...) are accepted
  and ignored. Optional `partition_by` / `cluster_by` configs are honoured if provided.
-#}
{%- materialization materialized_deduplicate, adapter='bigquery' -%}

{%- set target_relation = this -%}
{%- set unique_key = config.require('unique_key') -%}
{%- set timestamp_column = config.require('timestamp_column') -%}
{%- set priority_sort_columns = config.require('priority_sort_columns') -%}
{%- set data_retention_days = config.get('data_retention_days', none) -%}
{%- set partition_by = config.get('partition_by', none) -%}
{%- set cluster_by = config.get('cluster_by', none) -%}

{%- set existing = load_cached_relation(target_relation) -%}

{#- ORDER BY clause used to pick the surviving row per key -#}
{%- set order_clause -%}
    {% for col in priority_sort_columns %}`{{ col }}` DESC{% if not loop.last %}, {% endif %}{% endfor %}
{%- endset -%}

{#- Table options (partitioning / clustering) for the create branch -#}
{%- set table_options -%}
    {% if partition_by is not none %}PARTITION BY {{ partition_by }}{% endif %}
    {% if cluster_by is not none %}CLUSTER BY {{ cluster_by if cluster_by is string else cluster_by | join(', ') }}{% endif %}
{%- endset -%}

{%- if existing is none or should_full_refresh() -%}
    {%- set build_sql -%}
        CREATE OR REPLACE TABLE {{ target_relation }}
        {{ table_options }}
        AS (
            WITH source_data AS (
                {{ sql }}
            )
            SELECT * FROM source_data
            WHERE {{ unique_key }} IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {{ unique_key }} ORDER BY {{ order_clause }}
            ) = 1
        )
    {%- endset -%}
{%- else -%}
    {#- Incremental slice condition on the timestamp column (bare boolean, no leading WHERE) -#}
    {%- set ts = timestamp_column -%}
    {%- if data_retention_days is not none -%}
        {%- if ts == 'ts_ms' -%}
            {%- set slice_condition = ts ~ ' > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL ' ~ data_retention_days ~ ' DAY))' -%}
        {%- else -%}
            {%- set slice_condition = ts ~ ' > UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ' ~ data_retention_days ~ ' DAY)) * 1000' -%}
        {%- endif -%}
    {%- else -%}
        {%- if ts == 'ts_ms' -%}
            {%- set slice_condition = ts ~ ' > (SELECT COALESCE(MAX(t.' ~ ts ~ "), TIMESTAMP '1970-01-01 00:00:00') FROM " ~ target_relation | string ~ ' t)' -%}
        {%- else -%}
            {%- set slice_condition = ts ~ ' > (SELECT COALESCE(MAX(t.' ~ ts ~ '), 0) FROM ' ~ target_relation | string ~ ' t)' -%}
        {%- endif -%}
    {%- endif -%}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set key_cols = unique_key if unique_key is not string else [unique_key] -%}
    {%- set update_columns = dest_columns | rejectattr('name', 'in', key_cols) | list -%}
    {%- set build_sql -%}
        MERGE INTO {{ target_relation }} AS target
        USING (
            WITH new_rows AS (
                {{ sql }}
            )
            SELECT * FROM new_rows
            WHERE {{ unique_key }} IS NOT NULL
              AND {{ slice_condition }}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {{ unique_key }} ORDER BY {{ order_clause }}
            ) = 1
        ) AS source
        ON target.{{ unique_key }} = source.{{ unique_key }}
        WHEN MATCHED THEN UPDATE SET
            {% for col in update_columns -%}
                `{{ col.name }}` = source.`{{ col.name }}`{% if not loop.last %},{% endif %}
            {% endfor %}
        WHEN NOT MATCHED THEN INSERT
            ({% for col in dest_columns -%}`{{ col.name }}`{% if not loop.last %}, {% endif %}{%- endfor %})
        VALUES
            ({% for col in dest_columns -%}source.`{{ col.name }}`{% if not loop.last %}, {% endif %}{%- endfor %})
    {%- endset -%}
{%- endif -%}

{{ run_hooks(pre_hooks) }}

{%- call statement('main') -%}
    {{ build_sql }}
{%- endcall -%}

{{ run_hooks(post_hooks) }}

{{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
