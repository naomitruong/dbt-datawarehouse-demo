{#-
  BigQuery JSON extraction helpers (ported from the Postgres extract_json_* macros).
  Paths are dot-separated, e.g. `_id.$oid` or `sale_info.phone`.
-#}

{#- `a.b` -> `$."a"."b"`. Double quotes keep keys with `$` (Mongo `$oid` / `$date`)
    safe without terminating the single-quoted SQL string around the path. -#}
{%- macro _json_path(path) -%}
{%- set keys = path.split('.') -%}
${%- for key in keys -%}."{{ key }}"{%- endfor -%}
{%- endmacro -%}

{%- macro _bq_type(data_type) -%}
{%- set t = data_type | upper -%}
{%- if 'JSON' in t -%}JSON
{%- elif 'BOOL' in t -%}BOOL
{%- elif 'DOUBLE' in t or 'FLOAT' in t or 'REAL' in t -%}FLOAT64
{%- elif 'NUMERIC' in t or 'DECIMAL' in t -%}NUMERIC
{%- elif t in ['INT', 'INTEGER', 'INT64', 'BIGINT', 'SMALLINT'] -%}INT64
{%- else -%}STRING
{%- endif -%}
{%- endmacro -%}

{%- macro extract_json(json_column, path, data_type='STRING') -%}
{%- set jp = _json_path(path) | trim -%}
{%- set bq = _bq_type(data_type) | trim -%}
{%- if bq == 'JSON' -%}
JSON_QUERY({{ json_column }}, '{{ jp }}')
{%- elif bq == 'STRING' -%}
JSON_VALUE({{ json_column }}, '{{ jp }}')
{%- else -%}
SAFE_CAST(JSON_VALUE({{ json_column }}, '{{ jp }}') AS {{ bq }})
{%- endif -%}
{%- endmacro -%}
-------------------------------------------
{#- Numeric epoch (ms/us/ns) -> TIMESTAMP. Returns NULL when the value is not a bare number. -#}
{%- macro extract_json_timestamp_no_timezone(json_column, path, timestamps_format='ms') -%}
{%- set raw = extract_json(json_column, path) -%}
{%- set epoch = 'SAFE_CAST(SAFE_CAST(' ~ raw ~ ' AS FLOAT64) AS INT64)' -%}
CASE WHEN {{ raw }} IS NULL OR NOT REGEXP_CONTAINS({{ raw }}, r'^[0-9]+\.?[0-9]*$')
    THEN NULL
    {% if timestamps_format == 'ms' %}
    ELSE TIMESTAMP_MILLIS({{ epoch }})
    {% elif timestamps_format == 'us' %}
    ELSE TIMESTAMP_MICROS({{ epoch }})
    {% elif timestamps_format == 'ns' %}
    ELSE TIMESTAMP_MICROS(SAFE_CAST(SAFE_CAST({{ raw }} AS FLOAT64) / 1000 AS INT64))
    {% else %}
    ELSE NULL
    {% endif %}
END
{%- endmacro -%}
-------------------------------------------
{#- Alias for parity with the Postgres macros: BigQuery TIMESTAMP is always UTC,
    so both variants are equivalent. -#}
{%- macro extract_json_timestamp(json_column, path, timestamps_format='ms') -%}
{{ extract_json_timestamp_no_timezone(json_column, path, timestamps_format) }}
{%- endmacro -%}
-------------------------------------------
{#- Numeric epoch OR ISO-8601 string -> TIMESTAMP. -#}
{%- macro extract_json_timestamp_from_str(json_column, path, timestamps_format='ms') -%}
{%- set raw = extract_json(json_column, path) -%}
{%- set clean = 'REPLACE(' ~ raw ~ ", '\"', '')" -%}
{%- set epoch = 'SAFE_CAST(SAFE_CAST(' ~ raw ~ ' AS FLOAT64) AS INT64)' -%}
CASE
    WHEN {{ raw }} IS NULL THEN NULL
    WHEN REGEXP_CONTAINS({{ raw }}, r'^[0-9]+\.?[0-9]*$')
    THEN
        {% if timestamps_format == 'ms' %}
        TIMESTAMP_MILLIS({{ epoch }})
        {% elif timestamps_format == 'us' %}
        TIMESTAMP_MICROS({{ epoch }})
        {% elif timestamps_format == 'ns' %}
        TIMESTAMP_MICROS(SAFE_CAST(SAFE_CAST({{ raw }} AS FLOAT64) / 1000 AS INT64))
        {% else %}
        NULL
        {% endif %}
    WHEN REGEXP_CONTAINS({{ raw }}, r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
    THEN
        CASE
        WHEN REGEXP_CONTAINS({{ raw }}, r'\.\d')
        THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', {{ clean }})
        ELSE SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', {{ clean }})
        END
    ELSE NULL
END
{%- endmacro -%}
-------------------------------------------
{#- Days-since-epoch integer -> DATE. -#}
{%- macro extract_json_date(source, path_json) -%}
DATE_ADD(DATE '1970-01-01', INTERVAL {{ extract_json(source, path_json, 'INT64') }} DAY)
{%- endmacro -%}
