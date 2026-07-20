
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {% if target.name == 'prod' %}
        {{ generate_schema_name_for_env(custom_schema_name, node) }}
    {% else %}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {% endif %}
{%- endmacro %}