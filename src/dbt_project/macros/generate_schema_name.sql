{#
    Use the custom schema name verbatim (raw / staging / marts) instead of
    dbt's default "<target_schema>_<custom_schema>" concatenation. Keeps
    materialized objects in workspace.staging.* and workspace.marts.* to match
    the source/seed layout.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
