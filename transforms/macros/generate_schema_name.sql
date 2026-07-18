{#-
    Drop dbt's default `<target>_<custom_schema>` prefixing.
    The spec mandates that silver / gold schemas are literal so the API
    can read `gold.candles_1m` regardless of the dbt target environment.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}