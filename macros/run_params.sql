{#
  Snapshot time of the data = latest version start in the listings history.
  Used instead of current_timestamp(): the extract ends 2025-10-29, so "now" would inflate
  days-on-platform for every listing still active in the extract.
#}
{% macro data_snapshot_ts() -%}
(select max(valid_from) from {{ ref('stg_listings_versions') }})
{%- endmacro %}


{#
  Business date the daily notification export is produced for.
  Default = last COMPLETE day in the data (the snapshot day itself is partial).
#}
{% macro notification_run_date() -%}
{%- if var('run_date') -%}
date('{{ var("run_date") }}')
{%- else -%}
date_sub(date({{ data_snapshot_ts() }}), interval 1 day)
{%- endif -%}
{%- endmacro %}


{# Optional date partitioning; off by default because the BigQuery sandbox expires old partitions #}
{% macro partition_config(field) -%}
{%- if var('use_partitioning') -%}
{{ return({'field': field, 'data_type': 'date', 'granularity': 'day'}) }}
{%- else -%}
{{ return(none) }}
{%- endif -%}
{%- endmacro %}
