{#
  Placeholder prices seen in profiling: 1 (placeholder while the ad is drafted), 11111, 55555, 123456,
  7777777, 9999999 ... A repeated-digit or 1234.. sequence >= 10'000 is treated as a placeholder.
  BigQuery RE2 has no back-references, so "all digits equal" = removing the first digit leaves ''.
#}
{% macro is_placeholder_price(col) -%}
(
  {{ col }} is not null
  and {{ col }} >= 10000
  and (
    cast({{ col }} as int64) in (12345, 123456, 1234567, 12345678)
    or length(replace(cast(cast({{ col }} as int64) as string),
                      substr(cast(cast({{ col }} as int64) as string), 1, 1), '')) = 0
  )
)
{%- endmacro %}


{# A price usable for analytics / notifications: above the floor and not a placeholder #}
{% macro clean_price(col) -%}
case
  when {{ col }} >= {{ var('min_valid_price') }} and not {{ is_placeholder_price(col) }}
  then {{ col }}
end
{%- endmacro %}
