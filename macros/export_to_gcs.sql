{#
  Export a model to GCS as CSV with BigQuery EXPORT DATA.

  dbt run-operation export_daily_notifications
  dbt run-operation export_analytics

  The bucket must exist and be in the same location as the dataset (EU):
    gcloud storage buckets create gs://vladimir_dimitric_bucket --location=EU
#}

{% macro export_model_to_gcs(model_name, path, order_by=none, bucket=none) %}
  {% set bucket = bucket or var('export_bucket') %}
  {% set uri = 'gs://' ~ bucket ~ '/' ~ path ~ '/' ~ model_name ~ '_*.csv' %}
  {% set sql %}
    export data options (
      uri = '{{ uri }}',
      format = 'CSV',
      overwrite = true,
      header = true,
      field_delimiter = ','
    ) as
    select * from {{ ref(model_name) }}
    {% if order_by %}order by {{ order_by }}{% endif %}
  {% endset %}
  {% do log('Exporting ' ~ model_name ~ ' -> ' ~ uri, info=true) %}
  {% do run_query(sql) %}
{% endmacro %}


{% macro export_daily_notifications(bucket=none) %}
  {% set rel = ref('rpt_daily_price_drop_notifications') %}
  {% set res = run_query('select max(run_date) from ' ~ rel) %}
  {% set run_date = res.columns[0].values()[0] if execute else none %}
  {% if run_date is none %}
    {% do log('No notifications for this run - exporting header-only file is skipped.', info=true) %}
    {% do return(none) %}
  {% endif %}
  {{ export_model_to_gcs(
       'rpt_daily_price_drop_notifications',
       'notifications/run_date=' ~ run_date,
       order_by='user_id, digest_rank',
       bucket=bucket) }}
{% endmacro %}


{% macro export_analytics(bucket=none) %}
  {{ export_model_to_gcs('rpt_avg_price_drop_per_model', 'analytics', order_by='n_drop_events desc', bucket=bucket) }}
  {{ export_model_to_gcs('rpt_days_on_platform',         'analytics', order_by='grouping_level, n_listings desc', bucket=bucket) }}
  {{ export_model_to_gcs('rpt_overpriced_listings',      'analytics', order_by='price_premium_pct desc', bucket=bucket) }}
{% endmacro %}

{% macro export_daily_notifications(bucket=none) %}
  {% set res = run_query('select cast(' ~ notification_run_date() ~ ' as string)') %}
  {% set run_date = res.columns[0].values()[0] if execute else none %}
  {{ export_model_to_gcs('rpt_daily_price_drop_notifications', 'notifications/run_date=' ~ run_date,
                         order_by='user_id, digest_rank', bucket=bucket) }}
  {{ export_model_to_gcs('rpt_daily_notification_recipients', 'notifications/run_date=' ~ run_date,
                         order_by='user_id', bucket=bucket) }}
{% endmacro %}
