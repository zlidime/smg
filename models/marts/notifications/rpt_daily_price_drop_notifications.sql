/*
  DAILY EXPORT (task question 2 + 4).
  One row per listing in a user's digest for run_date. Exported to
  gs://<export_bucket>/notifications/run_date=YYYY-MM-DD/ by
  `dbt run-operation export_daily_notifications`.

  run_date: var('run_date') or, by default, the last complete day in the data.
  Grain: (run_date, user_id, listing_id). A CRM / messaging service joins user_id to the
  user's contact details and preferences (not in this dataset) and renders the message
  from these fields.
*/

with params as (

    select {{ notification_run_date() }} as run_date

),

selected as (

    select n.*
    from {{ ref('fct_price_drop_notifications') }} as n
    cross join params as p
    where n.drop_date = p.run_date
      and n.is_selected_for_digest

)

select
    drop_date                                                    as run_date,
    send_date,
    user_id,
    digest_rank,
    count(*) over (partition by user_id)                         as n_listings_in_digest,
    concat('Price drop on ', cast(count(*) over (partition by user_id) as string),
           if(count(*) over (partition by user_id) = 1, ' car', ' cars'),
           ' matching your saved search')                        as message_subject,
    listing_id,
    listing_url,
    make_key,
    model_key,
    version_full_name,
    first_registration_year,
    mileage,
    body_color,
    fuel_type,
    seller_type,
    cast(price_before as int64)                                  as price_before_chf,
    cast(price_now as int64)                                     as price_now_chf,
    cast(drop_amount as int64)                                   as drop_amount_chf,
    round(drop_pct * 100, 1)                                     as drop_pct,
    entered_price_range,
    days_on_platform,
    matched_saved_search_ids,
    matched_search_labels,
    concat(
        coalesce(version_full_name, concat(make_key, ' ', coalesce(model_key, ''))),
        coalesce(concat(' (', cast(first_registration_year as string), ')'), ''),
        ': ', format("%'d", cast(price_before as int64)), ' -> ',
        format("%'d", cast(price_now as int64)), ' CHF (-',
        cast(round(drop_pct * 100, 1) as string), '%)',
        if(entered_price_range, ' - now within your budget', '')
    )                                                            as message_line
from selected
