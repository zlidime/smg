/*
  Q2 user-level view: one row per user to notify on run_date.
  Detail rows (one per car) are in rpt_daily_price_drop_notifications; both are exported per run_date.
*/
select
    run_date,
    send_date,
    user_id,
    any_value(message_subject)                                       as message_subject,
    count(*)                                                         as n_cars,
    countif(entered_price_range)                                     as n_cars_now_in_budget,
    max(drop_pct)                                                    as max_drop_pct,
    string_agg(distinct matched_search_labels, ' | ')                as matched_searches,
    string_agg(message_line, '\n' order by digest_rank)              as message_body,
    array_to_string(array_agg(cast(listing_id as string) order by digest_rank), ',') as listing_ids
from {{ ref('rpt_daily_price_drop_notifications') }}
group by run_date, send_date, user_id