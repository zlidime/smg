/*
  Q2 user-level view: one row per user to notify on run_date.
  Detail rows (one per car) are in rpt_daily_price_drop_notifications; both are exported per run_date.
*/
with labels as (

    select
        user_id,
        string_agg(distinct trim(lbl), ' | ' order by trim(lbl)) as matched_searches
    from {{ ref('rpt_daily_price_drop_notifications') }},
         unnest(split(matched_search_labels, ' | ')) as lbl
    group by user_id

)

select
    n.run_date,
    n.send_date,
    n.user_id,
    any_value(n.message_subject)                                       as message_subject,
    count(*)                                                           as n_cars,
    countif(n.entered_price_range)                                     as n_cars_entered_budget,
    max(n.drop_pct)                                                    as max_drop_pct,
    any_value(l.matched_searches)                                      as matched_searches,
    string_agg(n.message_line, '\n' order by n.digest_rank)            as message_body,
    array_to_string(array_agg(cast(n.listing_id as string) order by n.digest_rank), ',') as listing_ids
from {{ ref('rpt_daily_price_drop_notifications') }} as n
join labels as l using (user_id)
group by n.run_date, n.send_date, n.user_id