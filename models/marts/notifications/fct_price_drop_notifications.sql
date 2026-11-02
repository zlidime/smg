{{ config(cluster_by = ['drop_date', 'user_id']) }}

/*
  Grain: one row per (user, listing, drop_date) candidate notification, for ALL days in history.
  The daily export (rpt_daily_price_drop_notifications) is a filter on this table, so any past
  day can be re-produced / backfilled with the same logic.

  Steps
  1. Collapse matches: several saved searches of one user, or several drops of one listing on the
     same day -> one row. price_before = price before the first drop of the day.
  2. Verify end-of-day state: listing still 'activated' and its end-of-day price still below
     price_before (no notification for a car that was sold / paused / repriced up the same day).
  3. Cooldown: suppress if the same user + listing already had a candidate in the last
     notification_cooldown_days days at the same or a lower price (re-notify only on a further drop).
  4. Digest cap: rank remaining rows per user and day by drop_pct; keep top max_listings_per_digest.
*/

with matches as (

    select * from {{ ref('fct_saved_search_price_drop_matches') }}

),

per_user_listing_day as (

    select
        user_id,
        listing_id,
        changed_date                                                       as drop_date,
        array_agg(old_price order by changed_at limit 1)[offset(0)]        as price_before,
        max(changed_at)                                                    as last_drop_at,
        array_to_string(array_agg(distinct cast(saved_search_id as string)
                                  order by cast(saved_search_id as string)), ',') as matched_saved_search_ids,
        string_agg(distinct search_label, ' | ')                           as matched_search_labels,
        logical_or(entered_price_range)                                    as entered_price_range,
        logical_or(has_unsupported_filters)                                as matched_search_has_unsupported_filters
    from matches
    group by user_id, listing_id, changed_date

),

end_of_day as (

    select
        p.*,
        v.status                                                           as eod_status,
        v.price                                                            as price_now,
        v.platform,
        v.make_key,
        v.model_key,
        v.version_full_name,
        v.first_registration_year,
        v.mileage,
        v.body_color,
        v.fuel_type,
        v.seller_type,
        v.number_of_images
    from per_user_listing_day as p
    join {{ ref('stg_listings__versions') }} as v
      on  v.listing_id = p.listing_id
      and v.valid_from <= timestamp_sub(timestamp(date_add(p.drop_date, interval 1 day)), interval 1 microsecond)
      and v.valid_to   >  timestamp_sub(timestamp(date_add(p.drop_date, interval 1 day)), interval 1 microsecond)

),

eligible as (

    select *
    from end_of_day
    where eod_status = 'activated'
      and price_now < price_before

),

with_cooldown as (

    select
        *,
        min(price_now) over (
            partition by user_id, listing_id
            order by unix_date(drop_date)
            range between {{ var('notification_cooldown_days') }} preceding and 1 preceding
        )                                                                  as min_price_in_cooldown
    from eligible

),

flagged as (

    select
        *,
        coalesce(price_now >= min_price_in_cooldown, false)                as is_suppressed_by_cooldown
    from with_cooldown

),

ranked as (

    select
        f.*,
        case when not is_suppressed_by_cooldown then
            row_number() over (
                partition by user_id, drop_date, is_suppressed_by_cooldown
                order by entered_price_range desc,
                         safe_divide(price_before - price_now, price_before) desc,
                         price_before - price_now desc,
                         listing_id
            )
        end                                                                as digest_rank
    from flagged as f

)

select
    {{ dbt_utils.generate_surrogate_key(['r.user_id', 'r.listing_id', 'r.drop_date']) }} as notification_id,
    r.drop_date,
    date_add(r.drop_date, interval 1 day)                                  as send_date,
    r.user_id,
    r.listing_id,
    r.platform,
    r.make_key,
    r.model_key,
    r.version_full_name,
    r.first_registration_year,
    r.mileage,
    r.body_color,
    r.fuel_type,
    r.seller_type,
    r.number_of_images,
    r.price_before,
    r.price_now,
    r.price_before - r.price_now                                           as drop_amount,
    round(safe_divide(r.price_before - r.price_now, r.price_before), 4)    as drop_pct,
    r.entered_price_range,
    date_diff(r.drop_date, date(l.first_activated_at), day)                as days_on_platform,
    r.matched_saved_search_ids,
    r.matched_search_labels,
    r.matched_search_has_unsupported_filters,
    r.last_drop_at,
    r.min_price_in_cooldown,
    r.is_suppressed_by_cooldown,
    r.digest_rank,
    coalesce(r.digest_rank <= {{ var('max_listings_per_digest') }}, false) as is_selected_for_digest,
    concat('{{ var("listing_url_prefix") }}', cast(r.listing_id as string)) as listing_url
from ranked as r
left join {{ ref('dim_listing') }} as l using (listing_id)
