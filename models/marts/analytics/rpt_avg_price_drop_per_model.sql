/*
  Q5a - Average price drop per model.

  Two complementary views, both based on the same is_valid_drop definition:
  - per drop EVENT: how big is a typical single price cut (CHF and %)
  - per LISTING: total reduction from first live price to lowest live price, for listings that
    dropped at least once (what a seller of this model typically gives away overall)
  Cars only; models with fewer than min_model_sample_size drop events are excluded (noise).
*/

with drops as (

    select *
    from {{ ref('fct_listing_price_changes') }}
    where is_valid_drop
      and vehicle_category = 'car'
      and model_key is not null

),
per_listing as (

    select
        make_key,
        model_key,
        listing_id,
        max(running_peak - new_price)                         as max_drawdown_chf,
        max(safe_divide(running_peak - new_price, running_peak)) as max_drawdown_pct
    from (
        select *,
               max(old_price) over (partition by listing_id order by changed_at
                                    rows between unbounded preceding and current row) as running_peak
        from drops
    )
    group by make_key, model_key, listing_id

),

listing_agg as (

    select
        make_key,
        model_key,
        count(*)                   as n_listings_with_drop,
        avg(max_drawdown_chf)      as avg_total_drop_per_listing_chf,
        avg(max_drawdown_pct)      as avg_total_drop_per_listing_pct
    from per_listing
    group by make_key, model_key

),
event_agg as (

    select
        make_key,
        model_key,
        count(*)                                                as n_drop_events,
        avg(drop_amount)                                        as avg_drop_chf,
        approx_quantiles(drop_amount, 2)[offset(1)]             as median_drop_chf,
        avg(drop_pct)                                           as avg_drop_pct,
        avg(old_price)                                          as avg_price_before_drop_chf
    from drops
    group by make_key, model_key

)

select
    e.make_key,
    e.model_key,
    e.n_drop_events,
    l.n_listings_with_drop,
    round(e.avg_drop_chf, 0)                         as avg_drop_chf,
    round(e.median_drop_chf, 0)                      as median_drop_chf,
    round(e.avg_drop_pct * 100, 2)                   as avg_drop_pct,
    round(e.avg_price_before_drop_chf, 0)            as avg_price_before_drop_chf,
    round(l.avg_total_drop_per_listing_chf, 0)       as avg_total_drop_per_listing_chf,
    round(l.avg_total_drop_per_listing_pct * 100, 2) as avg_total_drop_per_listing_pct
from event_agg as e
join listing_agg as l using (make_key, model_key)
where e.n_drop_events >= {{ var('min_model_sample_size') }}
