/*
  Q5b - Average number of days a car is on the platform.

  Definition: time spent in status 'activated' (live and visible), summed over relists.
  Two populations, because listings still live at the snapshot are right-censored
  (their final duration is unknown, so they pull the average down):
    - avg_days_completed : listings no longer live -> unbiased "time to sell / remove"
    - avg_days_all       : all listings ever live, censored ones counted up to the snapshot
  Grouping levels: overall cars, per seller type, per make, per make+model.
*/

with cars as (

    select *
    from {{ ref('dim_listing') }}
    where vehicle_category = 'car'
      and was_ever_activated

),

agg as (

    select
        case
            when grouping(make_key) = 0 and grouping(model_key) = 0 then 'make_model'
            when grouping(make_key) = 0                             then 'make'
            when grouping(seller_type) = 0                          then 'seller_type'
            else 'all_cars'
        end                                                       as grouping_level,
        seller_type,
        make_key,
        model_key,
        count(*)                                                  as n_listings,
        countif(not is_active_at_snapshot)                        as n_completed,
        round(avg(if(not is_active_at_snapshot, days_active, null)), 1)                 as avg_days_completed,
        round(approx_quantiles(if(not is_active_at_snapshot, days_active, null), 2 ignore nulls)[safe_offset(1)], 1) as median_days_completed,
        round(avg(days_active), 1)                                as avg_days_all,
        round(avg(activation_episodes), 2)                        as avg_activation_episodes
    from cars
    group by grouping sets ((), (seller_type), (make_key), (make_key, model_key))

)

select *
from agg
where grouping_level in ('all_cars', 'seller_type')
   or n_listings >= {{ var('min_model_sample_size') }}
