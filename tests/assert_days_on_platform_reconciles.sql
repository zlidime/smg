-- Reconciliation: rpt_days_on_platform (GROUPING SETS) vs. each level recomputed with its own GROUP BY.
-- Returns only discrepancies. Counts must match exactly; averages within rounding; medians within
-- 5% or 2 days (APPROX_QUANTILES is approximate and its error grows with group size).

{% set metrics %}
    count(*)                                                                  as n_listings,
    countif(not is_active_at_snapshot)                                        as n_completed,
    round(avg(if(not is_active_at_snapshot, days_active, null)), 1)           as avg_days_completed,
    round(approx_quantiles(if(not is_active_at_snapshot, days_active, null), 2 ignore nulls)[safe_offset(1)], 1) as median_days_completed,
    round(avg(days_active), 1)                                                as avg_days_all,
    round(avg(activation_episodes), 2)                                        as avg_activation_episodes
{% endset %}

with cars as (
    select * from {{ ref('dim_listing') }}
    where vehicle_category = 'car' and was_ever_activated
),

expected as (
    select 'all_cars' as grouping_level, cast(null as string) as seller_type,
           cast(null as string) as make_key, cast(null as string) as model_key, {{ metrics }}
    from cars
    union all
    select 'seller_type', seller_type, cast(null as string), cast(null as string), {{ metrics }}
    from cars group by seller_type
    union all
    select 'make', cast(null as string), make_key, cast(null as string), {{ metrics }}
    from cars group by make_key
    having count(*) >= {{ var('min_model_sample_size') }}
    union all
    select 'make_model', cast(null as string), make_key, model_key, {{ metrics }}
    from cars group by make_key, model_key
    having count(*) >= {{ var('min_model_sample_size') }}
),

compared as (
    select
        coalesce(r.grouping_level, e.grouping_level) as grouping_level,
        coalesce(r.seller_type, e.seller_type)       as seller_type,
        coalesce(r.make_key, e.make_key)             as make_key,
        coalesce(r.model_key, e.model_key)           as model_key,
        r.n_listings as report_n, e.n_listings as expected_n,
        r.median_days_completed as report_median, e.median_days_completed as expected_median,
        case
            when r.grouping_level is null then 'MISSING_FROM_REPORT'
            when e.grouping_level is null then 'EXTRA_IN_REPORT'
            when r.n_listings != e.n_listings
              or r.n_completed != e.n_completed
              or abs(coalesce(r.avg_days_completed, 0) - coalesce(e.avg_days_completed, 0)) > 0.1
              or abs(coalesce(r.avg_days_all, 0) - coalesce(e.avg_days_all, 0)) > 0.1
              or abs(coalesce(r.avg_activation_episodes, 0) - coalesce(e.avg_activation_episodes, 0)) > 0.01
              or abs(coalesce(r.median_days_completed, 0) - coalesce(e.median_days_completed, 0))
                 > greatest(2, 0.05 * coalesce(e.median_days_completed, 0))  -- APPROX_QUANTILES: up to ~1-2 days on 100k-1.3M listings
            then 'FAIL'
            else 'PASS'
        end as audit_status
    from {{ ref('rpt_days_on_platform') }} as r
    full outer join expected as e
      on  r.grouping_level = e.grouping_level
      and r.seller_type is not distinct from e.seller_type
      and r.make_key    is not distinct from e.make_key
      and r.model_key   is not distinct from e.model_key
)

select * from compared where audit_status != 'PASS'
