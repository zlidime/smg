{{
  config(
    partition_by = partition_config('changed_date'),
    cluster_by = ['listing_id']
  )
}}

/*
  Every price change a buyer could actually see: consecutive ACTIVATED versions only.
  Drafts / deactivated versions in between are skipped (a draft price of 41 CHF was never public).

  Each change is classified; is_valid_drop = TRUE only for drops that pass all quality rules.
  Rejected changes stay in the table with quality_issue for audit.

  quality_issue (first matching rule wins):
    below_min_price         old or new price < min_valid_price (price on request = 1,
                            prices typed in thousands: 210000 -> 194.9)
    placeholder_price       11111 / 55555 / 123456 / 9999999 ...
    drop_exceeds_threshold  drop > max_valid_drop_pct (383600 -> 38300: extra digit)
    reverted_next_change    price goes back UP to >= reverted_drop_ratio of old price within
                            reverted_within_hours (typo fixed / mistaken repricing). Slower reverts
                            are real temporary discounts and stay valid.*/

with activated as (

    select listing_version_id, listing_id, valid_from, price_raw
    from {{ ref('stg_listings_versions') }}
    where status = 'activated'
    and price_raw is not null      -- missing/unparseable price is not a visible price change
                                     -- (0 rows today; guards future data)

),

with_prev as (

    select
        *,
        lag(price_raw)          over (partition by listing_id order by valid_from) as prev_price_raw,
        lag(listing_version_id) over (partition by listing_id order by valid_from) as prev_listing_version_id
    from activated

),

changes as (

    -- most activated -> activated versions are NOT price changes (images, description, nightly batch)
    select *
    from with_prev
    where prev_price_raw is not null
      and price_raw is distinct from prev_price_raw

),
with_next as (

    select
        *,
        lead(price_raw)  over (partition by listing_id order by valid_from) as next_price_raw,
        lead(valid_from) over (partition by listing_id order by valid_from) as next_changed_at
    from changes

),



classified as (

    select
        listing_version_id,
        prev_listing_version_id,
        listing_id,
        valid_from                                       as changed_at,
        date(valid_from)                                 as changed_date,
        prev_price_raw                                   as old_price,
        price_raw                                        as new_price,
        next_price_raw                                   as next_changed_price,
        price_raw - prev_price_raw                       as price_change_amount,
        safe_divide(price_raw - prev_price_raw, prev_price_raw) as price_change_pct,
        case
            when price_raw < prev_price_raw then 'drop'
            when price_raw > prev_price_raw then 'increase'
        end                                              as change_type,
        case
            when prev_price_raw < {{ var('min_valid_price') }}
              or price_raw      < {{ var('min_valid_price') }}
                then 'below_min_price'
            when {{ is_placeholder_price('prev_price_raw') }}
              or {{ is_placeholder_price('price_raw') }}
                then 'placeholder_price'
            when price_raw < prev_price_raw
             and safe_divide(prev_price_raw - price_raw, prev_price_raw) > {{ var('max_valid_drop_pct') }}
                then 'drop_exceeds_threshold'
              when price_raw < prev_price_raw
                    and next_price_raw > price_raw
                    and next_price_raw >= prev_price_raw * {{ var('reverted_drop_ratio') }}
                    and timestamp_diff(next_changed_at, valid_from, hour) < {{ var('reverted_within_hours') }}
                then 'reverted_next_change'
        end                                              as quality_issue
    from with_next

)

select
    listing_version_id                                   as price_change_id,
    *,
    change_type = 'drop' and quality_issue is null        as is_valid_drop,
    case when change_type = 'drop' then old_price - new_price end as drop_amount,
    case when change_type = 'drop'
         then safe_divide(old_price - new_price, old_price) end   as drop_pct
from classified
