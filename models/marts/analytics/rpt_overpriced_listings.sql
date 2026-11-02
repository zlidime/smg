/*
  Q5c - Overpriced listings.

  Criteria:
  1. Peer group = cars with the same make, model, first registration year, condition and
     mileage band, live at the snapshot, with a valid price. Groups smaller than
     min_peer_group_size are ignored (median not meaningful).
  2. Overpriced = price more than overpriced_premium_pct (25%) above the peer-group MEDIAN
     (median, not mean: robust to remaining outliers).
  3. Extra signal, not a filter: is_stale = live > 60 days without a single valid price drop
     -> the market is confirming the overpricing (nobody buys, seller does not react).

  Mileage is banded and missing mileage is its own band: profiling showed unreliable values
  (116k 'used' with 0 km), so it should not decide alone.
*/

with live as (

    select
        *,
        case
            when mileage is null    then 'unknown'
            when mileage < 10000    then '0-10k'
            when mileage < 30000    then '10-30k'
            when mileage < 60000    then '30-60k'
            when mileage < 100000   then '60-100k'
            when mileage < 150000   then '100-150k'
            else '150k+'
        end as mileage_band
    from {{ ref('dim_listing') }}
    where vehicle_category = 'car'
      and is_active_at_snapshot
      and current_price is not null
      and model_key is not null
      and first_registration_year is not null

),

peers as (

    select
        make_key, model_key, first_registration_year, condition_type, mileage_band,
        count(*)                                           as n_peers,
        approx_quantiles(current_price, 100)[offset(25)]   as peer_p25_price,
        approx_quantiles(current_price, 100)[offset(50)]   as peer_median_price,
        approx_quantiles(current_price, 100)[offset(75)]   as peer_p75_price
    from live
    group by make_key, model_key, first_registration_year, condition_type, mileage_band
    having count(*) >= {{ var('min_peer_group_size') }}

)

select
    l.listing_id,
    concat('{{ var("listing_url_prefix") }}', cast(l.listing_id as string)) as listing_url,
    l.make_key,
    l.model_key,
    l.version_full_name,
    l.first_registration_year,
    l.condition_type,
    l.mileage,
    l.mileage_band,
    l.seller_type,
    l.current_price,
    p.peer_median_price,
    p.peer_p25_price,
    p.peer_p75_price,
    p.n_peers,
    round(safe_divide(l.current_price, p.peer_median_price) - 1, 4)   as price_premium_pct,
    l.current_price - p.peer_median_price                             as price_premium_chf,
    l.days_active,
    l.n_valid_price_drops,
    l.days_active > 60 and l.n_valid_price_drops = 0                   as is_stale
from live as l
join peers as p using (make_key, model_key, first_registration_year, condition_type, mileage_band)
where l.current_price > p.peer_median_price * (1 + {{ var('overpriced_premium_pct') }})
