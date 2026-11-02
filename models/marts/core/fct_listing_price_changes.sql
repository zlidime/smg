{{
  config(
    partition_by = partition_config('changed_date'),
    cluster_by = ['make_key', 'model_key', 'listing_id']
  )
}}

/*
  Fact: one row per public price change, with the vehicle attributes valid AT THE TIME of the change
  (taken from the version that carries the new price, not from the current version).
  Analysts filter is_valid_drop; rejected rows stay for audit.
*/

select
    pc.price_change_id,
    pc.listing_id,
    pc.changed_at,
    pc.changed_date,
    pc.change_type,
    pc.old_price,
    pc.new_price,
    pc.price_change_amount,
    pc.price_change_pct,
    pc.drop_amount,
    pc.drop_pct,
    pc.quality_issue,
    pc.is_valid_drop,
    v.platform,
    v.vehicle_category,
    v.seller_type,
    v.condition_type,
    v.make_key,
    v.model_key,
    v.version_full_name,
    v.body_type,
    v.body_color,
    v.fuel_type,
    v.first_registration_year,
    v.mileage
from {{ ref('int_listing_price_changes') }} as pc
join {{ ref('stg_listings_versions') }} as v
  on v.listing_version_id = pc.price_change_id
