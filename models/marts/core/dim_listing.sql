{{ config(cluster_by = ['vehicle_category', 'make_key', 'model_key']) }}

/*
  One row per listing: latest known attributes + lifecycle + price-drop summary.
*/

with current_version as (

    select * from {{ ref('stg_listings__versions') }}
    where is_current

),

first_live_price as (

    select
        listing_id,
        array_agg(price ignore nulls order by valid_from limit 1)[safe_offset(0)] as first_activated_price
    from {{ ref('stg_listings__versions') }}
    where status = 'activated'
    group by listing_id

),

drops as (

    select
        listing_id,
        countif(is_valid_drop)                       as n_valid_price_drops,
        sum(if(is_valid_drop, drop_amount, 0))       as total_valid_drop_amount,
        max(if(is_valid_drop, changed_at, null))     as last_valid_drop_at
    from {{ ref('int_listing_price_changes') }}
    group by listing_id

)

select
    c.listing_id,
    c.platform,
    c.vehicle_category,
    c.seller_type,
    c.condition_type,
    c.make_key,
    c.model_key,
    c.version_full_name,
    c.body_type,
    c.body_color,
    c.fuel_type,
    c.transmission_type,
    c.cubic_capacity,
    c.first_registration_year,
    c.mileage,
    c.number_of_images,
    c.status                                       as current_status,
    c.price                                        as current_price,
    c.is_price_on_request,
    c.created_date,
    c.activation_date,
    c.valid_from                                   as last_changed_at,
    l.first_seen_at,
    l.first_activated_at,
    l.last_active_until,
    l.days_active,
    l.activation_episodes,
    l.was_ever_activated,
    l.is_active_at_snapshot,
    f.first_activated_price,
    coalesce(d.n_valid_price_drops, 0)             as n_valid_price_drops,
    coalesce(d.total_valid_drop_amount, 0)         as total_valid_drop_amount,
    d.last_valid_drop_at
from current_version as c
left join {{ ref('int_listing_lifecycle') }} as l using (listing_id)
left join first_live_price as f using (listing_id)
left join drops as d using (listing_id)
