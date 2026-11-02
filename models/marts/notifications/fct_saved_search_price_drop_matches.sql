{{ config(cluster_by = ['user_id', 'listing_id']) }}

/*
  Grain: one row per (saved search, valid price drop) that matches.

  Matching rules:
  - only valid drops (is_valid_drop) on configured vehicle categories (cars by default)
  - the drop happened while the search was active: created_at <= changed_at < deleted_at
    (no notifications about history before the user saved the search)
  - every filter present in the search must hold on the listing AS OF the drop, using the
    NEW price; a NULL filter means "no constraint"
  - unsupported filters (horse_power_min) are ignored; flagged via has_unsupported_filters

  Small search table (15 rows) -> cross join + filter is cheap. In production with millions of
  searches this becomes an equi-join on make_key with a fallback branch for searches without make.
*/

with drops as (

    select *
    from {{ ref('fct_listing_price_changes') }}
    where is_valid_drop
      and vehicle_category in (
        {%- for c in var('notification_vehicle_categories') -%}
          '{{ c }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
      )

),

searches as (

    select * from {{ ref('dim_saved_search') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['s.saved_search_id', 'd.price_change_id']) }} as match_id,
    s.saved_search_id,
    s.user_id,
    s.search_label,
    s.has_unsupported_filters,
    d.price_change_id,
    d.listing_id,
    d.changed_at,
    d.changed_date,
    d.old_price,
    d.new_price,
    d.drop_amount,
    d.drop_pct,
    -- the drop moved the car INTO the user's budget: strongest reason to notify
    (s.price_max is not null and d.old_price > s.price_max)          as entered_price_range
from drops as d
cross join searches as s
where d.changed_at >= s.created_at
  and (s.deleted_at is null or d.changed_at < s.deleted_at)
  and (s.make_key  is null or d.make_key   = s.make_key)
  and (s.model_key is null or d.model_key  = s.model_key)
  and (s.body_color is null or d.body_color = s.body_color)
  and (s.price_min is null or d.new_price >= s.price_min)
  and (s.price_max is null or d.new_price <= s.price_max)
  and (s.first_registration_year_min is null
       or d.first_registration_year >= s.first_registration_year_min)
