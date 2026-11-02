-- Every matched listing satisfies every supported filter of the saved search (re-checked independently)
select m.match_id
from {{ ref('fct_saved_search_price_drop_matches') }} as m
join {{ ref('dim_saved_search') }} as s using (saved_search_id)
join {{ ref('fct_listing_price_changes') }} as d using (price_change_id)
where (s.make_key  is not null and d.make_key   is distinct from s.make_key)
   or (s.model_key is not null and d.model_key  is distinct from s.model_key)
   or (s.body_color is not null and d.body_color is distinct from s.body_color)
   or (s.price_min is not null and d.new_price < s.price_min)
   or (s.price_max is not null and d.new_price > s.price_max)
   or (s.first_registration_year_min is not null
       and coalesce(d.first_registration_year, 0) < s.first_registration_year_min)
   or not d.is_valid_drop
