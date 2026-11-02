-- No match for a drop that happened before the search was saved or after it was deleted
select m.*
from {{ ref('fct_saved_search_price_drop_matches') }} as m
join {{ ref('dim_saved_search') }} as s using (saved_search_id)
where m.changed_at < s.created_at
   or (s.deleted_at is not null and m.changed_at >= s.deleted_at)
