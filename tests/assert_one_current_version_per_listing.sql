-- SCD2 invariant: every listing has exactly one current version (valid_to = 9999-01-01)
select listing_id, countif(is_current) as n_current
from {{ ref('stg_listings_versions') }}
group by listing_id
having countif(is_current) != 1
