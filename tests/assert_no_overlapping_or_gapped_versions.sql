-- SCD2 invariant: consecutive versions of a listing are contiguous (next.valid_from = prev.valid_to).
-- Raw data had 8'997 overlaps caused by zero-length removed->deleted versions; staging must fix them.
with ordered as (
    select
        listing_id,
        valid_from,
        lag(valid_to) over (partition by listing_id order by valid_from) as prev_valid_to
    from {{ ref('stg_listings__versions') }}
)
select *
from ordered
where prev_valid_to is not null
  and prev_valid_to != valid_from
