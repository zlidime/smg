/*
  One row per listing: how long it was actually live.

  days_active = sum of time spent in status 'activated' (capped at the data snapshot).
  NOT removed_date - created_date: profiling shows 336k removed -> activated and
  222k deactivated -> activated transitions (relisting / pausing), and ~2.5M draft versions
  that were never visible to buyers.

  activation_episodes = number of separate live periods (relists).
  is_active_at_snapshot = still live when the extract was taken -> days_active is a lower bound
  (right-censored), analytics report it separately.
*/

with versions as (

    select
        v.listing_id,
        v.status,
        v.valid_from,
        least(v.valid_to, s.snapshot_ts) as valid_to_capped,
        v.is_current,
        lag(v.status) over (partition by v.listing_id order by v.valid_from) as prev_status
    from {{ ref('stg_listings__versions') }} as v
    cross join (select {{ data_snapshot_ts() }} as snapshot_ts) as s

)

select
    listing_id,
    min(valid_from)                                                    as first_seen_at,
    min(if(status = 'activated', valid_from, null))                    as first_activated_at,
    max(if(status = 'activated', valid_to_capped, null))               as last_active_until,
    round(sum(if(status = 'activated',
              timestamp_diff(valid_to_capped, valid_from, second), 0)) / 86400, 2) as days_active,
    countif(status = 'activated'
            and (prev_status is null or prev_status != 'activated'))   as activation_episodes,
    logical_or(is_current and status = 'activated')                    as is_active_at_snapshot,
    logical_or(status = 'activated')                                   as was_ever_activated
from versions
group by listing_id
