/*
  One row per saved search, parsed from the raw line.

  Raw line format: id,user_id,created_at,deleted_at,{json with commas}
  -> first 4 fields split on comma, everything after the 4th comma is the JSON.

  Supported filters (exist in listings): make_key, model_key, color -> body_color,
  price_min/max, first_registration_year_min.
  Unsupported: horse_power_min (no horsepower in listings) -> kept in unsupported_filters
  and IGNORED in matching (documented in README).
*/

with source as (

    select trim(raw_line) as line
    from {{ source('raw', 'users_saved_searches') }}
    where trim(raw_line) <> ''                       -- file contains an empty line
      and not starts_with(trim(raw_line), 'id,')     -- header, if loaded without skip_leading_rows

),

split as (

    select
        regexp_extract(line, r'^([^,]*),')                        as saved_search_id_str,
        regexp_extract(line, r'^[^,]*,([^,]*),')                  as user_id_str,
        regexp_extract(line, r'^(?:[^,]*,){2}([^,]*),')           as created_at_str,
        regexp_extract(line, r'^(?:[^,]*,){3}([^,]*),')           as deleted_at_str,
        safe.parse_json(regexp_extract(line, r'^(?:[^,]*,){4}(.*)$')) as params
    from source

)

select
    safe_cast(saved_search_id_str as int64)                                  as saved_search_id,
    safe_cast(user_id_str as int64)                                          as user_id,
    safe_cast(created_at_str as timestamp)                                   as created_at,   -- no tz in file: assumed UTC
    safe_cast(nullif(deleted_at_str, '') as timestamp)                       as deleted_at,
    lower(json_value(params, '$.make_key'))                                  as make_key,
    lower(json_value(params, '$.model_key'))                                 as model_key,
    lower(json_value(params, '$.color'))                                     as body_color,
    safe_cast(json_value(params, '$.price_min') as numeric)                  as price_min,
    safe_cast(json_value(params, '$.price_max') as numeric)                  as price_max,
    safe_cast(json_value(params, '$.first_registration_year_min') as int64)  as first_registration_year_min,
    safe_cast(json_value(params, '$.horse_power_min') as int64)              as horse_power_min,
    array_to_string(array(
        select k from unnest(json_keys(params)) as k
        where k not in ('make_key', 'model_key', 'color', 'price_min', 'price_max',
                        'first_registration_year_min')
        order by k
    ), ',')                                                                  as unsupported_filters,
    params is null                                                           as is_params_invalid,
    to_json_string(params)                                                   as search_params_json
from split
