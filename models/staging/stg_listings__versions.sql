{{
  config(
    partition_by = partition_config('valid_from_date'),
    cluster_by = ['listing_id', 'valid_from']
  )
}}

/*
  One row per listing version (SCD2), typed and cleaned.

  Rules (see analyses/profiling.sql for the evidence):
  - Zero-length versions (valid_to = valid_from) are dropped. 18'101 of them: the nightly
    batch (~23:45) writes removed -> deleted in the same instant, which also produced
    duplicate (listing_id, valid_from) keys and the 8'997 "overlaps".
  - Raw values are kept (*_raw) for audit; cleaned columns are NULL when invalid.
    Nothing is silently deleted except the zero-length technical versions.
*/

with source as (

    select * from {{ source('raw', 'listings') }}
    where listing_id != 'listing_id'      -- header rows: one per export part if skip-rows is misconfigured

),
typed as (

    select
        safe_cast(listing_id as int64)                               as listing_id,
        lower(nullif(trim(brand), ''))                               as platform,
        lower(nullif(trim(vehicle_category), ''))                    as vehicle_category,
        lower(nullif(trim(seller_type), ''))                         as seller_type,
        lower(nullif(trim(condition_type), ''))                      as condition_type,
        lower(nullif(trim(status), ''))                              as status,
        lower(nullif(trim(make_key), ''))                            as make_key,
        lower(nullif(trim(model_key), ''))                           as model_key,
        nullif(trim(version_full_name), '')                          as version_full_name,
        lower(nullif(trim(body_type), ''))                           as body_type,
        lower(nullif(trim(body_color), ''))                          as body_color,
        lower(nullif(trim(fuel_type), ''))                           as fuel_type,
        lower(nullif(trim(transmission_type_group), ''))             as transmission_type,
        safe_cast(nullif(trim(cubic_capacity), '') as int64)         as cubic_capacity,
        safe_cast(nullif(trim(number_of_images), '') as int64)       as number_of_images,
        safe_cast(nullif(trim(price), '') as numeric)                as price_raw,
        safe_cast(nullif(trim(mileage), '') as int64)                as mileage_raw,
        safe_cast(nullif(trim(first_registration_year), '') as int64) as first_registration_year_raw,
        safe_cast(created_date as date)                              as created_date,
        safe_cast(activation_date as date)                           as activation_date,
        safe_cast(valid_from_time as timestamp)                      as valid_from,
        safe_cast(valid_to_time as timestamp)                        as valid_to
    from source

),

cleaned as (

    select
        *,
        valid_to = timestamp '9999-01-01 00:00:00+00'                as is_current,
        price_raw <= 1                                               as is_price_on_request,
        {{ is_placeholder_price('price_raw') }}                      as is_placeholder_price,
        {{ clean_price('price_raw') }}                               as price,
        case
            when first_registration_year_raw
                 between 1900 and extract(year from current_date()) + 1
            then first_registration_year_raw
        end                                                          as first_registration_year,
        case
            when mileage_raw between 0 and {{ var('max_valid_mileage') }}
            then mileage_raw
        end                                                          as mileage
    from typed
    where valid_to > valid_from          -- drop zero-length technical versions

)

select
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'valid_from']) }} as listing_version_id,
    listing_id,
    valid_from,
    valid_to,
    date(valid_from)                  as valid_from_date,
    is_current,
    status,
    platform,
    vehicle_category,
    seller_type,
    condition_type,
    make_key,
    model_key,
    version_full_name,
    body_type,
    body_color,
    fuel_type,
    transmission_type,
    cubic_capacity,
    first_registration_year,
    mileage,
    price,
    number_of_images,
    created_date,
    activation_date,
    is_price_on_request,
    is_placeholder_price,
    price is null                     as is_price_invalid,
    price_raw,
    mileage_raw,
    first_registration_year_raw
from cleaned
-- safety net: keep one row per version key even if the source starts sending true duplicates
qualify row_number() over (partition by listing_id, valid_from order by valid_to desc) = 1
