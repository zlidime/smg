select
    saved_search_id,
    user_id,
    created_at,
    deleted_at,
    deleted_at is null                                as is_active,
    make_key,
    model_key,
    body_color,
    price_min,
    price_max,
    first_registration_year_min,
    horse_power_min,
    unsupported_filters,
    unsupported_filters != ''                         as has_unsupported_filters,
    -- human-readable label for the notification message, e.g. "bmw m3, white, 10000-20000 CHF"
    array_to_string(array(
        select x from unnest([
            trim(concat(coalesce(make_key, ''), ' ', coalesce(model_key, ''))),
            body_color,
            case
                when price_min is not null and price_max is not null
                    then concat(format("%'d", cast(price_min as int64)), '-',
                                format("%'d", cast(price_max as int64)), ' CHF')
                when price_min is not null then concat('from ', format("%'d", cast(price_min as int64)), ' CHF')
                when price_max is not null then concat('up to ', format("%'d", cast(price_max as int64)), ' CHF')
            end,
            if(first_registration_year_min is not null,
               concat('registered ', cast(first_registration_year_min as string), '+'), null)
        ]) as x
        where x is not null and x != ''
    ), ', ')                                          as search_label,
    search_params_json
from {{ ref('stg_saved_searches') }}
