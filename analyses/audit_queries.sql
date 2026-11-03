-- ============================================================================
-- SMG LISTINGS DWH — FIVE BIGQUERY AUDIT TESTS
-- Project: smg-ch-automotive-interview
-- Dataset: vladimir_dimitric_{staging,intermediate,marts}
--
-- Run this whole file in BigQuery / DataGrip (standard SQL).
-- It contains FIVE independent SELECT statements and therefore returns five
-- result sets. Each result set contains only failing rows; zero rows = PASS.
--
-- Test 1: Average price drop per model (event + per-listing metrics)
-- Test 2: Days on platform (recalculate from dim_listing)
-- Test 3: Overpriced listings (recalculate peer medians and classification)
-- Test 4: Final notification listing exists + saved-search criteria verified
--         against staging data at the individual price-drop timestamp
-- Test 5: Notification candidates, cooldown, digest cap and daily export
--
-- Notes:
-- * These are audit queries, not dbt tests. They do not modify any tables.
-- * The saved-search audit intentionally checks only filters supported by the
--   project: make, model, color, min/max price, and minimum registration year.
-- * horse_power_min is unsupported because listing data has no horsepower field.
-- ============================================================================

-- --------------------------------------------------------------------------
-- TEST 1 — rpt_avg_price_drop_per_model
-- Independently recalculate the event and per-listing metrics from the price
-- change fact, using the same valid-drop / car / minimum-sample-size rules.
-- --------------------------------------------------------------------------
WITH drops AS (
  SELECT *
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.fct_listing_price_changes`
  WHERE is_valid_drop
    AND vehicle_category = 'car'
    AND model_key IS NOT NULL
),
per_listing AS (
  SELECT
    make_key, model_key, listing_id,
    MAX(running_peak - new_price)                           AS max_drawdown_chf,
    MAX(SAFE_DIVIDE(running_peak - new_price, running_peak)) AS max_drawdown_pct
  FROM (
    SELECT *,
           MAX(old_price) OVER (PARTITION BY listing_id ORDER BY changed_at
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_peak
    FROM drops
  )
  GROUP BY make_key, model_key, listing_id
),
listing_agg AS (
  SELECT
    make_key, model_key,
    COUNT(*)              AS n_listings_with_drop,
    AVG(max_drawdown_chf) AS avg_total_drop_per_listing_chf,
    AVG(max_drawdown_pct) AS avg_total_drop_per_listing_pct
  FROM per_listing
  GROUP BY make_key, model_key
),
event_agg AS (
  SELECT
    make_key,
    model_key,
    COUNT(*) AS n_drop_events,
    AVG(drop_amount) AS avg_drop_chf,
    APPROX_QUANTILES(drop_amount, 2)[SAFE_OFFSET(1)] AS median_drop_chf,
    AVG(drop_pct) AS avg_drop_pct,
    AVG(old_price) AS avg_price_before_drop_chf
  FROM drops
  GROUP BY make_key, model_key
  HAVING COUNT(*) >= 20
),
expected AS (
  SELECT
    e.make_key,
    e.model_key,
    e.n_drop_events,
    l.n_listings_with_drop,
    ROUND(e.avg_drop_chf, 0) AS avg_drop_chf,
    ROUND(e.median_drop_chf, 0) AS median_drop_chf,
    ROUND(e.avg_drop_pct * 100, 2) AS avg_drop_pct,
    ROUND(e.avg_price_before_drop_chf, 0) AS avg_price_before_drop_chf,
    ROUND(l.avg_total_drop_per_listing_chf, 0) AS avg_total_drop_per_listing_chf,
    ROUND(l.avg_total_drop_per_listing_pct * 100, 2) AS avg_total_drop_per_listing_pct
  FROM event_agg e
  JOIN listing_agg l USING (make_key, model_key)
),
comparison AS (
  SELECT
    COALESCE(r.make_key, e.make_key) AS make_key,
    COALESCE(r.model_key, e.model_key) AS model_key,
    r.n_drop_events AS report_n_drop_events,
    e.n_drop_events AS expected_n_drop_events,
    r.n_listings_with_drop AS report_n_listings_with_drop,
    e.n_listings_with_drop AS expected_n_listings_with_drop,
    r.avg_drop_chf AS report_avg_drop_chf,
    e.avg_drop_chf AS expected_avg_drop_chf,
    r.median_drop_chf AS report_median_drop_chf,
    e.median_drop_chf AS expected_median_drop_chf,
    r.avg_drop_pct AS report_avg_drop_pct,
    e.avg_drop_pct AS expected_avg_drop_pct,
    r.avg_price_before_drop_chf AS report_avg_price_before_drop_chf,
    e.avg_price_before_drop_chf AS expected_avg_price_before_drop_chf,
    r.avg_total_drop_per_listing_chf AS report_avg_total_drop_per_listing_chf,
    e.avg_total_drop_per_listing_chf AS expected_avg_total_drop_per_listing_chf,
    r.avg_total_drop_per_listing_pct AS report_avg_total_drop_per_listing_pct,
    e.avg_total_drop_per_listing_pct AS expected_avg_total_drop_per_listing_pct
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.rpt_avg_price_drop_per_model` r
  FULL OUTER JOIN expected e USING (make_key, model_key)
)
SELECT *,
  CASE
    WHEN report_n_drop_events IS NULL THEN 'MISSING_FROM_REPORT'
    WHEN expected_n_drop_events IS NULL THEN 'EXTRA_IN_REPORT'
    ELSE 'METRIC_MISMATCH'
  END AS audit_status
FROM comparison
WHERE report_n_drop_events IS NULL
   OR expected_n_drop_events IS NULL
   OR report_n_drop_events IS DISTINCT FROM expected_n_drop_events
   OR report_n_listings_with_drop IS DISTINCT FROM expected_n_listings_with_drop
   OR report_avg_drop_chf IS DISTINCT FROM expected_avg_drop_chf
   OR report_median_drop_chf IS DISTINCT FROM expected_median_drop_chf
   OR report_avg_drop_pct IS DISTINCT FROM expected_avg_drop_pct
   OR report_avg_price_before_drop_chf IS DISTINCT FROM expected_avg_price_before_drop_chf
   OR report_avg_total_drop_per_listing_chf IS DISTINCT FROM expected_avg_total_drop_per_listing_chf
   OR report_avg_total_drop_per_listing_pct IS DISTINCT FROM expected_avg_total_drop_per_listing_pct
ORDER BY make_key, model_key;


-- --------------------------------------------------------------------------
-- TEST 2 — rpt_days_on_platform
-- Recalculate all four grouping levels from dim_listing. Median uses the same
-- APPROX_QUANTILES(..., 2 IGNORE NULLS) definition as the dbt model.
-- --------------------------------------------------------------------------
WITH cars AS (
  SELECT *
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.dim_listing`
  WHERE vehicle_category = 'car'
    AND was_ever_activated
),
expected_all AS (
  SELECT
    CASE
      WHEN GROUPING(make_key) = 0 AND GROUPING(model_key) = 0 THEN 'make_model'
      WHEN GROUPING(make_key) = 0 THEN 'make'
      WHEN GROUPING(seller_type) = 0 THEN 'seller_type'
      ELSE 'all_cars'
    END AS grouping_level,
    seller_type,
    make_key,
    model_key,
    COUNT(*) AS n_listings,
    COUNTIF(NOT is_active_at_snapshot) AS n_completed,
    ROUND(AVG(IF(NOT is_active_at_snapshot, days_active, NULL)), 1) AS avg_days_completed,
    ROUND(APPROX_QUANTILES(IF(NOT is_active_at_snapshot, days_active, NULL), 2 IGNORE NULLS)[SAFE_OFFSET(1)], 1)
      AS median_days_completed,
    ROUND(AVG(days_active), 1) AS avg_days_all,
    ROUND(AVG(activation_episodes), 2) AS avg_activation_episodes
  FROM cars
  GROUP BY GROUPING SETS ((), (seller_type), (make_key), (make_key, model_key))
),
expected AS (
  SELECT *
  FROM expected_all
  WHERE grouping_level IN ('all_cars', 'seller_type')
     OR n_listings >= 20
),
comparison AS (
  SELECT
    COALESCE(r.grouping_level, e.grouping_level) AS grouping_level,
    COALESCE(r.seller_type, e.seller_type) AS seller_type,
    COALESCE(r.make_key, e.make_key) AS make_key,
    COALESCE(r.model_key, e.model_key) AS model_key,
    r.n_listings AS report_n_listings,
    e.n_listings AS expected_n_listings,
    r.n_completed AS report_n_completed,
    e.n_completed AS expected_n_completed,
    r.avg_days_completed AS report_avg_days_completed,
    e.avg_days_completed AS expected_avg_days_completed,
    r.median_days_completed AS report_median_days_completed,
    e.median_days_completed AS expected_median_days_completed,
    r.avg_days_all AS report_avg_days_all,
    e.avg_days_all AS expected_avg_days_all,
    r.avg_activation_episodes AS report_avg_activation_episodes,
    e.avg_activation_episodes AS expected_avg_activation_episodes
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.rpt_days_on_platform` r
  FULL OUTER JOIN expected e
    ON r.grouping_level = e.grouping_level
   AND r.seller_type IS NOT DISTINCT FROM e.seller_type
   AND r.make_key IS NOT DISTINCT FROM e.make_key
   AND r.model_key IS NOT DISTINCT FROM e.model_key
)
SELECT *,
  CASE
    WHEN report_n_listings IS NULL THEN 'MISSING_FROM_REPORT'
    WHEN expected_n_listings IS NULL THEN 'EXTRA_IN_REPORT'
    ELSE 'METRIC_MISMATCH'
  END AS audit_status
FROM comparison
WHERE report_n_listings IS NULL
   OR expected_n_listings IS NULL
   OR report_n_listings IS DISTINCT FROM expected_n_listings
   OR report_n_completed IS DISTINCT FROM expected_n_completed
   OR report_avg_days_completed IS DISTINCT FROM expected_avg_days_completed
   OR report_median_days_completed IS DISTINCT FROM expected_median_days_completed
   OR report_avg_days_all IS DISTINCT FROM expected_avg_days_all
   OR report_avg_activation_episodes IS DISTINCT FROM expected_avg_activation_episodes
ORDER BY grouping_level, make_key, model_key, seller_type;


-- --------------------------------------------------------------------------
-- TEST 3 — rpt_overpriced_listings
-- Recreate peer groups from live cars, then independently identify cars more
-- than 25% above peer median. Compare IDs, peer counts, medians and stale flag.
-- --------------------------------------------------------------------------
WITH live AS (
  SELECT
    *,
    CASE
      WHEN mileage IS NULL THEN 'unknown'
      WHEN mileage < 10000 THEN '0-10k'
      WHEN mileage < 30000 THEN '10-30k'
      WHEN mileage < 60000 THEN '30-60k'
      WHEN mileage < 100000 THEN '60-100k'
      WHEN mileage < 150000 THEN '100-150k'
      ELSE '150k+'
    END AS mileage_band
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.dim_listing`
  WHERE vehicle_category = 'car'
    AND is_active_at_snapshot
    AND current_price IS NOT NULL
    AND model_key IS NOT NULL
    AND first_registration_year IS NOT NULL
),
peers AS (
  SELECT
    make_key, model_key, first_registration_year, condition_type, mileage_band,
    COUNT(*) AS n_peers,
    APPROX_QUANTILES(current_price, 100)[OFFSET(25)] AS peer_p25_price,
    APPROX_QUANTILES(current_price, 100)[OFFSET(50)] AS peer_median_price,
    APPROX_QUANTILES(current_price, 100)[OFFSET(75)] AS peer_p75_price
  FROM live
  GROUP BY make_key, model_key, first_registration_year, condition_type, mileage_band
  HAVING COUNT(*) >= 10
),
expected AS (
  SELECT
    l.listing_id,
    p.peer_median_price,
    p.peer_p25_price,
    p.peer_p75_price,
    p.n_peers,
    ROUND(SAFE_DIVIDE(l.current_price, p.peer_median_price) - 1, 4) AS price_premium_pct,
    l.current_price - p.peer_median_price AS price_premium_chf,
    l.days_active,
    l.n_valid_price_drops,
    l.days_active > 60 AND l.n_valid_price_drops = 0 AS is_stale
  FROM live l
  JOIN peers p USING (make_key, model_key, first_registration_year, condition_type, mileage_band)
  WHERE l.current_price > p.peer_median_price * 1.25
),
comparison AS (
  SELECT
    COALESCE(r.listing_id, e.listing_id) AS listing_id,
    r.peer_median_price AS report_peer_median_price,
    e.peer_median_price AS expected_peer_median_price,
    r.peer_p25_price AS report_peer_p25_price,
    e.peer_p25_price AS expected_peer_p25_price,
    r.peer_p75_price AS report_peer_p75_price,
    e.peer_p75_price AS expected_peer_p75_price,
    r.n_peers AS report_n_peers,
    e.n_peers AS expected_n_peers,
    r.price_premium_pct AS report_price_premium_pct,
    e.price_premium_pct AS expected_price_premium_pct,
    r.price_premium_chf AS report_price_premium_chf,
    e.price_premium_chf AS expected_price_premium_chf,
    r.days_active AS report_days_active,
    e.days_active AS expected_days_active,
    r.n_valid_price_drops AS report_n_valid_price_drops,
    e.n_valid_price_drops AS expected_n_valid_price_drops,
    r.is_stale AS report_is_stale,
    e.is_stale AS expected_is_stale
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.rpt_overpriced_listings` r
  FULL OUTER JOIN expected e USING (listing_id)
)
SELECT *,
  CASE
    WHEN report_peer_median_price IS NULL THEN 'MISSING_FROM_REPORT'
    WHEN expected_peer_median_price IS NULL THEN 'EXTRA_IN_REPORT'
    ELSE 'METRIC_MISMATCH'
  END AS audit_status
FROM comparison
WHERE report_peer_median_price IS NULL
   OR expected_peer_median_price IS NULL
   OR report_peer_median_price IS DISTINCT FROM expected_peer_median_price
   OR report_peer_p25_price IS DISTINCT FROM expected_peer_p25_price
   OR report_peer_p75_price IS DISTINCT FROM expected_peer_p75_price
   OR report_n_peers IS DISTINCT FROM expected_n_peers
   OR report_price_premium_pct IS DISTINCT FROM expected_price_premium_pct
   OR report_price_premium_chf IS DISTINCT FROM expected_price_premium_chf
   OR report_days_active IS DISTINCT FROM expected_days_active
   OR report_n_valid_price_drops IS DISTINCT FROM expected_n_valid_price_drops
   OR report_is_stale IS DISTINCT FROM expected_is_stale
ORDER BY listing_id;


-- --------------------------------------------------------------------------
-- TEST 4 — FINAL NOTIFICATION -> FINAL LISTING -> STAGING SEARCH MATCH
-- Uses the supplied staging DDL exactly:
--   stg_listings_versions: listing_id, valid_from, valid_to, is_current,
--     make_key, model_key, body_color, price, first_registration_year
--   stg_saved_searches: saved_search_id, user_id, created_at, deleted_at,
--     make_key, model_key, body_color, price_min, price_max,
--     first_registration_year_min
--
-- For each final notification, confirm:
-- 1) listing exists in final dim_listing;
-- 2) listing has a staging version valid at end of notification run_date;
-- 3) at least one saved search belonging to that user was active at that time
--    and its supported criteria match the staging listing.
--
-- This checks the listing/search relationship using the end-of-day version.
-- It does not replay the exact intraday price-drop event because the supplied
-- final notification DDL/event timestamp was not provided.

WITH notification_rows AS (
  SELECT
    run_date,
    user_id,
    listing_id
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.rpt_daily_price_drop_notifications`
),

final_listing_check AS (
  SELECT
    n.*,
    l.listing_id IS NOT NULL AS listing_exists_in_final
  FROM notification_rows n
  LEFT JOIN `smg-ch-automotive-interview.vladimir_dimitric_marts.dim_listing` l
    ON l.listing_id = n.listing_id
),

notification_as_of AS (
  SELECT
    *,
    TIMESTAMP(DATE_ADD(run_date, INTERVAL 1 DAY)) AS as_of_exclusive
  FROM final_listing_check
),

staging_listing AS (
  SELECT
    n.*,
    v.listing_id AS staging_listing_id,
    v.valid_from AS staging_valid_from,
    v.valid_to AS staging_valid_to,
    v.status AS staging_status,
    v.make_key AS listing_make_key,
    v.model_key AS listing_model_key,
    v.body_color AS listing_body_color,
    v.price AS listing_price,
    v.first_registration_year AS listing_first_registration_year
  FROM notification_as_of n
  LEFT JOIN `smg-ch-automotive-interview.vladimir_dimitric_staging.stg_listings_versions` v
    ON v.listing_id = n.listing_id
   AND v.valid_from < n.as_of_exclusive
   AND v.valid_to >= n.as_of_exclusive
),

search_evaluation AS (
  SELECT
    l.run_date,
    l.user_id,
    l.listing_id,
    l.listing_exists_in_final,
    l.staging_listing_id IS NOT NULL AS listing_exists_in_staging,
    l.staging_valid_from,
    l.staging_valid_to,
    l.staging_status,
    l.listing_make_key,
    l.listing_model_key,
    l.listing_body_color,
    l.listing_price,
    l.listing_first_registration_year,

    s.saved_search_id,
    s.user_id AS saved_search_user_id,
    s.created_at,
    s.deleted_at,
    s.make_key AS search_make_key,
    s.model_key AS search_model_key,
    s.body_color AS search_body_color,
    s.price_min,
    s.price_max,
    s.first_registration_year_min,

    (
      s.saved_search_id IS NOT NULL
      AND s.user_id = l.user_id
      AND s.created_at < l.as_of_exclusive
      AND (s.deleted_at IS NULL OR s.deleted_at >= l.as_of_exclusive)
      AND (s.make_key IS NULL OR l.listing_make_key = s.make_key)
      AND (s.model_key IS NULL OR l.listing_model_key = s.model_key)
      AND (s.body_color IS NULL OR l.listing_body_color = s.body_color)
      AND (s.price_min IS NULL OR l.listing_price >= s.price_min)
      AND (s.price_max IS NULL OR l.listing_price <= s.price_max)
      AND (
        s.first_registration_year_min IS NULL
        OR l.listing_first_registration_year >= s.first_registration_year_min
      )
    ) AS matches_saved_search

  FROM staging_listing l
  LEFT JOIN `smg-ch-automotive-interview.vladimir_dimitric_staging.stg_saved_searches` s
    ON s.user_id = l.user_id
),

per_notification AS (
  SELECT
    run_date,
    user_id,
    listing_id,
    listing_exists_in_final,
    listing_exists_in_staging,
    staging_valid_from,
    staging_valid_to,
    staging_status,
    listing_make_key,
    listing_model_key,
    listing_body_color,
    listing_price,
    listing_first_registration_year,
    COUNTIF(matches_saved_search) AS matching_saved_search_count,
    STRING_AGG(
      IF(matches_saved_search, CAST(saved_search_id AS STRING), NULL),
      ', ' ORDER BY saved_search_id
    ) AS matching_saved_search_ids
  FROM search_evaluation
  GROUP BY
    run_date,
    user_id,
    listing_id,
    listing_exists_in_final,
    listing_exists_in_staging,
    staging_valid_from,
    staging_valid_to,
    staging_status,
    listing_make_key,
    listing_model_key,
    listing_body_color,
    listing_price,
    listing_first_registration_year
)

SELECT
  *,
  CASE
    WHEN NOT listing_exists_in_final THEN 'LISTING_MISSING_FROM_FINAL'
    WHEN NOT listing_exists_in_staging THEN 'LISTING_MISSING_FROM_STAGING'
    WHEN matching_saved_search_count = 0 THEN 'NO_ACTIVE_SEARCH_MATCH'
    ELSE 'PASS'
  END AS audit_status
FROM per_notification
WHERE NOT listing_exists_in_final
   OR NOT listing_exists_in_staging
   OR matching_saved_search_count = 0
ORDER BY run_date, user_id, listing_id;


-- TEST 5 — NOTIFICATION CANDIDATES / COOLDOWN / DIGEST / EXPORT
-- Check candidate uniqueness, end-of-day price/status, cooldown flag logic,
-- digest cap, and that each selected candidate appears once in the daily export.
-- --------------------------------------------------------------------------
WITH candidates AS (
  SELECT *
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.fct_price_drop_notifications`
),
per_user_day AS (
  SELECT
    user_id,
    drop_date,
    COUNTIF(is_selected_for_digest) AS selected_count,
    COUNT(*) AS candidate_count
  FROM candidates
  GROUP BY user_id, drop_date
),
duplicate_candidates AS (
  SELECT user_id, listing_id, drop_date, COUNT(*) AS row_count
  FROM candidates
  GROUP BY user_id, listing_id, drop_date
  HAVING COUNT(*) != 1
),
export_duplicates AS (
  SELECT run_date, user_id, listing_id, COUNT(*) AS row_count
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.rpt_daily_price_drop_notifications`
  GROUP BY run_date, user_id, listing_id
  HAVING COUNT(*) != 1
),
selected_missing_from_export AS (
  SELECT
    c.drop_date,
    c.user_id,
    c.listing_id,
    c.digest_rank
  FROM candidates c
  LEFT JOIN `smg-ch-automotive-interview.vladimir_dimitric_marts.rpt_daily_price_drop_notifications` r
    ON r.run_date = c.drop_date
   AND r.user_id = c.user_id
   AND r.listing_id = c.listing_id
  WHERE c.is_selected_for_digest
    AND r.listing_id IS NULL
),
export_without_selected_candidate AS (
  SELECT
    r.run_date,
    r.user_id,
    r.listing_id
  FROM `smg-ch-automotive-interview.vladimir_dimitric_marts.rpt_daily_price_drop_notifications` r
  LEFT JOIN candidates c
    ON c.drop_date = r.run_date
   AND c.user_id = r.user_id
   AND c.listing_id = r.listing_id
   AND c.is_selected_for_digest
  WHERE c.listing_id IS NULL
),
violations AS (
  SELECT
    'DUPLICATE_CANDIDATE' AS audit_status,
    CAST(drop_date AS STRING) AS audit_date,
    user_id,
    listing_id,
    CAST(row_count AS STRING) AS detail
  FROM duplicate_candidates

  UNION ALL

  SELECT
    'DUPLICATE_EXPORT_ROW', CAST(run_date AS STRING), user_id, listing_id,
    CAST(row_count AS STRING)
  FROM export_duplicates

  UNION ALL

  SELECT
    'DIGEST_CAP_EXCEEDED', CAST(drop_date AS STRING), user_id, CAST(NULL AS INT64),
    CONCAT('selected_count=', CAST(selected_count AS STRING), '; max=5')
  FROM per_user_day
  WHERE selected_count > 5

  UNION ALL

  SELECT
    'SELECTED_ROW_SUPPRESSED_OR_UNRANKED', CAST(drop_date AS STRING), user_id, listing_id,
    CONCAT('suppressed=', CAST(is_suppressed_by_cooldown AS STRING), '; rank=', COALESCE(CAST(digest_rank AS STRING), 'NULL'))
  FROM candidates
  WHERE is_selected_for_digest
    AND (is_suppressed_by_cooldown OR digest_rank IS NULL OR digest_rank > 5)

  UNION ALL

  SELECT
    'COOLDOWN_FLAG_MISMATCH', CAST(drop_date AS STRING), user_id, listing_id,
    CONCAT('stored=', CAST(is_suppressed_by_cooldown AS STRING), '; expected=',
      CAST(COALESCE(price_now >= min_price_in_cooldown, FALSE) AS STRING))
  FROM candidates
  WHERE is_suppressed_by_cooldown IS DISTINCT FROM COALESCE(price_now >= min_price_in_cooldown, FALSE)

  UNION ALL

  SELECT
    'NOTIFICATION_PRICE_NOT_LOWER', CAST(drop_date AS STRING), user_id, listing_id,
    CONCAT('before=', CAST(price_before AS STRING), '; now=', CAST(price_now AS STRING))
  FROM candidates
  WHERE is_selected_for_digest
    AND NOT (price_now < price_before)

  UNION ALL

  SELECT
    'SELECTED_LISTING_NOT_ACTIVATED_AT_EOD', CAST(c.drop_date AS STRING), c.user_id, c.listing_id,
    CONCAT('end_of_day_status=', COALESCE(v.status, 'NO_VERSION'))
  FROM candidates c
  LEFT JOIN `smg-ch-automotive-interview.vladimir_dimitric_staging.stg_listings_versions` v
    ON v.listing_id = c.listing_id
   AND v.valid_from <= TIMESTAMP_SUB(TIMESTAMP(DATE_ADD(c.drop_date, INTERVAL 1 DAY)), INTERVAL 1 MICROSECOND)
   AND v.valid_to > TIMESTAMP_SUB(TIMESTAMP(DATE_ADD(c.drop_date, INTERVAL 1 DAY)), INTERVAL 1 MICROSECOND)
  WHERE c.is_selected_for_digest
    AND (v.listing_id IS NULL OR v.status != 'activated')

  UNION ALL

  SELECT
    'SELECTED_ROW_MISSING_FROM_EXPORT', CAST(drop_date AS STRING), user_id, listing_id,
    CONCAT('digest_rank=', COALESCE(CAST(digest_rank AS STRING), 'NULL'))
  FROM selected_missing_from_export

  UNION ALL

  SELECT
    'EXPORT_ROW_NOT_SELECTED', CAST(run_date AS STRING), user_id, listing_id,
    'No selected candidate exists'
  FROM export_without_selected_candidate
)
SELECT *
FROM violations
ORDER BY audit_status, audit_date, user_id, listing_id;

-- END OF FIVE AUDIT TESTS