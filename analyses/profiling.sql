-- =====================================================================
-- Data profiling: raw.listings + raw.users_saved_searches (BigQuery)
-- Put in dbt/analyses/profiling.sql. Run section by section.
-- Each finding -> a staging rule -> a dbt test.
-- =====================================================================

-- 0. Typed view used by most checks below
CREATE TEMP TABLE l AS
SELECT
  listing_id,
  status,
  vehicle_category,
  seller_type,
  condition_type,
  make_key,
  NULLIF(model_key, '')                               AS model_key,
  body_color,
  SAFE_CAST(price AS NUMERIC)                         AS price,
  SAFE_CAST(mileage AS INT64)                         AS mileage,
  SAFE_CAST(first_registration_year AS INT64)         AS reg_year,
  SAFE_CAST(created_date AS DATE)                     AS created_date,
  SAFE_CAST(activation_date AS DATE)                  AS activation_date,
  SAFE_CAST(valid_from_time AS TIMESTAMP)             AS valid_from,
  SAFE_CAST(valid_to_time AS TIMESTAMP)               AS valid_to,
  price AS price_raw, mileage AS mileage_raw,
  valid_from_time AS vf_raw, valid_to_time AS vt_raw
FROM {{ source('raw', 'listings') }};

-- 1. Volume and grain ------------------------------------------------
SELECT COUNT(*) AS row_cnt,
       COUNT(DISTINCT listing_id) AS listing_cnt,
       ROUND(COUNT(*) / COUNT(DISTINCT listing_id), 2) AS versions_per_listing,
       MIN(valid_from) AS min_vf, MAX(valid_from) AS max_vf,
       MIN(created_date) AS min_created, MAX(created_date) AS max_created
FROM l;

-- versions per listing distribution
SELECT n_versions, COUNT(*) AS listings
FROM (SELECT listing_id, COUNT(*) n_versions FROM l GROUP BY 1)
GROUP BY 1 ORDER BY 1;

-- 2. Parse failures (type contract for staging) ----------------------
SELECT
  COUNTIF(price IS NULL AND price_raw IS NOT NULL)       AS bad_price,
  COUNTIF(mileage IS NULL AND mileage_raw IS NOT NULL)   AS bad_mileage,
  COUNTIF(valid_from IS NULL)                            AS bad_valid_from,
  COUNTIF(valid_to IS NULL)                              AS bad_valid_to
FROM l;

-- 3. Null / empty profile per column ---------------------------------
SELECT
  COUNTIF(model_key IS NULL)                         AS null_model,
  COUNTIF(NULLIF(fuel_type,'') IS NULL)              AS null_fuel,
  COUNTIF(NULLIF(transmission_type_group,'') IS NULL) AS null_transmission,
  COUNTIF(NULLIF(cubic_capacity,'') IS NULL)         AS null_cc,
  COUNTIF(NULLIF(mileage,'') IS NULL)                AS null_mileage,
  COUNTIF(NULLIF(version_full_name,'') IS NULL)      AS null_version
FROM {{ source('raw', 'listings') }};

-- 4. Domain values (-> accepted_values tests) ------------------------
SELECT 'status' col, status v, COUNT(*) n FROM l GROUP BY 1,2
UNION ALL SELECT 'vehicle_category', vehicle_category, COUNT(*) FROM l GROUP BY 1,2
UNION ALL SELECT 'seller_type', seller_type, COUNT(*) FROM l GROUP BY 1,2
UNION ALL SELECT 'condition_type', condition_type, COUNT(*) FROM l GROUP BY 1,2
UNION ALL SELECT 'body_color', body_color, COUNT(*) FROM l GROUP BY 1,2
ORDER BY col, n DESC;

-- 5. Outliers / sentinel values --------------------------------------
SELECT
  COUNTIF(price <= 1)                                 AS price_placeholder,   -- placeholder (95% drafts, see README 3.3)
  COUNTIF(price BETWEEN 2 AND 99)                     AS price_suspicious_low,
  COUNTIF(price >= 2000000)                           AS price_suspicious_high,
  COUNTIF(price IN (1111111,7777777,9999999,1234567)) AS price_pattern,
  COUNTIF(reg_year < 1900 OR reg_year > EXTRACT(YEAR FROM CURRENT_DATE()) + 1) AS bad_reg_year,
  COUNTIF(mileage > 1000000)                          AS mileage_over_1m,
  COUNTIF(condition_type = 'new' AND mileage > 1000)  AS new_with_mileage,
  COUNTIF(condition_type = 'used' AND mileage = 0)    AS used_zero_mileage
FROM l;

SELECT APPROX_QUANTILES(price, 100)[OFFSET(1)]  AS p01,
       APPROX_QUANTILES(price, 100)[OFFSET(50)] AS p50,
       APPROX_QUANTILES(price, 100)[OFFSET(99)] AS p99
FROM l WHERE price > 1;

-- 6. SCD2 integrity ---------------------------------------------------
-- 6a. duplicate version key
SELECT listing_id, valid_from, COUNT(*) n
FROM l GROUP BY 1,2 HAVING n > 1;

-- 6b. full-row duplicates
SELECT COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(t)) AS exact_dup_rows
FROM {{ source('raw', 'listings') }} t;

-- 6c. validity window sanity + one current row per listing
SELECT
  COUNTIF(valid_to < valid_from)                    AS negative_window,
  COUNTIF(valid_to = TIMESTAMP '9999-01-01')        AS current_rows
FROM l;

SELECT listing_id, COUNT(*) n_current
FROM l WHERE valid_to = TIMESTAMP '9999-01-01'
GROUP BY 1 HAVING n_current > 1;

-- 6d. gaps / overlaps between consecutive versions
WITH x AS (
  SELECT listing_id, valid_from, valid_to,
         LAG(valid_to) OVER (PARTITION BY listing_id ORDER BY valid_from) AS prev_to
  FROM l)
SELECT
  COUNTIF(prev_to IS NOT NULL)                  AS transitions,
  COUNTIF(prev_to = valid_from)                 AS contiguous,
  COUNTIF(prev_to < valid_from)                 AS gaps,
  COUNTIF(prev_to > valid_from)                 AS overlaps
FROM x;

-- 6e. listing history ending without a current row (closed listings?)
SELECT COUNT(*) AS listings_without_current
FROM (SELECT listing_id, MAX(valid_to) mx FROM l GROUP BY 1)
WHERE mx < TIMESTAMP '9999-01-01';

-- 7. Change behaviour (what triggers a new version) ------------------
WITH x AS (
  SELECT *,
    LAG(price)  OVER w AS prev_price,
    LAG(status) OVER w AS prev_status
  FROM l WINDOW w AS (PARTITION BY listing_id ORDER BY valid_from))
SELECT
  COUNTIF(prev_price IS NOT NULL AND price < prev_price) AS price_drops,
  COUNTIF(prev_price IS NOT NULL AND price > prev_price) AS price_increases,
  COUNTIF(prev_price = price AND prev_status = status)   AS other_attr_change,
  COUNTIF(prev_status != status)                          AS status_changes
FROM x;

-- status transition matrix
WITH x AS (
  SELECT status, LAG(status) OVER (PARTITION BY listing_id ORDER BY valid_from) prev_status FROM l)
SELECT prev_status, status, COUNT(*) n
FROM x WHERE prev_status IS NOT NULL
GROUP BY 1,2 ORDER BY n DESC;

-- drop size distribution (spot absurd drops e.g. 25000 -> 1)
WITH x AS (
  SELECT price, LAG(price) OVER (PARTITION BY listing_id ORDER BY valid_from) prev_price
  FROM l WHERE status = 'activated')
SELECT
  COUNTIF(price < prev_price)                                   AS drops,
  COUNTIF(price < prev_price AND price <= 1)                    AS drop_to_placeholder,
  COUNTIF(price < prev_price AND SAFE_DIVIDE(prev_price-price, prev_price) > 0.5) AS drop_over_50pct,
  APPROX_QUANTILES(SAFE_DIVIDE(prev_price-price, prev_price), 10) AS drop_pct_deciles
FROM x WHERE prev_price > 1;

-- 8. Date consistency ------------------------------------------------
SELECT
  COUNTIF(activation_date < created_date)          AS activated_before_created,
  COUNTIF(DATE(valid_from) < created_date)         AS version_before_created
FROM l;

-- 9. Saved searches --------------------------------------------------
-- raw line check: blank lines, column count
SELECT raw_line,
       TRIM(raw_line) = ''                                       AS is_blank,
       SAFE.PARSE_JSON(REGEXP_EXTRACT(raw_line, r'^(?:[^,]*,){4}(.*)$')) IS NULL AS bad_json
FROM {{ source('raw', 'users_saved_searches') }};

-- which filter keys exist (-> which ones we can actually match on)
SELECT key, COUNT(*) n
FROM {{ source('raw', 'users_saved_searches') }},
UNNEST(JSON_KEYS(SAFE.PARSE_JSON(REGEXP_EXTRACT(raw_line, r'^(?:[^,]*,){4}(.*)$')))) AS key
WHERE TRIM(raw_line) <> ''
GROUP BY 1 ORDER BY n DESC;

-- 10. Cross-file coverage: do saved-search values exist in listings? --
WITH ss AS (
  SELECT
    JSON_VALUE(p,'$.make_key')  AS make_key,
    JSON_VALUE(p,'$.model_key') AS model_key,
    JSON_VALUE(p,'$.color')     AS color
  FROM (SELECT SAFE.PARSE_JSON(REGEXP_EXTRACT(raw_line, r'^(?:[^,]*,){4}(.*)$')) p
        FROM {{ source('raw', 'users_saved_searches') }} WHERE TRIM(raw_line) <> ''))
SELECT ss.make_key, ss.model_key, ss.color,
       (SELECT COUNT(DISTINCT listing_id) FROM l
         WHERE (ss.make_key  IS NULL OR l.make_key  = ss.make_key)
           AND (ss.model_key IS NULL OR l.model_key = ss.model_key)
           AND (ss.color     IS NULL OR l.body_color = ss.color)) AS matching_listings
FROM ss;
