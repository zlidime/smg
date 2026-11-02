-- A. Duplicate (listing_id, valid_from) rows: which columns differ?
WITH d AS (
  SELECT *, COUNT(*) OVER (PARTITION BY listing_id, valid_from_time) AS n
  FROM {{ source('raw', 'listings') }})
SELECT listing_id, valid_from_time, valid_to_time, status, price, mileage,
       number_of_images, body_color, model_key, version_full_name
FROM d WHERE n > 1
ORDER BY listing_id, valid_from_time, valid_to_time
LIMIT 20;

-- A2. Summary: per duplicate pair, which columns differ
WITH d AS (
  SELECT *, COUNT(*) OVER (PARTITION BY listing_id, valid_from_time) AS n
  FROM {{ source('raw', 'listings') }}),
g AS (
  SELECT listing_id, valid_from_time,
    COUNT(DISTINCT valid_to_time)     > 1 AS diff_valid_to,
    COUNT(DISTINCT status)            > 1 AS diff_status,
    COUNT(DISTINCT price)             > 1 AS diff_price,
    COUNT(DISTINCT mileage)           > 1 AS diff_mileage,
    COUNT(DISTINCT number_of_images)  > 1 AS diff_images,
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(make_key, model_key, version_full_name, body_color, fuel_type))) > 1 AS diff_vehicle
  FROM d WHERE n > 1
  GROUP BY 1, 2)
SELECT COUNT(*) AS dup_keys,
       COUNTIF(diff_valid_to) AS diff_valid_to,
       COUNTIF(diff_status)   AS diff_status,
       COUNTIF(diff_price)    AS diff_price,
       COUNTIF(diff_mileage)  AS diff_mileage,
       COUNTIF(diff_images)   AS diff_images,
       COUNTIF(diff_vehicle)  AS diff_vehicle
FROM g;

-- B. Sample of >50% drops: typo fix or real drop?
WITH x AS (
  SELECT listing_id, make_key, model_key, first_registration_year, status,
         SAFE_CAST(price AS NUMERIC) AS price,
         LAG(SAFE_CAST(price AS NUMERIC))  OVER w AS prev_price,
         LEAD(SAFE_CAST(price AS NUMERIC)) OVER w AS next_price,
         valid_from_time
  FROM {{ source('raw', 'listings') }}
  WINDOW w AS (PARTITION BY listing_id ORDER BY valid_from_time))
SELECT * FROM x
WHERE prev_price > 1 AND price > 1
  AND SAFE_DIVIDE(prev_price - price, prev_price) > 0.5
ORDER BY FARM_FINGERPRINT(CAST(listing_id AS STRING))
LIMIT 30;

-- B2. Summary: how many >50% drops bounce back (typo pattern)?
WITH x AS (
  SELECT SAFE_CAST(price AS NUMERIC) AS price,
         LAG(SAFE_CAST(price AS NUMERIC))  OVER w AS prev_price,
         LEAD(SAFE_CAST(price AS NUMERIC)) OVER w AS next_price
  FROM {{ source('raw', 'listings') }}
  WINDOW w AS (PARTITION BY listing_id ORDER BY valid_from_time))
SELECT
  COUNT(*)                                                       AS big_drops,
  COUNTIF(next_price IS NOT NULL AND next_price >= prev_price * 0.9) AS bounced_back,
  COUNTIF(ROUND(prev_price / NULLIF(price, 0)) IN (10, 100, 1000))   AS factor_10x,
  COUNTIF(next_price IS NULL OR next_price < prev_price * 0.9)       AS stayed_low
FROM x
WHERE prev_price > 1 AND price > 1
  AND SAFE_DIVIDE(prev_price - price, prev_price) > 0.5;
