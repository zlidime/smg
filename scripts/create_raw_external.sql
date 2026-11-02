-- Raw listings = external table over the GCS landing zone (no duplicate storage).
-- 80 gzip CSV parts from a BigQuery export; every part has a header -> skip_leading_rows = 1.
-- All columns STRING: typing and cleaning happen in staging.
-- Swap the project/dataset for the target environment.

CREATE OR REPLACE EXTERNAL TABLE `dime-smg-task.raw.listings` (
  created_date STRING, activation_date STRING, seller_type STRING, make_key STRING,
  model_key STRING, price STRING, first_registration_year STRING, mileage STRING,
  fuel_type STRING, condition_type STRING, body_color STRING, valid_from_time STRING,
  valid_to_time STRING, listing_id STRING, body_type STRING, cubic_capacity STRING,
  transmission_type_group STRING, vehicle_category STRING, status STRING,
  version_full_name STRING, brand STRING, number_of_images STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://listings_interview_source/ds_interview_listings_*'],
  compression = 'GZIP',
  skip_leading_rows = 1
);

-- check: expect headers = 0, n = 15294784
-- SELECT COUNTIF(status = 'status') AS headers, COUNT(*) AS n FROM `dime-smg-task.raw.listings`;
