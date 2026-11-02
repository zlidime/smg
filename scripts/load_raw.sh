#!/usr/bin/env bash
# Raw layer: load source files from GCS into BigQuery 1:1, every column STRING.
# Idempotent (--replace). Usage:  PROJECT=dime-smg-task DATASET=raw ./scripts/load_raw.sh
set -euo pipefail

PROJECT="${PROJECT:-dime-smg-task}"
DATASET="${DATASET:-raw}"
LOCATION="${LOCATION:-EU}"
LISTINGS_URI="${LISTINGS_URI:-gs://listings_interview_source/ds_interview_listings_*}"
SEARCHES_URI="${SEARCHES_URI:-gs://interview_source_csv/users_and_saved_searches.csv}"

LISTINGS_SCHEMA="created_date:STRING,activation_date:STRING,seller_type:STRING,make_key:STRING,model_key:STRING,price:STRING,first_registration_year:STRING,mileage:STRING,fuel_type:STRING,condition_type:STRING,body_color:STRING,valid_from_time:STRING,valid_to_time:STRING,listing_id:STRING,body_type:STRING,cubic_capacity:STRING,transmission_type_group:STRING,vehicle_category:STRING,status:STRING,version_full_name:STRING,brand:STRING,number_of_images:STRING"

bq --project_id="$PROJECT" show "$PROJECT:$DATASET" >/dev/null 2>&1 \
  || bq --project_id="$PROJECT" mk --dataset --location="$LOCATION" "$PROJECT:$DATASET"

# Listings: gzip CSV export split in parts -> wildcard
bq --project_id="$PROJECT" load --source_format=CSV --skip_leading_rows=1 --replace \
  "$PROJECT:$DATASET.listings" "$LISTINGS_URI" "$LISTINGS_SCHEMA"

# Saved searches: JSON column is unquoted and contains commas -> load each line whole
# (tab delimiter does not occur in the file, quoting disabled), parse in staging.
bq --project_id="$PROJECT" load --source_format=CSV --skip_leading_rows=1 --replace \
  --field_delimiter=$'\t' --quote="" \
  "$PROJECT:$DATASET.users_saved_searches" "$SEARCHES_URI" "raw_line:STRING"

bq --project_id="$PROJECT" query --use_legacy_sql=false --format=pretty "
select 'listings' as tbl, count(*) as n from \`$PROJECT.$DATASET.listings\`
union all
select 'users_saved_searches', count(*) from \`$PROJECT.$DATASET.users_saved_searches\`"
