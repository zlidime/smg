# SMG Listings DWH – price-drop notifications

dbt + BigQuery solution for the Data Engineer homework: a raw → staging → intermediate → marts DWH on
AutoScout24 / MotoScout24 listing history and users' saved searches. It produces a daily
price-drop notification export to GCS and three analytics reports.

---

## 1. Approach

1. **Profile first** (`analyses/profiling.sql`, `analyses/followup.sql`). Every cleaning rule below
   is based on a number from these queries, not on an assumption.
2. **Raw = untouched.** Files are loaded 1:1 with every column as `STRING`, so a bad value can never break
   ingestion. Typing and cleaning happen in staging.
3. **Flag, don't delete.** Invalid values become `NULL` in the clean column, with the original kept
   in `*_raw` plus a flag. Rejected price changes stay in the fact table with a `quality_issue`.
4. **One definition of a "real price drop"** (`is_valid_drop`). Notifications and analytics both use it,
   so they always agree.
5. **Tests encode the findings.** Source tests only warn (dirty input is expected). Staging and mart
   tests fail the build (the output contract must hold).

## 2. What the data is

| File | Content | Grain |
|---|---|---|
| `gs://listings_interview_source/ds_interview_listings_*` (gzip CSV, BigQuery export parts) | **SCD Type 2** history of listings (ads). A new version opens on any change (price, status, images, …). `valid_to = 9999-01-01` is the current version. | listing version. 15.3M rows, 1.52M listings, ~10 versions each |
| `gs://interview_source_csv/users_and_saved_searches.csv` | Saved searches with filters in JSON | saved search. 15 rows, 5 users |

Listing status lifecycle seen in the data: `draft → activated ⇄ deactivated → removed → deleted`, plus
**336k `removed → activated` relists**.

## 3. Data-quality findings → decisions

| Finding (profiling) | Volume | Decision | Where |
|---|---|---|---|
| Duplicate `(listing_id, valid_from)`. Always a `removed` row with `valid_from = valid_to`, plus `deleted`, written by the ~23:45 nightly batch | 18,101 keys | Drop **zero-length versions** (never live). This also removes the 8,997 window overlaps | `stg_listings__versions` |
| `price = 1` = "price on request" | 121k rows | `is_price_on_request`, clean `price` = NULL | staging |
| Prices typed in thousands (210,000 → **194.9**), < 500 CHF | 11k+ | price floor `min_valid_price = 500` | staging + price changes |
| Placeholder prices 11111 / 55555 / 123456 / 9999999 | ~1k | `is_placeholder_price` macro (repeated digits / sequences ≥ 10k) | `macros/price_rules.sql` |
| Drops > 50%: mostly typo corrections (383,600 → 38,300) | 9,793 | `drop_exceeds_threshold`, not a valid drop | `int_listing_price_changes` |
| Drop immediately reverted (181,900 → 18,911 → 180,911) | 1,218 of the big drops | `reverted_next_change`, not a valid drop | same |
| Price edits in `draft` / `deactivated` (40,000 → 41 in draft) | – | only consecutive **activated** versions are compared | same |
| Registration year 1880 / 2923 | 592 | NULL outside 1900..current+1 | staging |
| Mileage 99,999,999 / > 1M km | 7.5k | NULL | staging |
| 116k `used` cars with 0 km, 16.5k `new` with > 1,000 km | – | mileage not reliable → only coarse bands, never a hard filter | overpriced report |
| ~50% of new versions change neither price nor status (images, nightly batch) | 6.9M | price changes = rows where price actually differs | price changes |
| Saved-search CSV: JSON column **unquoted** with commas + an empty line | – | load the whole line as one STRING, split the first 4 fields with regex, rest = JSON | `stg_saved_searches` |
| `horse_power_min` filter: no horsepower in listings | 1 search | filter ignored, listed in `unsupported_filters` | `dim_saved_search` |
| `color` (search) vs `body_color` (listing) | – | mapped in staging | same |

Accepted trade-off: genuine > 50% drops on cheap cars (4,900 → 1,900) are lost. For a **user-facing
alert, precision beats recall**. One false "Porsche 911 now 194 CHF" alert costs more trust than a
missed real drop.

## 4. Model layers

```
raw (STRING, 1:1 from GCS)
 ├─ listings ──────────────► stg_listings__versions ─┬─► int_listing_price_changes ─► fct_listing_price_changes ─┐
 │                                                    └─► int_listing_lifecycle ──────► dim_listing ─────────────┤
 └─ users_saved_searches ──► stg_saved_searches ─────────────────────────────────────► dim_saved_search ────────┤
                                                                                                                 ▼
                                    fct_saved_search_price_drop_matches ─► fct_price_drop_notifications ─► rpt_daily_price_drop_notifications ─► GCS CSV
                                    rpt_avg_price_drop_per_model · rpt_days_on_platform · rpt_overpriced_listings ─► GCS CSV
```

| Layer | Dataset | Models | Purpose |
|---|---|---|---|
| raw | `raw` | `listings`, `users_saved_searches` | exact copy of source files |
| staging | `vladimir_dimitric_staging` | `stg_listings__versions`, `stg_saved_searches` | typed, cleaned, flagged, technical rows removed. One row per source entity |
| intermediate | `vladimir_dimitric_intermediate` | `int_listing_price_changes`, `int_listing_lifecycle` | business logic building blocks: classified price changes, time live per listing |
| marts (presentation) | `vladimir_dimitric_marts` | `dim_listing`, `dim_saved_search`, `fct_listing_price_changes`, notification and report models | star-schema style facts and dims, plus report tables for export |

## 5. Answers to the questions

### Q1. Notify a user when a car matching their saved search drops in price

`fct_saved_search_price_drop_matches` pairs every **valid** price drop on a car with every saved search where:

- the drop happened while the search was active (`created_at ≤ drop < deleted_at`). No alerts about
  history from before the user saved the search, and none after they deleted it;
- every supported filter holds on the listing **at the time of the drop, using the new price**. A missing
  filter means "no constraint";
- `entered_price_range` is set when the drop moved the car **into** the user's budget (old price above
  `price_max`, new price within it). That's the strongest reason to notify, so it ranks first.

### Q2. Daily export: who gets notified and with what

`rpt_daily_price_drop_notifications` has one row per car in a user's daily digest. Columns:

| Group | Columns |
|---|---|
| who / when | `run_date`, `send_date`, `user_id`, `digest_rank`, `n_listings_in_digest` |
| the car | `listing_id`, `listing_url`, `make_key`, `model_key`, `version_full_name`, `first_registration_year`, `mileage`, `body_color`, `fuel_type`, `seller_type`, `days_on_platform` |
| the drop | `price_before_chf`, `price_now_chf`, `drop_amount_chf`, `drop_pct`, `entered_price_range` |
| why the user gets it | `matched_saved_search_ids`, `matched_search_labels` |
| ready-made text | `message_subject`, `message_line` |

Before a row gets into the export:
1. Several matching searches of one user, or several drops of one car on the same day, collapse into
   **one row per user + car + day**.
2. **End-of-day check:** the car must still be `activated`, and its end-of-day price must still be below the
   price before the first drop. No alert for a car that was sold, paused or repriced back up the same day.
3. **Cooldown (7 days):** if the same user was already sent the same car in the last 7 days at the same or
   a lower price, it's suppressed. A further drop does notify again.
4. **Digest cap:** at most 5 cars per user per day, ranked by "entered your budget" first, then by % drop.

The export contains `user_id` only. The CRM / messaging service joins email, push token, language and
opt-in preferences, none of which are in this dataset.

### Q3. How often, and what the message contains

**Frequency: one daily digest, sent in the morning (e.g. 08:00 CH time) for the previous day's drops.**
- The source itself is a nightly batch (most versions are written at ~23:45), so faster than daily
  would only be possible with streaming CDC.
- A digest prevents spam from sellers who reprice in several small steps. Together with the cooldown
  and the 5-car cap, a user gets **at most 1 message a day**, and only when something actually changed.
- Possible later step (needs product and CRM input): instant push only for `entered_price_range`
  drops on a very specific search (make + model + budget). That's the most valuable and most time-sensitive case.

**Message (email / push):**
```
Subject: Price drop on 3 cars matching your saved search

BMW 320d Touring (2019) · 85'000 km
  24'900 → 22'500 CHF  (-9.6%) · now within your budget
  Listed for 34 days · [View car]
... (max 5 cars, ranked)

Your search: "bmw, from 10'000 CHF"  ·  [Edit search]  ·  [Unsubscribe from price alerts]
```
Content per car: photo, title, year, mileage, old → new price with % and CHF, the "within your budget" badge,
days listed (a stale listing makes the seller more open to negotiation), and a deep link. The footer
names the matching search and gives controls (edit search, unsubscribe), which matters for trust and is required for compliance.

### Q4. Export to GCS

```bash
gcloud storage buckets create gs://vladimir_dimitric_bucket --location=EU
dbt run-operation export_daily_notifications   # -> gs://vladimir_dimitric_bucket/notifications/run_date=YYYY-MM-DD/*.csv
dbt run-operation export_analytics             # -> gs://vladimir_dimitric_bucket/analytics/*.csv
```
`EXPORT DATA` with `overwrite=true` makes it idempotent: re-running a day replaces that day's file. The
`run_date=` folder keeps one partition per day for downstream consumers and backfills.

### Q5. Analytics queries

All three are dbt models (materialized and exportable). The SQL is in `models/marts/analytics/`.

**a. Average price drop per model** (`rpt_avg_price_drop_per_model`). Cars, valid drops only, models with
≥ 20 drop events. Two views:
- per drop event: avg / median CHF, avg %
- per listing: total reduction from the highest price before a drop to the lowest after it

**b. Average days a car is on the platform** (`rpt_days_on_platform`)
- days = time actually spent in `activated` status, summed over relists. **Not** `removed − created`,
  because drafts were never visible and 336k relists would be double-counted.
- reported separately for **completed** listings (no longer live: an unbiased "time to sell / remove") and
  **all** listings. Listings still live at the snapshot are right-censored and pull the average down.
- levels: all cars, seller type, make, make + model.
- "now" = the data snapshot (2025-10-29), not `current_date()`. Otherwise every live listing would gain a year.

**c. Overpriced listings** (`rpt_overpriced_listings`)
- peer group = same make, model, registration year, condition and mileage band, among cars live at the snapshot.
  Groups need ≥ 10 cars.
- **overpriced = price > 25% above the peer-group median** (median rather than mean, so outliers have less effect).
- extra signal `is_stale`: live > 60 days with no price drop. The market is confirming the overpricing.
- output: premium in % and CHF, peer p25 / median / p75, number of peers, days live.

## 6. Tests

62 data tests, `dbt build` runs them after each model:
- **sources (warn):** not_null keys, accepted status values
- **staging (error):** unique `(listing_id, valid_from)`, `valid_to > valid_from`, accepted values for
  status / category / seller / condition, price ≥ floor, saved-search JSON parses, `deleted_at ≥ created_at`
- **SCD2 invariants (singular):** exactly one current version per listing, no gaps or overlaps between versions
- **business rules:** valid drop is between 0 and 50% and above the floor, matches only within the search lifetime,
  matches re-checked independently against every filter, notification price lower than before, digest cap
  respected, one row per user + listing in the daily export

## 7. How to run

```bash
# 0. auth
gcloud auth login && gcloud auth application-default login

# 1. raw layer (GCS -> BigQuery, all STRING)
PROJECT=smg-ch-automotive-interview DATASET=vladimir_dimitric_raw ./scripts/load_raw.sh

# 2. dbt
pip install dbt-bigquery
cp profiles.yml.example ~/.dbt/profiles.yml
dbt deps
dbt build --target smg --vars '{raw_project: smg-ch-automotive-interview, raw_dataset: vladimir_dimitric_raw}'

# 3. a specific notification day (backfill / re-run)
dbt build -s rpt_daily_price_drop_notifications+ --target smg --vars '{run_date: "2025-10-27", raw_project: ..., raw_dataset: ...}'

# 4. export
dbt run-operation export_daily_notifications --target smg
dbt run-operation export_analytics --target smg
```

Developed in a personal BigQuery sandbox (`dime-smg-task`) while project permissions were being set up.
The `dev` target points there. In the sandbox, partitioning is off (`use_partitioning: false`) because
sandbox partitions older than 60 days are deleted automatically, and this data is from 2024–2025.

## 8. What I would do in production

- **Incremental models:** `stg_listings__versions` and `int_listing_price_changes` as `incremental`
  (merge on `listing_version_id`, re-reading a 2-day lookback because a version's `valid_to` is updated when
  the next one arrives). Partition by date, cluster by `listing_id`. Today's full rebuild of 15M rows is
  fine for a test, but not for a daily job.
- **Orchestration:** Cloud Composer / Airflow DAG: load → `dbt build` → tests must pass → export → trigger
  the CRM. If a test fails, nothing gets sent. A bad notification is worse than a late one.
- **Notification state:** a `notifications_sent` table written back by the messaging service. The cooldown
  should be based on what was actually sent (delivered, opened), not on candidates as in this version.
- **Scaling the match:** with millions of saved searches, replace the cross join with an equi-join on
  `make_key` (plus a separate branch for searches without a make), or invert it: index searches by filter.
- **Better overpriced model:** a regression or ML price estimate (make, model, year, mileage, fuel, power,
  equipment) instead of peer-group medians, and the same model could power a "good deal" badge.
- **Data contracts with the source team:** the duplicate zero-length versions, the `price = 1` convention
  and decimal-thousands typos should be fixed or documented at the source, not only patched downstream.
- **Source freshness tests**, and row-count anomaly checks on the daily load.

## 9. Assumptions

- Timestamps without a time zone (saved searches) are UTC.
- Notifications are for `vehicle_category = 'car'` (the task talks about cars). It's configurable via
  `notification_vehicle_categories`.
- Listing URL format `https://www.autoscout24.ch/de/d/<listing_id>` is assumed. It's configurable.
- Prices are CHF.

## Repo layout

```
analyses/      profiling.sql, followup.sql        data profiling that drove the rules
macros/        price_rules, run_params, export_to_gcs
models/
  staging/     stg_listings__versions, stg_saved_searches (+ sources, tests)
  intermediate/int_listing_price_changes, int_listing_lifecycle
  marts/core/  dim_listing, dim_saved_search, fct_listing_price_changes
  marts/notifications/  fct_saved_search_price_drop_matches, fct_price_drop_notifications, rpt_daily_price_drop_notifications
  marts/analytics/      rpt_avg_price_drop_per_model, rpt_days_on_platform, rpt_overpriced_listings
tests/         singular tests (SCD2 invariants, matching rules)
scripts/       load_raw.sh, run_profiling.py
```
# smg
