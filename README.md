# SMG Listings DWH – price-drop notifications

dbt + BigQuery solution for the Data Engineer homework. It builds a raw → staging → intermediate → marts DWH on
AutoScout24 / MotoScout24 listing history and users' saved searches, and produces a daily price-drop
notification export to GCS plus three analytics reports.

**Status:** built and tested on the full data in `smg-ch-automotive-interview`: 14 models, **65 data tests, all passing**
(`dbt build`: 79 nodes, 0 errors). Five independent audit queries reconcile (section 6). Exports are in
`gs://vladimir_dimitric_bucket`.

---

## 1. Approach

1. **Profile first** (`analyses/profiling.sql`, `analyses/followup.sql`). Every rule below is based on a number
   from these queries, not on an assumption.
2. **Raw = untouched.** Every column is `STRING`, so a bad value can never break ingestion. Typing and cleaning
   happen in staging.
3. **Flag, don't delete.** Invalid values become `NULL` in the clean column, with the original kept in `*_raw`
   plus a flag. Rejected price changes stay in the fact table with a `quality_issue`.
4. **One definition of a "real price drop"** (`is_valid_drop`). Notifications and analytics both use it, so they
   always agree.
5. **Tests encode the findings.** Source tests only warn (dirty input is expected). Staging and mart tests fail
   the build (the output contract must hold).
6. **Check the output numbers, not only the tests.** The first full run exposed a logic error that every test
   passed (section 3.1).

## 2. The data

| Source | Content | Grain / volume |
|---|---|---|
| `gs://listings_interview_source/ds_interview_listings_*` (80 gzip CSV parts, a BigQuery export) | **SCD Type 2** history of listings (ads). A new version opens on any change (price, status, images, …). `valid_to = 9999-01-01` = current version. | listing version: **15.3M rows, 1.52M listings**, ~10 versions each |
| `gs://interview_source_csv/users_and_saved_searches.csv` | saved searches, filters as JSON | saved search: **15 searches, 5 users** |

Listing status lifecycle: `draft → activated ⇄ deactivated → removed → deleted`, including
**336k `removed → activated` relists**.

## 3. Data-quality findings → decisions

| Finding (from profiling) | Volume | Decision | Where |
|---|---|---|---|
| Duplicate `(listing_id, valid_from)`. Always a `removed` row with `valid_from = valid_to` plus `deleted`, written by the ~23:45 nightly batch | 18,101 keys | drop **zero-length versions** (never live). This also removes the 8,997 window overlaps | `stg_listings_versions` |
| One header row per export part when loaded without skip-rows | 80 rows | `skip_leading_rows = 1` on the external table, plus a defensive filter in staging | `scripts/create_raw_external.sql`, staging |
| `price = 1` = "price on request" | 121k rows | `is_price_on_request`, clean `price` = NULL | staging |
| Prices typed in thousands (210,000 → **194.9**), anything under 500 CHF | 11k+ | price floor `min_valid_price = 500` | staging + price changes |
| Placeholder prices 11111 / 55555 / 123456 / 9999999 | ~1k | `is_placeholder_price` macro (repeated digits / sequences ≥ 10k) | `macros/price_rules.sql` |
| Drops > 50%, mostly extra-digit typos (383,600 → 38,300) | 9,793 | `drop_exceeds_threshold`, not a valid drop | `int_listing_price_changes` |
| Drop reverted within 24h (181,900 → 18,911 → 180,911) | 24,532 | `reverted_next_change`, not a valid drop (see 3.1) | same |
| Price edits in `draft` / `deactivated` (40,000 → 41 in a draft) | – | only consecutive **activated** versions are compared | same |
| Missing / unparseable price in an activated version | 0 rows | excluded from price-change events before the `LAG` (20k → NULL → 19k reads as 20k → 19k), so `change_type` can never be NULL. Guards future data | same |
| Registration year 1880 / 2923 | 592 | NULL outside 1900..current+1 | staging |
| Mileage 99,999,999 / > 1M km | 7.5k | NULL | staging |
| 116k `used` cars with 0 km, 16.5k `new` with > 1,000 km | – | mileage not reliable: coarse bands only, never a hard filter | overpriced report |
| ~50% of new versions change neither price nor status | 6.9M of 13.8M transitions | price change = a version where the price actually differs | price changes |
| Saved-search CSV: JSON column **unquoted** with commas, plus an empty line | – | load each line whole as one STRING, take the first 4 fields with regex, the rest is the JSON | `stg_saved_searches` |
| `horse_power_min`: listings have no horsepower | 1 search | ignored, listed in `unsupported_filters` | `dim_saved_search` |
| `color` (search) vs `body_color` (listing) | – | mapped in staging | same |

Accepted trade-off: genuine > 50% drops on cheap cars (4,900 → 1,900) are lost. For a **user-facing alert,
precision beats recall**. One false "Porsche 911 now 194 CHF" alert costs more trust than a missed real drop.

### 3.1 How the revert rule was built (iteration)

| Version | Rule | Reverted | Valid drops | What happened |
|---|---|---|---|---|
| v1 | next change ≥ 90% of old price | 346,304 | 675,120 | all tests passed, but a third of drops being "reverted" was implausible. The rule also matched **further drops** (20,000 → 19,500 → 19,000) |
| v2 | next change goes back **up** to ≥ 90% of old | 83,518 | 937,906 | bug fixed. Then profiled how long a revert takes |
| v3 | as v2, **within 24 h** | 24,532 | 996,892 | slower reverts are real temporary discounts and stay valid |

Time to revert (v2 population):

| Reverted within | Drops | Avg drop |
|---|---|---|
| < 1 day | 25,086 | 7.8% |
| 1–3 days | 8,562 | 6.1% |
| 3–14 days | 18,326 | 6.3% |
| 14+ days | 32,198 | 5.7% |

A typo is fixed within hours; a promotion runs for days, and a buyer could have bought at the lower price.

**Caveat:** the revert rule looks ahead. That's fine for historical analytics. In the daily run it's covered by
the end-of-day check, and a correction made later than the next morning would already have been notified.

Final classification of all 1.23M public price changes:

| Change | Quality issue | Count |
|---|---|---|
| drop | – (**valid**) | 996,892 |
| increase | – | 192,422 |
| drop | reverted_next_change | 24,532 |
| drop | drop_exceeds_threshold | 4,934 |
| drop | below_min_price | 4,394 |
| increase | below_min_price | 3,148 |
| drop | placeholder_price | 2,323 |
| increase | placeholder_price | 330 |

96% of all drops are valid; about 5:1 drops to increases, as expected in a used-car market.

### 3.2 What the data shows (cars, full history)

| Metric | Value |
|---|---|
| cars ever live | 1.30M |
| of which no longer live (completed) | 1.15M |
| days live until sold / removed (completed) | **median ~27, average ~54** (long tail of listings live for months) |
| private sellers vs dealers (average, completed) | **36.5 vs 55.9 days** |
| activation episodes per listing | 1.38 on average (relisting is common, so days live must sum the activated periods) |

Private cars leave the platform about 20 days faster than dealer cars. One plausible reason is more realistic
pricing or a more motivated seller. That's a hypothesis worth testing, not a finding.

## 4. Model layers

```
raw (STRING)
 ├─ listings (external table on GCS) ─► stg_listings_versions ─┬─► int_listing_price_changes ─► fct_listing_price_changes ─┐
 │                                                               └─► int_listing_lifecycle ──────► dim_listing ─────────────┤
 └─ users_saved_searches ─────────────► stg_saved_searches ─────────────────────────────────────► dim_saved_search ────────┤
                                                                                                                            ▼
                      fct_saved_search_price_drop_matches ─► fct_price_drop_notifications ─► rpt_daily_price_drop_notifications ─► rpt_daily_notification_recipients ─► GCS CSV (both)
                      rpt_avg_price_drop_per_model · rpt_days_on_platform · rpt_overpriced_listings ─────────────────────────► GCS CSV
```

| Layer | Dataset | Models | Materialization | Purpose |
|---|---|---|---|---|
| raw | `raw` | `listings`, `users_saved_searches` | external table / table | exact copy of the source files |
| staging | `vladimir_dimitric_staging` | `stg_listings_versions`, `stg_saved_searches` | view (*) | typed, cleaned, flagged, technical rows removed |
| intermediate | `vladimir_dimitric_intermediate` | `int_listing_price_changes`, `int_listing_lifecycle` | table | business-logic building blocks |
| marts | `vladimir_dimitric_marts` | `dim_listing`, `dim_saved_search`, `fct_listing_price_changes`, notification + report models | table | facts, dimensions and report tables for export |

(*) Views because of the BigQuery sandbox's 10 GB storage limit. In production staging would be an incremental,
partitioned table (section 8).

### Output volumes (full data)

| Model | Rows |
|---|---|
| `stg_listings_versions` | ~15.3M versions |
| `dim_listing` | 1.52M listings |
| `fct_listing_price_changes` | 1.2M price changes |
| `fct_saved_search_price_drop_matches` | 162.8k |
| `fct_price_drop_notifications` | 154.4k candidates (all days) |
| `rpt_daily_price_drop_notifications` | 25 rows for 2025-10-28 (5 users × cap of 5) |
| `rpt_daily_notification_recipients` | 5 users for 2025-10-28 |
| `rpt_avg_price_drop_per_model` | ~1.2k models |
| `rpt_overpriced_listings` | ~6.5k listings |

## 5. Answers to the questions

### Q1. Notify a user when a car matching their saved search drops in price

`fct_saved_search_price_drop_matches` pairs every **valid** price drop on a car with every saved search where:

- the drop happened while the search was active (`created_at ≤ drop < deleted_at`). No alerts about history from
  before the search was saved, and none after it was deleted;
- every supported filter holds on the listing **at the time of the drop, using the new price**. A missing filter
  means "no constraint";
- `entered_price_range` is set when the drop moved the car **into** the user's budget (old price above
  `price_max`, new price within it). That's the strongest reason to notify, so it ranks first.

All filters are optional, so there's no universal equi-join key (joining on `make_key` would drop 4 of the 15
searches). With 15 searches, a cross join plus filter is cheap and honest about the cost. At scale it becomes an
equi-join on `make_key` plus a small cross-join branch for searches without a make (section 8).

### Q2. Daily export: who gets notified and with what

`rpt_daily_price_drop_notifications` has one row per car in a user's daily digest:

| Group | Columns |
|---|---|
| who / when | `run_date`, `send_date`, `user_id`, `digest_rank`, `n_listings_in_digest` |
| the car | `listing_id`, `listing_url`, `make_key`, `model_key`, `version_full_name`, `first_registration_year`, `mileage`, `body_color`, `fuel_type`, `seller_type`, `days_on_platform` |
| the drop | `price_before_chf`, `price_now_chf`, `drop_amount_chf`, `drop_pct`, `entered_price_range` |
| why this user | `matched_saved_search_ids`, `matched_search_labels` |
| ready-made text | `message_subject`, `message_line` |

How a candidate becomes a row (`fct_price_drop_notifications`):
1. **Collapse:** several matching searches of one user, or several drops of one car in a day, become **one row
   per user + car + day**. A digest can mix cars from different searches of the same user.
2. **End-of-day check:** the car is still `activated`, and its end-of-day price is still below the price before
   the first drop. No alert for a car that was sold, paused or repriced back up the same day.
3. **Cooldown (7 days):** suppressed if the same user + car already had a candidate in the last 7 days at the
   same or a lower price. A further drop does notify again. The cooldown is based on **eligible candidates**, not
   confirmed deliveries: a car cut by yesterday's 5-car cap can suppress itself today unless its price drops
   further. Using actual sends needs delivery history from the messaging service (section 8).
4. **Digest cap:** at most 5 cars per user per day, ranked by "entered your budget" first, then by % drop.

Suppressed and capped rows stay in `fct_price_drop_notifications` with their flags, for audit and tuning. The
export filters `is_selected_for_digest`.

**Two files per day** answer "which users" and "what we share" separately:

| File | Grain | Use |
|---|---|---|
| `rpt_daily_notification_recipients` | one row per user | who gets a message today: subject, number of cars, cars now in budget, matched searches, rendered message body, listing ids |
| `rpt_daily_price_drop_notifications` | one row per car in a user's digest | what to render: car, prices, drop, link, rank |

The exports contain `user_id` only. The CRM / messaging service joins email, push token, language and opt-in
preferences, none of which are in this dataset.

### Q3. How often, and what the message contains

**Frequency: one daily digest, in the morning (e.g. 08:00 CH time), for the previous day's drops.**
- The source is a nightly batch (most versions are written at ~23:45). Faster than daily would need streaming CDC.
- A digest prevents spam from sellers who reprice in several small steps. With the cooldown and the 5-car cap, a
  user gets **at most 1 message a day**, and only when something actually changed.
- Possible later step: instant push only for `entered_price_range` drops on very specific searches
  (make + model + budget). That's the most valuable and most time-sensitive case.

**Message (email / push):**
```
Subject: Price drop on 3 cars matching your saved search

Volvo EX30 E60 Twin Ultra Performance AWD (2026) · 100 km
  53'965 → 46'500 CHF  (-13.8%) · now within your budget
  Listed for 7 days · [View car]
... (max 5 cars, ranked)

Your search: "volvo, 5'000-50'000 CHF"  ·  [Edit search]  ·  [Unsubscribe from price alerts]
```
Per car: photo, title, year, mileage, old → new price (CHF and %), the "within your budget" badge, days listed
(an older listing usually means a seller more open to negotiation), and a deep link. The footer names the
matching search and gives controls, which matters for trust and is required for compliance.

### Q4. Export to GCS

```bash
gcloud storage buckets create gs://vladimir_dimitric_bucket --location=EU
dbt run-operation export_daily_notifications   # -> gs://vladimir_dimitric_bucket/notifications/run_date=YYYY-MM-DD/ (recipients + notifications)
dbt run-operation export_analytics             # -> gs://vladimir_dimitric_bucket/analytics/*.csv
```
BigQuery `EXPORT DATA` with `overwrite = true` makes it idempotent: re-running a day replaces that day's file.
`run_date=` folders keep one partition per day for downstream consumers and backfills.

- `run_date` comes from the same macro the model uses (var or last complete day), not from the data, so the export
  also runs on a day with no notifications. A downstream job can tell "no notifications today" from "the pipeline
  failed".
- **Scope:** this project prepares and exports the notification data. Sending (email / push) is done by the CRM /
  messaging service, which joins `user_id` to contact details, language and opt-ins.

### Q5. Analytics queries

All three are dbt models, so they're tested, documented and exportable. The SQL is in `models/marts/analytics/`.

**a. Average price drop per model** (`rpt_avg_price_drop_per_model`). Cars, valid drops only, models with at
least 20 drop events. Two views, because "average drop" can mean either:
- per drop **event**: average and median CHF, average %
- per **listing**: **max drawdown**, the largest fall from the running peak price to a later price, in time order.
  Not the sum of drops, which double-counts when the price goes up in between (20k → 18k → 25k → 24k).

**b. Average days a car is on the platform** (`rpt_days_on_platform`)
- days = time actually spent in `activated` status, summed over relists. **Not** `removed − created`: drafts were
  never visible, and the 336k relists would be double-counted.
- reported separately for **completed** listings (no longer live: an unbiased "time to sell / remove") and
  **all** listings (those still live are right-censored and pull the average down). The next step would be
  survival analysis (Kaplan–Meier).
- levels via `GROUPING SETS`: all cars, seller type, make, make + model. The median sits beside the average
  because durations are skewed.
- "now" = the data snapshot (2025-10-29), not `current_date()`.

**c. Overpriced listings** (`rpt_overpriced_listings`)
- peer group = same make, model, registration year, condition and mileage band, among cars live at the snapshot,
  with at least 10 peers.
- **overpriced = price ≥ 25% above the peer-group median** (median, so outliers have less effect).
- `is_stale`: live > 60 days with no price drop. The market is confirming the overpricing.
- output: premium in % and CHF, peer p25 / median / p75, number of peers, days live.
- This is a heuristic flag for **potential** overpricing (price vs comparable cars), not proof: equipment,
  condition and history aren't in the data.

## 6. Tests

**65 data tests**, run by `dbt build` after each model:
- **sources (warn):** not_null keys, accepted status values
- **staging (error):** unique `(listing_id, valid_from)`, `valid_to > valid_from`, accepted values for status /
  category / seller / condition, price ≥ floor, saved-search JSON parses, `deleted_at ≥ created_at`
- **SCD2 invariants (singular):** exactly one current version per listing; no gaps or overlaps between versions
- **business rules:** a valid drop is between 0% and 50% and above the floor; matches only within the search lifetime;
  every match re-checked independently against every filter; notification price below the price before; digest cap
  respected; one row per user + listing in the daily export

- **reconciliation (singular):** `assert_days_on_platform_reconciles` recomputes every level of
  `rpt_days_on_platform` with a separate `GROUP BY` (no `GROUPING SETS`) and full-outer-joins it to the report

### Independent audit queries (`analyses/audit_queries.sql`)

Five recalculation queries, each returning only discrepancies (0 rows = pass):

| # | Audit | What it recomputes independently | Result |
|---|---|---|---|
| 1 | Average price drop per model | event metrics + per-listing max drawdown from the price-change fact | 0 discrepancies |
| 2 | Days on platform | all four levels with separate `GROUP BY`s | 0 real discrepancies (see below) |
| 3 | Overpriced listings | peer groups, peer percentiles, threshold, stale flag | 0 discrepancies |
| 4 | Notification ↔ listing ↔ saved search | every exported row: listing exists in the mart and in staging at end of day, and at least one active search of that user matches it | 0 discrepancies |
| 5 | Candidates, cooldown, cap, export | uniqueness, cooldown flag, digest cap, end-of-day status, selected rows ⇄ export rows | 0 discrepancies |

All counts and averages match exactly. The only differences are **medians in the three largest groups** of audit 2
(all cars 27.6 vs 27.7, private 23.1 vs 22.8, dealers 27.1 vs 27.9 days, i.e. < 1 day on 130k–1.3M listings). That is
the expected approximation of `APPROX_QUANTILES`: it keeps a compressed sketch, so its error grows with group size and
two runs can differ slightly. Smaller groups match exactly. Medians are therefore compared with a tolerance (2% or
1 day). The seller-type rows also add up to the all-cars total (128,764 + 1,167,298 = 1,296,062), so no rows are lost.

Note: the audits check the **mechanics** (aggregation, joins, flags); they reuse the same business definitions
(`is_valid_drop`, `days_active`), so they don't validate those definitions themselves. The definitions are justified
by the profiling in section 3.

An earlier draft of audit 1 still used a sum of drops per listing; it flagged every model until it was aligned with
the max-drawdown definition, which confirmed that the fix was applied.

Lesson from section 3.1: the tests guard the structure, but the v1 revert bug passed all of them. A useful
addition would be a guard on distributions, e.g. the share of drops flagged `reverted_next_change` must stay
under a threshold.

## 7. How to run

```bash
# 0. auth
gcloud auth login && gcloud auth application-default login

# 1. raw layer
bq query --location=EU --use_legacy_sql=false < scripts/create_raw_external.sql   # listings: external table on GCS
PROJECT=<project> DATASET=raw ./scripts/load_raw.sh                       # saved searches (one STRING per line)

# 2. dbt
pip install dbt-bigquery
cp profiles.yml.example profiles.yml
dbt deps --profiles-dir .
dbt build --profiles-dir .                                                # dev = personal sandbox
dbt build --profiles-dir . --target smg \
  --vars '{raw_project: smg-ch-automotive-interview, raw_dataset: vladimir_dimitric_raw}'

# 3. a specific notification day (backfill / re-run)
dbt build --profiles-dir . -s rpt_daily_price_drop_notifications --vars '{run_date: "2025-10-27"}'

# 4. export
dbt run-operation export_daily_notifications --profiles-dir . --target smg
dbt run-operation export_analytics --profiles-dir . --target smg
```

I developed this in a personal BigQuery sandbox (`dime-smg-task`) while project permissions were being set up.
Sandbox constraints shaped three choices: raw listings as an external table, staging as views (10 GB storage limit),
and partitioning switched off (`use_partitioning: false`), because the sandbox expires partitions older than 60 days
and this data is from 2024–2025.

## 8. What I would do in production

- **Incremental models:** `stg_listings_versions` and `int_listing_price_changes` as `incremental` (merge on
  `listing_version_id`, re-reading a 2-day lookback because a version's `valid_to` is updated when the next one
  arrives). Partition by date, cluster by `listing_id`. Today every downstream model re-scans the staging view
  (~3 GiB per model), which is fine for a test but not for a daily job.
- **Orchestration:** Cloud Composer / Airflow: load → `dbt build` → tests must pass → export → trigger the CRM. If a
  test fails, nothing is sent. A bad notification is worse than a late one.
- **Notification state:** a `notifications_sent` table written back by the messaging service. Today the cooldown
  compares against **candidates**, not what was **sent**. A car cut by yesterday's cap suppresses itself today
  unless the price drops further. Using sends is recursive (today depends on yesterday's selection), so it needs
  an incremental model or CRM feedback.
- **Scaling the match:** an equi-join on `make_key` plus a small cross-join branch for searches without a make,
  or invert it: index searches by filter, or match per event in a streaming service.
- **Better overpriced model:** a regression / ML price estimate (make, model, year, mileage, fuel, power,
  equipment) instead of peer medians. The same model could power a "good deal" badge.
- **Data contracts with the source team:** zero-length duplicate versions, the `price = 1` convention and
  decimal-thousand typos should be fixed or documented at the source, not only patched downstream.
- **Monitoring:** source freshness, row-count anomaly checks, and distribution guards (share of rejected drops,
  notifications per day).

## 9. Assumptions

- Timestamps without a time zone (saved searches) are UTC.
- Notifications are for `vehicle_category = 'car'`, configurable via `notification_vehicle_categories`.
- Listing URL format `https://www.autoscout24.ch/de/d/<listing_id>` is assumed and configurable.
- Prices are CHF.
- All thresholds are dbt vars in `dbt_project.yml`, so they can be tuned without code changes.

## Repo layout

```
analyses/      profiling.sql, followup.sql         profiling that drove the rules
               audit_queries.sql                    five independent audit queries (section 6)
macros/        price_rules, run_params, export_to_gcs
models/
  staging/     stg_listings_versions, stg_saved_searches (+ sources, tests)
  intermediate/int_listing_price_changes, int_listing_lifecycle
  marts/core/  dim_listing, dim_saved_search, fct_listing_price_changes
  marts/notifications/  fct_saved_search_price_drop_matches, fct_price_drop_notifications,
                        rpt_daily_price_drop_notifications, rpt_daily_notification_recipients
  marts/analytics/      rpt_avg_price_drop_per_model, rpt_days_on_platform, rpt_overpriced_listings
tests/         singular tests (SCD2 invariants, matching rules, days-on-platform reconciliation)
scripts/       create_raw_external.sql, load_raw.sh, run_profiling.py
```
