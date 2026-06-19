# Marathos — Big Data Lab Project

> **Author:** Zineb Mouhib  
> **Program:** Data Engineering  
> **Dataset:** TWO_CENTURIES_OF_UM_RACES.csv — ultramarathon race results spanning 200 years  
> **Scope:** European ultramarathon events (kilometer-based races only)

### Project Overview
Marathos is a big data pipeline project built on **Databricks** using a **Medallion Architecture** (Bronze → Silver → Gold). The pipeline ingests raw ultramarathon race data, cleans and transforms it, and exposes it as a dimensional model ready for analytics and dashboards.

The project uses **Delta Live Tables (DLT)** for the Bronze and Silver layers, **Unity Catalog** for data governance, and **Databricks AI/BI Dashboard** for interactive visualizations.

**Scope decision:** To deliver a focused, high-quality dataset for analysis, the Silver layer filters the data to **European countries only** and **kilometer-based events only** (mile-based and other-unit events are excluded). This decision was made to ensure consistent units of measurement and a clear geographic story for the dashboard and Genie Space.

---

## Unity Catalog Structure

| Level | Name |
|---|---|
| Catalog | `marathos` |
| Bronze schema | `marathos.bronze` |
| Silver schema | `marathos.silver` |
| Gold schema | `marathos.gold` |
| Raw data (Volume) | `/Volumes/marathos/bronze/raw/TWO_CENTURIES_OF_UM_RACES.csv` |

> **Note:** The Volume is located under the `bronze` schema rather than a separate `landing` schema. This is a known structural deviation from best practice (a dedicated landing zone is preferred). It is functional and documented here for transparency.

---

## Bronze Layer — `marathos.bronze.races`

**File:** `transformation/bronze/bronze_races.sql`  
**Type:** Materialized View (DLT)  
**Source:** CSV file via `read_files()` from Unity Catalog Volume

### What it does
Ingests the raw CSV file as-is, renaming columns to snake_case. No filtering or transformation is applied — this layer preserves the original global data in its entirety.

### Key columns

| Column | Description |
|---|---|
| `year_of_event` | Year the race took place |
| `event_name` | Full name of the event |
| `event_distance_length` | Distance or duration (e.g. `50km`, `24h`) |
| `athlete_performance` | Finishing time or distance covered |
| `athlete_average_speed` | Speed string (e.g. `8.5 km/h`) |
| `athlete_id` | Unique athlete identifier |
| `athlete_gender` | Gender of the athlete |
| `athlete_country` | Country of the athlete |

---

## Silver Layer — `marathos.silver.obt`

**File:** `transformation/silver/silver_notebook.py`  
**Type:** Materialized View (DLT)  
**Source:** `marathos.bronze.races`

### What it does
Reads from the Bronze table and applies a full cleaning, filtering, and standardisation pipeline. This is where the dataset scope is narrowed to **European, kilometer-based ultramarathon events**.

### Cleaning and filtering decisions

| Step | Decision | Rationale |
|---|---|---|
| Event unit | **Kept `km` only** — `mi` and `h`-based events removed | Ensures consistent unit of measurement across the entire dataset; removes the need for unit-conversion logic downstream |
| Athlete country | **Kept European countries only** (ISO 3166-1 alpha-3 codes) | Focuses the analysis on a clear, presentable geographic scope (22 countries represented in the final dataset) |
| Invalid unit combinations | **Removed** | e.g. a distance event paired with a distance performance value makes no logical sense |
| Speed outside 0.5–35 km/h | **Removed** | Physically impossible values |
| Speed containing `:` | **Removed** | Malformed strings |
| Null `athlete_id`, `athlete_performance`, `event_distance_length` | **Removed** | Cannot be used for analysis |
| Gender values outside `M`, `F`, `W` | **Nulled out** | Raw data contained inconsistent values (`X` and other garbage); only valid gender codes are kept |

### Transformations applied

- `athlete_performance` → converted to `performance_seconds` (all remaining events are time-based, since only `km` events are kept)
- `athlete_average_speed` → extracted numeric float from string (e.g. `"8.5 km/h"` → `8.5`)
- `athlete_year_of_birth` → cast to `int`
- `event_number_of_finishers` → cast to `int`
- `athlete_gender` / `athlete_country` → standardised to uppercase
- `athlete_age_at_event` → derived as `year_of_event - athlete_year_of_birth`

### Surrogate keys (SHA-256)

| Key column | Source columns | Rationale |
|---|---|---|
| `event_id` | `event_name` | Stable hash across pipeline runs |
| `athlete_key` | `athlete_id` | Preferred over `dense_rank()` for streaming compatibility |
| `year_id` | `year_of_event` | Consistent time dimension key |

> SHA-2 (256-bit) was chosen over `dense_rank()` because it does not require a full table scan and produces consistent IDs when new data arrives — important for streaming-compatible pipelines.

### Helper functions (inlined)

The following UDFs are defined directly in the Silver notebook (not imported via `sys.path`) because DLT notebooks do not support external module imports:

- `get_event_unit()` — extracts unit from event distance string (`km`, `mi`, `h`)
- `get_performance_unit()` — determines if performance is a time (`h`) or distance (`km`)
- `is_valid_unit_combination()` — validates that event and performance units are logically consistent
- `time_to_seconds()` — converts `HH:MM:SS` strings to integer seconds
- `add_hash_id()` — generates SHA-256 surrogate keys

---

## Dimensional Model (Task 4)

Star schema designed in [dbdiagram.io](https://dbdiagram.io).

```
          dim_event
              │
              │ 1:many
              ▼
dim_athlete ──► fct_results ◄── dim_time
```

### Tables

**`fct_results`** — Fact table

| Column | Type | Description |
|---|---|---|
| `result_id` | string (PK) | Surrogate key |
| `event_id` | string (FK) | References `dim_event` |
| `athlete_key` | string (FK) | References `dim_athlete` |
| `year_id` | string (FK) | References `dim_time` |
| `performance_seconds` | int | Finishing time in seconds |
| `athlete_average_speed` | float | Speed in km/h |
| `athlete_age_at_event` | int | Age at time of race |

**`dim_event`** — Event dimension

| Column | Type | Description |
|---|---|---|
| `event_id` | string (PK) | SHA-256 hash of event_name |
| `event_name` | string | Full event name |
| `event_distance_length` | string | Original distance string (all `km`-based) |
| `event_number_of_finishers` | int | Total finishers |
| `event_dates` | string | Event date(s) |

**`dim_athlete`** — Athlete dimension

| Column | Type | Description |
|---|---|---|
| `athlete_key` | string (PK) | SHA-256 hash of athlete_id |
| `athlete_id` | string | Original athlete identifier |
| `athlete_country` | string | European country code (uppercase) |
| `athlete_gender` | string | Gender (`M` or `F`, uppercase) |
| `athlete_year_of_birth` | int | Year of birth |
| `athlete_age_category` | string | Age group category |

**`dim_time`** — Time dimension

| Column | Type | Description |
|---|---|---|
| `year_id` | string (PK) | SHA-256 hash of year_of_event |
| `year_of_event` | int | Year of the race |

---

## Gold Serving Layer — Mart Views

Two analytics-ready mart views were built on top of the star schema to power the dashboard and Genie Space directly, avoiding repeated joins in every downstream query.

**`mart_top_countries`** — Aggregated results per European country (total finishers, average speed, average time)

**`mart_fastest_athletes`** — Top 1,000 fastest individual performances, joined and pre-formatted for direct use in tables and charts

---

## Pipeline Configuration

**Pipeline name:** `marathos_zineb_mouhib`  
**Type:** ETL pipeline (Delta Live Tables)  
**Catalog:** `marathos`  
**Default schema:** `bronze`  
**Mode:** Triggered  

> **Important:** When creating a DLT pipeline in Databricks, Unity Catalog must be selected **before** any other field. If the pipeline is created with Hive Metastore, the storage option cannot be changed after creation.

> **Important:** The Silver table name is specified as `marathos.silver.obt` directly in `@dlt.table(name=...)` to ensure correct catalog/schema placement regardless of the pipeline's default schema setting.

> **Important:** Gold layer materialized views are managed entirely by the DLT pipeline definition. They cannot be updated directly via `CREATE OR REFRESH MATERIALIZED VIEW` in the SQL Editor — any changes to Gold logic must go through a pipeline **Full Refresh**.

---

## Known Issues & Lessons Learned

| Issue | Root cause | Resolution |
|---|---|---|
| Tables created in wrong catalog | Pipeline created with Hive Metastore instead of Unity Catalog | Delete pipeline, recreate with Unity Catalog selected first |
| `PERMISSION_DENIED: Can not move tables across catalogs` | Pipeline storage option locked to wrong catalog | Delete and recreate pipeline — storage option cannot be changed after creation |
| Silver table landing in `bronze` schema | Default schema in pipeline was `bronze` | Specify full `catalog.schema.table` path in `@dlt.table(name=...)` |
| Helper functions not importable in DLT | DLT notebooks do not support `sys.path` imports | Inline all helper functions directly in the DLT notebook |
| `MATERIALIZED_VIEW_OPERATION_NOT_ALLOWED` when editing Gold tables in SQL Editor | Materialized views created by a DLT pipeline cannot be replaced outside the pipeline | Edit the `.sql` files inside the pipeline's `transformation/gold` folder, then trigger a **Full Refresh** |
| Genie Space returned outdated results (non-European athletes) after a data scope change | Genie Space cached the old table schema/data after Gold tables were refreshed | Remove and re-add the affected table in Genie Space's Data configuration, then start a new chat thread |
| A small number of corrupted `event_distance_length` values remain (e.g. multi-stage races with comma-separated distances) | Source data contains a handful of non-standard distance strings | Documented as a known minor data quality limitation; affects fewer than 5 of several thousand unique distance values and does not materially impact aggregate results |

---

## Dashboard (Task 6)

An interactive dashboard was built using Databricks AI/BI Dashboard, connected directly to the Gold layer tables, focused on the European ultramarathon scene.

### Datasets (SQL-based)

| Dataset | Description |
|---------|-------------|
| `kpi_overview` | Aggregated KPI metrics across the full European dataset (unique races, total results, countries represented, average speed) |
| `top_countries` | Top 15 European countries by finishers, with average speed |
| `gender_distribution` | Result count by gender |
| `participation_trend` | Number of results per year (1996–2023) |
| `speed_by_distance` | Average speed by event distance |
| `top10_fastest` | Top 10 fastest individual athlete performances |

### Visualizations

- **KPI counter widgets** – Countries, Total Results, Unique Races, Average Speed (km/h)
- **Choropleth map** – European finishers by country, color-coded by total finisher volume
- **Bar chart** – Gender distribution (M vs F)
- **Line chart** – Participation trend over time (1996–2023), showing the COVID-19 dip in 2020
- **Horizontal bar chart** – Top 15 countries by finishers
- **Table** – Top 10 fastest athletes with country, gender, event, speed, and time

### Cross-filtering

Clicking on a data point in the participation trend line chart filters the rest of the dashboard (including the `Countries` KPI) to that specific year, allowing exploration of how many countries were active in a given year.

### Data Quality Fix

During dashboard development, a gender inconsistency was discovered in the Silver layer. The raw data contained `F`, `W`, and `X` values for `athlete_gender`. A cleaning step was added to the Silver notebook to:
- Keep only valid values (`M`, `F`)
- Null out invalid values (`X` and other)
- Pipeline was rerun to propagate the fix through Silver → Gold

A second, larger data quality decision was made later in the project: the dataset was narrowed from a global, mixed-unit (`km`/`mi`) dataset to a **European, kilometer-only** dataset, directly in the Silver layer. This required rebuilding all Gold dimension and fact tables, renaming mart views (`mart_top_countries_km` → `mart_top_countries`, `mart_fastest_athletes_km` → `mart_fastest_athletes`), and removing all mile-specific mart views and dashboard widgets.

---

## Genie Space (Task 7)

A Genie Space was created to allow business stakeholders to ask ad hoc questions in natural language without involving data analysts.

### Connected Tables

All Gold layer tables relevant to the European, kilometer-only scope are linked to the Genie Space:
- `marathos.gold.fct_results`
- `marathos.gold.dim_athlete`
- `marathos.gold.dim_event`
- `marathos.gold.dim_time`
- `marathos.gold.mart_top_countries`
- `marathos.gold.mart_fastest_athletes`

### Instructions

The Genie Space includes a written instruction noting that the dataset covers European, kilometer-based ultramarathon events only, with `athlete_country` using ISO 3166-1 alpha-3 codes, to prevent the model from making incorrect assumptions about unit or geographic scope.

### Verification

Genie responses were manually verified in the `genie_verification` notebook using SQL queries against the Gold tables, for questions including:
- Which European country has the most ultramarathon finishers?
- How has participation in European ultramarathons changed over the years?
- What is the gender distribution among European ultramarathon athletes?
- Who are the fastest athletes in European ultramarathons?
- What is the average speed by event distance?
- Which countries have the highest average finishing speed?

**Key finding:** After the dataset scope was narrowed to Europe-only, Genie Space initially continued to return non-European athletes (e.g. New Zealand, USA, South Africa) in response to "fastest athletes" queries, despite the underlying Gold tables already containing only European data — confirmed via direct SQL queries in the `genie_verification` notebook. This was caused by Genie Space caching outdated table metadata. Removing and re-adding the affected table in the Genie Space configuration, combined with starting a new chat thread, resolved the issue. This finding demonstrates the importance of independently verifying AI-generated answers against trusted data sources, even after confirming the underlying data is correct.

### Technologies Used

- Databricks AI/BI Dashboard
- Databricks Genie Space
- SQL (Databricks SQL dialect)
- Delta Live Tables (DLT) – Bronze/Silver/Gold pipeline
- Unity Catalog – `marathos` catalog
- Delta Lake – Materialized Views


