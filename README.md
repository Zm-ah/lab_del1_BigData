# Marathos — Big Data Lab Project

> **Author:** Zineb Mouhib  
> **Program:** Data Engineering  
> **Dataset:** TWO_CENTURIES_OF_UM_RACES.csv — ultramarathon race results spanning 200 years


### Project Overview
Marathos is a big data pipeline project built on **Databricks** using a **Medallion Architecture**(Bronze → Silver → Gold). The pipeline ingests raw ultramarathon race data, cleans and transforms it, and exposes it as a dimensional model ready for analytics and dashboards.
The project uses **Delta Live Tables (DLT)** for the Bronze and Silver layers, **Unity Catalog** for data governance, and **Plotly** for interactive dashboards.



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
Ingests the raw CSV file as-is, renaming columns to snake_case. No filtering or transformation is applied — this layer preserves the original data in its entirety.

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
Reads from the Bronze table and applies a full cleaning and standardisation pipeline.

### Cleaning decisions

| Step | Decision | Rationale |
|---|---|---|
| Multi-day events (`d` unit) | **Removed** | Unreliable data format, documented decision |
| Invalid unit combinations | **Removed** | e.g. distance event with distance performance makes no sense |
| Speed outside 0.5–35 km/h | **Removed** | Physically impossible values |
| Speed containing `:` | **Removed** | Malformed strings |
| Null `athlete_id`, `athlete_performance`, `event_distance_length` | **Removed** | Cannot be used for analysis |
| Null `athlete_club` | **Kept** | High null rate, non-critical column |
| Null `athlete_average_speed` | **Kept** | Speed not always recorded, still useful rows |

### Transformations applied

- `athlete_performance` → converted to `performance_seconds` (for time-based events) or `performance_km` (for distance-based events)
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

- `get_event_unit()` — extracts unit from event distance string (`km`, `mi`, `h`, `d`)
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
| `performance_km` | float | Distance covered in km |
| `athlete_average_speed` | float | Speed in km/h |
| `athlete_age_at_event` | int | Age at time of race |

**`dim_event`** — Event dimension

| Column | Type | Description |
|---|---|---|
| `event_id` | string (PK) | SHA-256 hash of event_name |
| `event_name` | string | Full event name |
| `event_type` | string | `distance` or `time` |
| `event_distance_length` | string | Original distance/duration string |
| `event_number_of_finishers` | int | Total finishers |
| `event_dates` | string | Event date(s) |

**`dim_athlete`** — Athlete dimension

| Column | Type | Description |
|---|---|---|
| `athlete_key` | string (PK) | SHA-256 hash of athlete_id |
| `athlete_id` | string | Original athlete identifier |
| `athlete_country` | string | Country (uppercase) |
| `athlete_gender` | string | Gender (uppercase) |
| `athlete_year_of_birth` | int | Year of birth |
| `athlete_age_category` | string | Age group category |

**`dim_time`** — Time dimension

| Column | Type | Description |
|---|---|---|
| `year_id` | string (PK) | SHA-256 hash of year_of_event |
| `year_of_event` | int | Year of the race |

---

## Pipeline Configuration

**Pipeline name:** `marathos_zineb_mouhib`  
**Type:** ETL pipeline (Delta Live Tables)  
**Catalog:** `marathos`  
**Default schema:** `bronze`  
**Mode:** Triggered  

> **Important:** When creating a DLT pipeline in Databricks, Unity Catalog must be selected **before** any other field. If the pipeline is created with Hive Metastore, the storage option cannot be changed after creation.

> **Important:** The Silver table name is specified as `marathos.silver.obt` directly in `@dlt.table(name=...)` to ensure correct catalog/schema placement regardless of the pipeline's default schema setting.

---

## Known Issues & Lessons Learned

| Issue | Root cause | Resolution |
|---|---|---|
| Tables created in wrong catalog | Pipeline created with Hive Metastore instead of Unity Catalog | Delete pipeline, recreate with Unity Catalog selected first |
| `PERMISSION_DENIED: Can not move tables across arclight catalogs` | Pipeline storage option locked to wrong catalog | Delete and recreate pipeline — storage option cannot be changed after creation |
| Silver table landing in `bronze` schema | Default schema in pipeline was `bronze` | Specify full `catalog.schema.table` path in `@dlt.table(name=...)` |
| Helper functions not importable in DLT | DLT notebooks do not support `sys.path` imports | Inline all helper functions directly in the DLT notebook |

---



## Dashboard (Task 6)

An interactive dashboard was built using Databricks AI/BI Dashboard 
connected directly to the Gold layer tables.

### Datasets (SQL-based)
| Dataset | Description |
|---------|-------------|
| `kpi_overview` | Aggregated KPI metrics across all data |
| `kpi_filtered` | Filterable KPI metrics by gender, event type and year |
| `top_countries_km` | Top 15 countries by finishers in km races |
| `top_countries_mi` | Top 15 countries by finishers in mile races |
| `gender_distribution` | Result count by gender and event type |
| `participation_trend` | Number of results per year |
| `speed_by_age_gender` | Average speed per age category and gender |
| `top10_fastest_km` | Top 10 fastest athletes in km races |
| `speed_by_distance` | Average speed and time by event distance |

### Visualizations
- **Counter widgets** – Interactive KPI cards (unique races, total results, countries, avg speed)
- **Bar charts** – Country rankings, gender distribution, age category analysis, fastest athletes
- **Line/Area chart** – Participation trend over time

### Interactive Filters
Three global filters connected across all relevant datasets:
- `event_type` – Filter by kilometers or miles
- `athlete_gender` – Filter by M or F
- `year_of_event` – Filter by year (1996–2023)

### Data Quality Fix
During dashboard development, a gender inconsistency was discovered in the 
Silver layer. The raw data contained `F`, `W`, and `X` values for 
`athlete_gender`. A cleaning step was added to Silver notebook to:
- Keep only valid values (`M`, `F`, `W`)
- Null out invalid values (`X` and other)
- Pipeline was rerun to propagate fix through Silver → Gold

---

##  Genie Space(Task 7)

A Genie Space was created to allow business stakeholders to ask 
ad hoc questions in natural language without involving data analysts.

### Connected Tables
All Gold layer tables were linked to the Genie Space:
- `marathos.gold.fct_results`
- `marathos.gold.dim_athlete`
- `marathos.gold.dim_event`
- `marathos.gold.dim_time`
- `marathos.gold.mart_fastest_athletes_km`
- `marathos.gold.mart_fastest_athletes_mi`
- `marathos.gold.mart_top_countries_km`
- `marathos.gold.mart_top_countries_mi`

### Verification
Genie responses were manually verified in the `genie_verification` 
notebook using SQL queries against the Gold tables.

**Key finding:** When asked about the top 5 fastest athletes, Genie used 
`MAX(speed)` while our mart table `mart_fastest_athletes_km` is based on 
`athlete_average_speed`. This resulted in different rankings:
- Genie: SWE athlete 394189 (28.22 km/h max speed)
- SQL verification: NZL athlete 868574 (24.79 km/h avg speed)

This finding demonstrates the importance of verifying AI-generated 
answers against trusted data sources before sharing with stakeholders.

### Technologies Used
- Databricks AI/BI Dashboard
- Databricks Genie Space
- SQL (Databricks SQL dialect)
- Delta Live Tables (DLT) – Bronze/Silver/Gold pipeline
- Unity Catalog – `marathos` catalog
- Delta Lake – Materialized Views



