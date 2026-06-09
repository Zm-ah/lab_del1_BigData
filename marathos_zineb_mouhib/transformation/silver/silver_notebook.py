# Databricks notebook source
# silver_notebook.py
# Marathos – Silver Layer (DLT pipeline)
# Reads from marathos.bronze.races (external table), cleans data, writes to obt

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType

# ── Helper functions (from utilities.py) ──────────────────────────────────

def get_event_unit(val):
    if val is None: return None
    val = val.lower().strip()
    for unit in ["km", "mi", "h", "d"]:
        if val.endswith(unit): return unit
    return None

def get_performance_unit(perf):
    if perf is None: return None
    perf = perf.strip()
    if ":" in perf: return "h"
    try:
        float(perf)
        return "km"
    except ValueError:
        return None

def is_valid_unit_combination(event_unit, performance_unit):
    if event_unit is None or performance_unit is None: return False
    if event_unit in ("km", "mi") and performance_unit == "h": return True
    if event_unit == "h" and performance_unit == "km": return True
    return False

def time_to_seconds(time_str):
    if time_str is None: return None
    try:
        s = time_str.strip()
        extra_days = 0
        if "day" in s:
            parts = s.split(",")
            extra_days = int(parts[0].strip().split(" ")[0])
            s = parts[1].strip()
        hms = s.split(":")
        if len(hms) == 3:
            h, m, sec = int(hms[0]), int(hms[1]), int(float(hms[2]))
            return extra_days * 86400 + h * 3600 + m * 60 + sec
        return None
    except Exception:
        return None

get_event_unit_udf         = F.udf(get_event_unit, StringType())
get_performance_unit_udf   = F.udf(get_performance_unit, StringType())
is_valid_unit_combination_udf = F.udf(is_valid_unit_combination)
time_to_seconds_udf        = F.udf(time_to_seconds, IntegerType())

def add_hash_id(df: DataFrame, source_cols: list, id_col: str) -> DataFrame:
    concat_col = F.concat_ws("||", *[F.col(c).cast("string") for c in source_cols])
    return df.withColumn(id_col, F.sha2(concat_col, 256))

# COMMAND ----------

@dlt.table(
    name="marathos.silver.obt",
    comment="Cleaned and standardised silver OBT table for ultramarathon races",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def silver_obt():

    # ── STEP 1: Read from Bronze ───────────────────────────────────────────
    # Read from the external bronze table (not managed by this pipeline)
    df_bronze = spark.read.table("marathos.bronze.races")

    # ── STEP 2: Extract and classify units ────────────────────────────────
    # Determines the unit for each event distance and athlete performance,
    # then validates that the combination makes logical sense.
    # Valid combos:
    #   event = km or mi  →  performance must be a time (HH:MM:SS)
    #   event = h         →  performance must be a distance (decimal km)
    df_with_units = (
        df_bronze
        .withColumn("event_unit",       get_event_unit_udf(F.col("event_distance_length")))
        .withColumn("performance_unit", get_performance_unit_udf(F.col("athlete_performance")))
        .withColumn("is_valid_combo",   is_valid_unit_combination_udf(
                                            F.col("event_unit"),
                                            F.col("performance_unit")
                                        ))
    )

    # ── STEP 3: Remove invalid rows ───────────────────────────────────────
    # Drops:
    #   - multi-day events (unit = "d") — unreliable data, documented decision
    #   - rows where event/performance unit combo is invalid
    #   - rows with null performance, distance, or athlete_id
    #   - speeds containing ":" (malformed strings)
    #   - speeds outside the realistic range of 0.5–35 km/h
    #     (nulls are kept — speed is not always recorded)
    df_clean = (
    df_with_units
    .filter(F.col("event_unit") != "d")
    .filter(F.col("is_valid_combo") == True)
    .filter(F.col("athlete_performance").isNotNull())
    .filter(F.col("event_distance_length").isNotNull())
    .filter(F.col("athlete_id").isNotNull())
    .filter(~F.col("athlete_average_speed").contains(":"))
    .filter(
        F.regexp_replace(F.col("athlete_average_speed"), "[^0-9.]", "").cast("float").between(0.5, 35)
        | F.col("athlete_average_speed").isNull()
    )
    .filter(
        F.col("performance_seconds").isNull() | 
        (F.col("performance_seconds") > 0)
    )
)

    # ── STEP 4: Convert performance values ───────────────────────────────
    # For time-based events (km/mi): strip unit suffix, convert HH:MM:SS → seconds
    # For distance-based events (h): strip unit suffix, cast to float km
    df_converted = (
        df_clean
        .withColumn(
            "athlete_performance_clean",
            F.trim(F.regexp_replace(F.col("athlete_performance"), r"\s*(h|km|mi)$", ""))
        )
        .withColumn(
            "performance_seconds",
            F.when(
                F.col("performance_unit") == "h",
                time_to_seconds_udf(F.col("athlete_performance_clean"))
            ).otherwise(None)
        )
        .withColumn(
            "performance_km",
            F.when(
                F.col("performance_unit") == "km",
                F.col("athlete_performance_clean").cast("float")
            ).otherwise(None)
        )
        .drop("athlete_performance_clean")
    )

    # ── STEP 5: Generate surrogate keys using sha2 ────────────────────────
    # sha2 produces a stable 256-bit hash ID based on column values.
    # Preferred over dense_rank() for streaming pipelines — no full table
    # scan needed and IDs remain consistent when new data arrives.
    # Source: recommended by instructor in labbsnack v23
    df_ids = add_hash_id(df_converted, ["event_name"],    "event_id")
    df_ids = add_hash_id(df_ids,       ["athlete_id"],    "athlete_key")
    df_ids = add_hash_id(df_ids,       ["year_of_event"], "year_id")

    # ── STEP 6: Final type casting and standardisation ────────────────────
    # - Extracts numeric speed value from strings like "8.5 km/h"
    # - Casts year of birth and finisher count to integers
    # - Standardises gender and country to uppercase
    # - Derives athlete age at the time of the event
    # - Drops temporary helper columns
    df_silver = (
        df_ids
        .withColumn("athlete_average_speed",
                    F.expr("try_cast(split(athlete_average_speed, ' ')[0] as float)"))
        .withColumn("athlete_year_of_birth",
                    F.col("athlete_year_of_birth").cast("int"))
        .withColumn("event_number_of_finishers",
                    F.col("event_number_of_finishers").cast("int"))
        .withColumn("athlete_gender",
                    F.upper(F.trim(F.col("athlete_gender"))))
        .withColumn("athlete_country",
                    F.upper(F.trim(F.col("athlete_country"))))
        .withColumn("athlete_age_at_event",
                    F.col("year_of_event") - F.col("athlete_year_of_birth"))
        .drop("is_valid_combo", "event_unit", "performance_unit")
    )

    # DLT handles saving — just return the cleaned DataFrame
    return df_silver