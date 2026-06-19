# Databricks notebook source
# silver_notebook.py
# Marathos – Silver Layer (DLT pipeline)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType

# ── Helper functions ───────────────────────────────────────────────────────

def get_event_unit(val):
    if val is None: return None
    val = val.lower().strip()
    for unit in ["km", "mi", "h"]:
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

get_event_unit_udf            = F.udf(get_event_unit, StringType())
get_performance_unit_udf      = F.udf(get_performance_unit, StringType())
is_valid_unit_combination_udf = F.udf(is_valid_unit_combination)
time_to_seconds_udf           = F.udf(time_to_seconds, IntegerType())


def add_hash_id(df: DataFrame, source_cols: list, id_col: str) -> DataFrame:
    concat_col = F.concat_ws("||", *[F.col(c).cast("string") for c in source_cols])
    return df.withColumn(id_col, F.sha2(concat_col, 256))    

# European country codes
EUROPE = [
    "SWE","NOR","FIN","DNK","DEU","FRA","ESP","ITA",
    "GBR","BEL","NLD","POL","AUT","CHE","PRT","CZE",
    "HUN","ROU","GRC","HRV","SVK","SVN","SRB","BGR",
    "EST","LVA","LTU","IRL","LUX","ISL","MKD","ALB"
]

# COMMAND ----------

@dlt.table(
    name="marathos.silver.obt",
    comment="Cleaned silver layer – km events in Europe only",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def silver_obt():

    # Step 1: Read from Bronze
    df_bronze = spark.read.table("marathos.bronze.races")

    # Step 2: Classify units for event distance and athlete performance
    df_with_units = (
        df_bronze
        .withColumn("event_unit",       get_event_unit_udf(F.col("event_distance_length")))
        .withColumn("performance_unit", get_performance_unit_udf(F.col("athlete_performance")))
        .withColumn("is_valid_combo",   is_valid_unit_combination_udf(
            F.col("event_unit"),
            F.col("performance_unit")))
    )

    # Step 3: Filter out invalid rows
    # Keeps only: km events, European countries, valid speeds and performances
    df_clean = (
        df_with_units
        .filter(F.col("event_unit") == "km")
        .filter(F.upper(F.trim(F.col("athlete_country"))).isin(EUROPE))
        .filter(F.col("is_valid_combo") == True)
        .filter(F.col("athlete_performance").isNotNull())
        .filter(F.col("event_distance_length").isNotNull())
        .filter(F.col("athlete_id").isNotNull())
        .filter(~F.col("athlete_average_speed").contains(":"))
        .filter(
            F.regexp_replace(F.col("athlete_average_speed"), "[^0-9.]", "").cast("float").between(0.5, 35)
            | F.col("athlete_average_speed").isNull()
        )
    )

    # Step 4: Convert performance to seconds
    # All km events have time format HH:MM:SS
    df_converted = (
        df_clean
        .withColumn(
            "athlete_performance_clean",
            F.trim(F.regexp_replace(F.col("athlete_performance"), r"\s*(h|km|mi)$", ""))
        )
        .withColumn(
            "performance_seconds",
            time_to_seconds_udf(F.col("athlete_performance_clean"))
        )
        .drop("athlete_performance_clean")
        .filter(F.col("performance_seconds") > 0)
    )

    # Step 5: Generate surrogate keys using sha2
    df_ids = add_hash_id(df_converted, ["event_name"],    "event_id")
    df_ids = add_hash_id(df_ids,       ["athlete_id"],    "athlete_key")
    df_ids = add_hash_id(df_ids,       ["year_of_event"], "year_id")

    # Step 6: Final type casting and standardisation
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
        .withColumn("athlete_gender",
                    F.when(F.col("athlete_gender").isin("M", "F", "W"),
                        F.col("athlete_gender"))
                     .otherwise(None))
        .withColumn("athlete_country",
                    F.upper(F.trim(F.col("athlete_country"))))
        .withColumn("athlete_age_at_event",
                    F.col("year_of_event") - F.col("athlete_year_of_birth"))
        .drop("is_valid_combo", "event_unit", "performance_unit")
    )

    return df_silver