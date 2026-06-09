USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.fct_results
  COMMENT "Fact table - gold layer" AS
SELECT
  sha2(concat_ws('||', 
    cast(athlete_id as string), 
    cast(event_id as string), 
    cast(year_of_event as string)
  ), 256)                     AS result_id,
  event_id,
  athlete_key,
  year_id,
  performance_seconds,
  performance_km,
  athlete_average_speed,
  athlete_age_at_event
FROM marathos.silver.obt;