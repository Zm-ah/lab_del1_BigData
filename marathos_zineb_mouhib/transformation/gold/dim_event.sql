USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Dim table - gold layer" AS
SELECT
  event_id,
  MAX_BY(event_name, year_of_event)             AS event_name,
  MAX_BY(event_distance_length, year_of_event)  AS event_distance_length,
  MAX_BY(
    CASE 
      WHEN event_distance_length LIKE '%km' THEN 'kilometers'
      WHEN event_distance_length LIKE '%mi' THEN 'miles'
      ELSE 'other'
    END, year_of_event)                          AS event_type,
  MAX_BY(event_number_of_finishers, year_of_event) AS event_number_of_finishers,
  MAX_BY(event_dates, year_of_event)            AS event_dates
FROM marathos.silver.obt
GROUP BY event_id;