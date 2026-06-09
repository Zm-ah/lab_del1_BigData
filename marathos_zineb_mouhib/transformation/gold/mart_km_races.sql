USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_top_countries_km
  COMMENT "Serving view - top countries in km races" AS
SELECT
  a.athlete_country,
  COUNT(*)                              AS total_finishers,
  ROUND(AVG(f.athlete_average_speed), 2) AS avg_speed_kmh,
  ROUND(AVG(f.performance_seconds) / 3600.0, 2) AS avg_time_hours
FROM marathos.gold.fct_results f
LEFT JOIN marathos.gold.dim_athlete a ON f.athlete_key = a.athlete_key
LEFT JOIN marathos.gold.dim_event e   ON f.event_id = e.event_id
WHERE e.event_type = 'kilometers'
GROUP BY a.athlete_country
ORDER BY total_finishers DESC;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_fastest_athletes_km
  COMMENT "Serving view - fastest athletes in km races" AS
SELECT
  a.athlete_id,
  a.athlete_country,
  a.athlete_gender,
  e.event_name,
  e.event_distance_length,
  f.performance_seconds,
  ROUND(f.performance_seconds / 3600.0, 2) AS performance_hours,
  f.athlete_average_speed
FROM marathos.gold.fct_results f
LEFT JOIN marathos.gold.dim_athlete a ON f.athlete_key = a.athlete_key
LEFT JOIN marathos.gold.dim_event e   ON f.event_id = e.event_id
WHERE e.event_type = 'kilometers'
  AND f.performance_seconds IS NOT NULL
ORDER BY f.performance_seconds ASC
LIMIT 1000;