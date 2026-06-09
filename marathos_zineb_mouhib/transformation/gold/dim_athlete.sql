USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Dim table - gold layer" AS
SELECT
  athlete_key,
  MAX_BY(athlete_id, year_of_event)             AS athlete_id,
  MAX_BY(athlete_country, year_of_event)        AS athlete_country,
  MAX_BY(athlete_gender, year_of_event)         AS athlete_gender,
  MAX_BY(athlete_year_of_birth, year_of_event)  AS athlete_year_of_birth,
  MAX_BY(athlete_age_category, year_of_event)   AS athlete_age_category
FROM marathos.silver.obt
GROUP BY athlete_key;