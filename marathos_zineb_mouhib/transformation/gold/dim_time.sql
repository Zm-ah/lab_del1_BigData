USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_time
  COMMENT "Dim table - gold layer" AS
SELECT
  year_id,
  MAX_BY(year_of_event, year_of_event) AS year_of_event
FROM marathos.silver.obt
GROUP BY year_id;