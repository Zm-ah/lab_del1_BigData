CREATE OR REFRESH MATERIALIZED VIEW marathos.silver.obt
AS
SELECT *,
  CASE 
    WHEN `event_distance_length` RLIKE '\\d+km' THEN 'distance'
    WHEN `event_distance_length` RLIKE '\\d+mi' THEN 'distance'
    WHEN `event_distance_length` RLIKE '\\d+h' THEN 'time'
    ELSE 'unknown'
  END AS event_type
FROM marathos.bronze.races
WHERE event_distance_length NOT RLIKE '\\d+d'
AND athlete_year_of_birth > 1900;




df.columns