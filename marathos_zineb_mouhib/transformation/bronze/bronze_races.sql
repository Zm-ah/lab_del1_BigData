CREATE OR REFRESH MATERIALIZED VIEW races
AS
SELECT 
  `Year of event`             AS year_of_event,
  `Event dates`               AS event_dates,
  `Event name`                AS event_name,
  `Event distance/length`     AS event_distance_length,
  `Event number of finishers` AS event_number_of_finishers,
  `Athlete performance`       AS athlete_performance,
  `Athlete club`              AS athlete_club,
  `Athlete country`           AS athlete_country,
  `Athlete year of birth`     AS athlete_year_of_birth,
  `Athlete gender`            AS athlete_gender,
  `Athlete age category`      AS athlete_age_category,
  `Athlete average speed`     AS athlete_average_speed,
  `Athlete ID`                AS athlete_id
FROM read_files(
  '/Volumes/marathos/bronze/raw/TWO_CENTURIES_OF_UM_RACES.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);