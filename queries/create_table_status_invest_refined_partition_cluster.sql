CREATE OR REPLACE TABLE
  `project.status_invest_refined_zone.status_invest_2019_2023`
CLUSTER BY
  quarter AS (
  SELECT
    * EXCEPT(year,
      quarter),
    CONCAT(REGEXP_EXTRACT(STRING(year), r'^\d{4}'), "-", quarter) AS quarter
  FROM
    `project.status_invest_raw_zone.status_invest` )