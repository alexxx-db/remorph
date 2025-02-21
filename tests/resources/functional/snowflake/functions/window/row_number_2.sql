-- snowflake sql:
SELECT
  symbol,
  exchange,
  shares,
  ROW_NUMBER() OVER (
    PARTITION BY exchange
    ORDER BY
      shares DESC RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS row_number
FROM
  trades;

-- databricks sql:
SELECT
  symbol,
  exchange,
  shares,
  ROW_NUMBER() OVER (
    PARTITION BY exchange
    ORDER BY
      shares DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS row_number
FROM
<<<<<<< HEAD
  trades;
=======
<<<<<<< HEAD
<<<<<<< HEAD
  trades;
=======
  trades;
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
=======
  trades;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
