-- snowflake sql:
SELECT
  symbol,
  exchange,
  shares,
  ROW_NUMBER() OVER (
    PARTITION BY exchange
    ORDER BY
      shares
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
<<<<<<< HEAD
<<<<<<< HEAD
      shares ASC NULLS LAST
=======
      shares ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
=======
      shares ASC NULLS LAST
>>>>>>> 448ea6a0 (Some window functions does not support window frame conditions (#999))
  ) AS row_number
FROM
  trades;
