-- snowflake sql:
SELECT
  dense_rank() OVER (
    PARTITION BY col1
    ORDER BY
      col2 DESC RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS dense_rank_col1
FROM
  tabl;

-- databricks sql:
SELECT
  DENSE_RANK() OVER (
    PARTITION BY col1
    ORDER BY
      col2 DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS dense_rank_col1
FROM
<<<<<<< HEAD
  tabl;
=======
  tabl;
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
