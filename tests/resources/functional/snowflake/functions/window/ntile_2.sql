-- snowflake sql:
SELECT
  ntile(col1) OVER (
    PARTITION BY col1
    ORDER BY
      col2 DESC RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS ntile_col1
FROM
  tabl;

-- databricks sql:
SELECT
  NTILE(col1) OVER (
    PARTITION BY col1
    ORDER BY
      col2 DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS ntile_col1
FROM
<<<<<<< HEAD
<<<<<<< HEAD
  tabl;
=======
  tabl;
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
=======
  tabl;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
