-- snowflake sql:
SELECT
  lead(col1) OVER (
    PARTITION BY col1
    ORDER BY
      col2
  ) AS lead_col1
FROM
  tabl;

-- databricks sql:
SELECT
  LEAD(col1) OVER (
    PARTITION BY col1
    ORDER BY
<<<<<<< HEAD
      col2 ASC NULLS LAST
=======
      col2 ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
  ) AS lead_col1
FROM
  tabl;
