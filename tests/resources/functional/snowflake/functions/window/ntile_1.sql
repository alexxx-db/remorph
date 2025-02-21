<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> databrickslabs-main
-- snowflake sql:
SELECT
  ntile(col1) OVER (
    PARTITION BY col1
    ORDER BY
      col2
  ) AS ntile_col1
FROM
  tabl;

-- databricks sql:
SELECT
  NTILE(col1) OVER (
    PARTITION BY col1
    ORDER BY
      col2 ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS ntile_col1
FROM
  tabl;
<<<<<<< HEAD
=======
=======

=======
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
-- snowflake sql:
SELECT
  ntile(col1) OVER (
    PARTITION BY col1
    ORDER BY
      col2
  ) AS ntile_col1
FROM
  tabl;

-- databricks sql:
<<<<<<< HEAD
SELECT NTILE(col1) OVER (PARTITION BY col1 ORDER BY col2 NULLS LAST) AS ntile_col1 FROM tabl;
>>>>>>> 2c98cd8a ([snowflake] cleanup functional tests (#831))
=======
SELECT
  NTILE(col1) OVER (
    PARTITION BY col1
    ORDER BY
      col2 ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS ntile_col1
FROM
  tabl;
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
>>>>>>> databrickslabs-main
