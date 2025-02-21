-- snowflake sql:
SELECT
  nth_value(col1, 42) over (
    partition by col1
    order by
      col2
  ) AS nth_value_col1
FROM
  tabl;

-- databricks sql:
SELECT
  NTH_VALUE(col1, 42) OVER (
    PARTITION BY col1
    ORDER BY
      col2 ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS nth_value_col1
FROM
<<<<<<< HEAD
  tabl;
=======
<<<<<<< HEAD
<<<<<<< HEAD
  tabl;
=======
  tabl;
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
=======
  tabl;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
