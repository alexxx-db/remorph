-- snowflake sql:
SELECT
  first_value(col1) over (
    partition by col1
    order by
      col2
  ) AS first_value_col1
FROM
  tabl;

-- databricks sql:
SELECT
  FIRST(col1) OVER (
    PARTITION BY col1
    ORDER BY
      col2 ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
  ) AS first_value_col1
FROM
<<<<<<< HEAD
<<<<<<< HEAD
  tabl;
=======
  tabl;
>>>>>>> 77496af5 (Correctly generate `F.fn_name` for builtin PySpark functions (#1037))
=======
  tabl;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
