-- snowflake sql:
SELECT
  last_value(col1) AS last_value_col1
FROM
  tabl;

-- databricks sql:
SELECT
  LAST(col1) AS last_value_col1
FROM
<<<<<<< HEAD
  tabl;
=======
<<<<<<< HEAD
<<<<<<< HEAD
  tabl;
=======
  tabl;
>>>>>>> 77496af5 (Correctly generate `F.fn_name` for builtin PySpark functions (#1037))
=======
  tabl;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
