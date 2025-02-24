-- snowflake sql:
SELECT
  taba.col_a,
  taba.col_b,
  last_value(
    CASE
      WHEN taba.col_c IN ('xyz', 'abc') THEN taba.col_d
    END
  ) ignore nulls OVER (
    partition BY taba.col_e
    ORDER BY
      taba.col_f DESC RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS derived_col_a
FROM
  schema_a.table_a taba;

-- databricks sql:
SELECT
  taba.col_a,
  taba.col_b,
  LAST(
    CASE
      WHEN taba.col_c IN ('xyz', 'abc') THEN taba.col_d
    END
  ) IGNORE NULLS OVER (
    PARTITION BY taba.col_e
    ORDER BY
      taba.col_f DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS derived_col_a
FROM
<<<<<<< HEAD
  schema_a.table_a AS taba;
=======
<<<<<<< HEAD
<<<<<<< HEAD
  schema_a.table_a AS taba;
=======
  schema_a.table_a AS taba;
>>>>>>> 77496af5 (Correctly generate `F.fn_name` for builtin PySpark functions (#1037))
=======
  schema_a.table_a AS taba;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
