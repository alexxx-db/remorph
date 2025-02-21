-- snowflake sql:
SELECT
  taba.col_a,
  taba.col_b,
  nth_value(
    CASE
      WHEN taba.col_c IN ('xyz', 'abc') THEN taba.col_d
    END,
    42
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
  NTH_VALUE(
    CASE
      WHEN taba.col_c IN ('xyz', 'abc') THEN taba.col_d
    END,
    42
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
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
=======
  schema_a.table_a AS taba;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
