-- snowflake sql:
SELECT
  tabb.col_a,
  tabb.col_b,
  first_value(
    CASE
      WHEN tabb.col_c IN ('xyz', 'abc') THEN tabb.col_d
    END
  ) ignore nulls OVER (
    partition BY tabb.col_e
    ORDER BY
      tabb.col_f DESC RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS derived_col_a
FROM
  schema_a.table_a taba
  LEFT JOIN schema_b.table_b AS tabb ON taba.col_e = tabb.col_e;

-- databricks sql:
SELECT
  tabb.col_a,
  tabb.col_b,
  FIRST(
    CASE
      WHEN tabb.col_c IN ('xyz', 'abc') THEN tabb.col_d
    END
  ) IGNORE NULLS OVER (
    PARTITION BY tabb.col_e
    ORDER BY
      tabb.col_f DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS derived_col_a
FROM
  schema_a.table_a AS taba
<<<<<<< HEAD
<<<<<<< HEAD
  LEFT JOIN schema_b.table_b AS tabb ON taba.col_e = tabb.col_e;
=======
  LEFT JOIN schema_b.table_b AS tabb ON taba.col_e = tabb.col_e;
>>>>>>> 77496af5 (Correctly generate `F.fn_name` for builtin PySpark functions (#1037))
=======
  LEFT JOIN schema_b.table_b AS tabb ON taba.col_e = tabb.col_e;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
