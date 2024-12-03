-- snowflake sql:
SELECT
  12345.678::DOUBLE AS double_val,
  12345.678::DOUBLE PRECISION AS double_precision_val,
  12345.678::FLOAT AS float_val,
  12345.678::FLOAT4 AS float4_val,
  12345.678::FLOAT8 AS float8_val,
  12345.678::REAL AS real_val

-- databricks sql:
SELECT
  CAST(12345.678 AS DOUBLE) AS double_val,
  CAST(12345.678 AS DOUBLE) AS double_precision_val,
  CAST(12345.678 AS DOUBLE) AS float_val,
  CAST(12345.678 AS DOUBLE) AS float4_val,
  CAST(12345.678 AS DOUBLE) AS float8_val,
<<<<<<< HEAD
<<<<<<< HEAD
  CAST(12345.678 AS DOUBLE) AS real_val;
=======
  CAST(12345.678 AS DOUBLE) AS real_val;
>>>>>>> ae9cf349 ([snowflake] fix data type mapping and explain `UnparsedType` (#810))
=======
  CAST(12345.678 AS DOUBLE) AS real_val;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
