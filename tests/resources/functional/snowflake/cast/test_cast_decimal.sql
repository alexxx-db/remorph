-- snowflake sql:
SELECT
    12345::DECIMAL(10, 2) AS decimal_val,
    12345::NUMBER(10, 2) AS number_val,
    12345::NUMERIC(10, 2) AS numeric_val,
    12345::BIGINT AS bigint_val

-- databricks sql:
SELECT
  CAST(12345 AS DECIMAL(10, 2)) AS decimal_val,
  CAST(12345 AS DECIMAL(10, 2)) AS number_val,
  CAST(12345 AS DECIMAL(10, 2)) AS numeric_val,
<<<<<<< HEAD
<<<<<<< HEAD
  CAST(12345 AS DECIMAL(38, 0)) AS bigint_val;
=======
  CAST(12345 AS DECIMAL(38, 0)) AS bigint_val;
>>>>>>> ae9cf349 ([snowflake] fix data type mapping and explain `UnparsedType` (#810))
=======
  CAST(12345 AS DECIMAL(38, 0)) AS bigint_val;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
