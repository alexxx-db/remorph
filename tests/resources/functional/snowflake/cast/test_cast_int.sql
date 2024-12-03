-- snowflake sql:
SELECT
  123::BYTEINT AS byteint_val,
  123::SMALLINT AS smallint_val,
  123::INT AS int_val,
  123::INTEGER AS integer_val,
  123::BIGINT AS bigint_val,
  123::TINYINT AS tinyint_val

-- databricks sql:
SELECT
  CAST(123 AS DECIMAL(38, 0)) AS byteint_val,
  CAST(123 AS DECIMAL(38, 0)) AS smallint_val,
  CAST(123 AS DECIMAL(38, 0)) AS int_val,
  CAST(123 AS DECIMAL(38, 0)) AS integer_val,
  CAST(123 AS DECIMAL(38, 0)) AS bigint_val,
<<<<<<< HEAD
<<<<<<< HEAD
  CAST(123 AS TINYINT) AS tinyint_val;
=======
  CAST(123 AS TINYINT) AS tinyint_val;
>>>>>>> ae9cf349 ([snowflake] fix data type mapping and explain `UnparsedType` (#810))
=======
  CAST(123 AS TINYINT) AS tinyint_val;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
