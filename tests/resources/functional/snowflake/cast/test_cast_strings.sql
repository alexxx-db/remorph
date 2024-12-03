-- snowflake sql:
SELECT
  '12345'::VARCHAR(10) AS varchar_val,
  '12345'::STRING AS string_val,
  '12345'::TEXT AS text_val,
  'A'::CHAR(1) AS char_val,
  'A'::CHARACTER(1) AS character_val

-- databricks sql:
SELECT
  CAST('12345' AS STRING) AS varchar_val,
  CAST('12345' AS STRING) AS string_val,
  CAST('12345' AS STRING) AS text_val,
  CAST('A' AS STRING) AS char_val,
<<<<<<< HEAD
<<<<<<< HEAD
  CAST('A' AS STRING) AS character_val;
=======
  CAST('A' AS STRING) AS character_val;
>>>>>>> ae9cf349 ([snowflake] fix data type mapping and explain `UnparsedType` (#810))
=======
  CAST('A' AS STRING) AS character_val;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
