-- snowflake sql:
SELECT
  '12:34:56'::TIME AS time_val,
  '2024-01-01 12:34:56'::TIMESTAMP AS timestamp_val,
  '2024-01-01 12:34:56 +00:00'::TIMESTAMP_LTZ AS timestamp_ltz_val,
  '2024-01-01 12:34:56'::TIMESTAMP_NTZ AS timestamp_ntz_val,
  '2024-01-01 12:34:56 +00:00'::TIMESTAMP_TZ AS timestamp_tz_val

-- databricks sql:
SELECT
  CAST('12:34:56' AS TIMESTAMP) AS time_val,
  CAST('2024-01-01 12:34:56' AS TIMESTAMP) AS timestamp_val,
  CAST('2024-01-01 12:34:56 +00:00' AS TIMESTAMP) AS timestamp_ltz_val,
  CAST('2024-01-01 12:34:56' AS TIMESTAMP_NTZ) AS timestamp_ntz_val,
<<<<<<< HEAD
  CAST('2024-01-01 12:34:56 +00:00' AS TIMESTAMP) AS timestamp_tz_val;
=======
<<<<<<< HEAD
<<<<<<< HEAD
  CAST('2024-01-01 12:34:56 +00:00' AS TIMESTAMP) AS timestamp_tz_val;
=======
  CAST('2024-01-01 12:34:56 +00:00' AS TIMESTAMP) AS timestamp_tz_val;
>>>>>>> ae9cf349 ([snowflake] fix data type mapping and explain `UnparsedType` (#810))
=======
  CAST('2024-01-01 12:34:56 +00:00' AS TIMESTAMP) AS timestamp_tz_val;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
