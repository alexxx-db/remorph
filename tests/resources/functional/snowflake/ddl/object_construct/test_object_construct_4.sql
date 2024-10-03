-- snowflake sql:
SELECT OBJECT_CONSTRUCT('Key_One', PARSE_JSON('NULL'), 'Key_Two', NULL, 'Key_Three', 'null') as obj;

-- databricks sql:
<<<<<<< HEAD
<<<<<<< HEAD
=======
SELECT
  STRUCT(
    FROM_JSON('NULL', {JSON_COLUMN_SCHEMA}) AS Key_One,
    NULL AS Key_Two,
    'null' AS Key_Three
  ) AS obj;

-- experimental sql:
>>>>>>> 6de49dae (Improve coverage around snowflake functions (#860))
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
SELECT STRUCT(PARSE_JSON('NULL') AS Key_One, NULL AS Key_Two, 'null' AS Key_Three) AS obj;
