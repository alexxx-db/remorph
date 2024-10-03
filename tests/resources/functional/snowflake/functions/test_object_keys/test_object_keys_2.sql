-- snowflake sql:
SELECT OBJECT_KEYS (PARSE_JSON (column1)) AS keys
FROM table
ORDER BY 1;

-- databricks sql:
SELECT
  JSON_OBJECT_KEYS(PARSE_JSON(column1)) AS keys
FROM table
ORDER BY
<<<<<<< HEAD
  1 NULLS LAST;
=======
  1 NULLS LAST;
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
