
-- snowflake sql:
SELECT MONTHNAME(TO_TIMESTAMP('2015-04-03 10:00:00')) AS MONTH;

-- databricks sql:
<<<<<<< HEAD:tests/resources/functional/snowflake/core_engine/functions/dates/test_monthname_1.sql
SELECT DATE_FORMAT(TO_TIMESTAMP('2015-04-03 10:00:00'), 'MMM') AS MONTH;
=======
SELECT DATE_FORMAT(cast('2015-04-03 10:00:00' as TIMESTAMP), 'MMM') AS MONTH;
>>>>>>> 2c98cd8a ([snowflake] cleanup functional tests (#831)):tests/resources/functional/snowflake/functions/dates/monthname/test_monthname_1.sql
