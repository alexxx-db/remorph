-- snowflake sql:
SELECT DAYNAME(TO_TIMESTAMP('2015-04-03 10:00:00')) AS MONTH;

-- databricks sql:
<<<<<<< HEAD
=======
<<<<<<<< HEAD:tests/resources/functional/snowflake/functions/dates/dayname/test_dayname_1.sql
>>>>>>> 9dcc986e (Fix implementation of Snowflake's TO_TIME/TO_TIMESTAMP functions (#1218))
<<<<<<<< HEAD:tests/resources/functional/snowflake/core_engine/functions/dates/test_dayname_1.sql
SELECT DATE_FORMAT(TO_TIMESTAMP('2015-04-03 10:00:00'), 'E') AS MONTH;
========
SELECT DATE_FORMAT(cast('2015-04-03 10:00:00' as TIMESTAMP), 'E') AS MONTH;
>>>>>>>> 2c98cd8a ([snowflake] cleanup functional tests (#831)):tests/resources/functional/snowflake/functions/dates/dayname/test_dayname_1.sql
<<<<<<< HEAD
=======
========
SELECT DATE_FORMAT(TO_TIMESTAMP('2015-04-03 10:00:00'), 'E') AS MONTH;
>>>>>>>> 9dcc986e (Fix implementation of Snowflake's TO_TIME/TO_TIMESTAMP functions (#1218)):tests/resources/functional/snowflake/core_engine/functions/dates/test_dayname_1.sql
>>>>>>> 9dcc986e (Fix implementation of Snowflake's TO_TIME/TO_TIMESTAMP functions (#1218))
