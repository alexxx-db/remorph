
-- snowflake sql:
SELECT TO_TIME('2018-05-15', 'yyyy-MM-dd');

-- databricks sql:
<<<<<<< HEAD
<<<<<<< HEAD:tests/resources/functional/snowflake/core_engine/functions/conversion/test_to_time_1.sql
SELECT DATE_FORMAT(TO_TIMESTAMP('2018-05-15', 'yyyy-MM-dd'), 'HH:mm:ss');
=======
SELECT TO_TIMESTAMP('2018-05-15', 'yyyy-MM-dd');
>>>>>>> 6de49dae (Improve coverage around snowflake functions (#860)):tests/resources/functional/snowflake/functions/conversion/test_to_time/test_to_time_1.sql
=======
SELECT DATE_FORMAT(TO_TIMESTAMP('2018-05-15', 'yyyy-MM-dd'), 'HH:mm:ss');
>>>>>>> 9dcc986e (Fix implementation of Snowflake's TO_TIME/TO_TIMESTAMP functions (#1218))
