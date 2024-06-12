-- ## GET_BIT
--
<<<<<<< HEAD
-- The GET_BIT function is not supported in Databricks SQL. The following example
-- shows how to convert it to a Databricks equivalent
=======
-- The GET_BIT function is identical in TSql and Databricks.
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
--
-- tsql sql:
SELECT GET_BIT(42, 7);

<<<<<<< HEAD
<<<<<<< HEAD
-- databricks sql:
SELECT GETBIT(42, 7);
=======
-- GET_BIT sql:
=======
-- databricks sql:
>>>>>>> bb50ebb6 (Better coverage reports (#456))
SELECT BIT_COUNT(42);
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
