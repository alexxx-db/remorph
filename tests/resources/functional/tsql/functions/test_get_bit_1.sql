-- ## GET_BIT
--
<<<<<<< HEAD
-- The GET_BIT function is not supported in Databricks SQL. The following example
-- shows how to convert it to a Databricks equivalent
=======
<<<<<<< HEAD
<<<<<<< HEAD
-- The GET_BIT function is not supported in Databricks SQL. The following example
-- shows how to convert it to a Databricks equivalent
=======
-- The GET_BIT function is identical in TSql and Databricks.
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
-- The GET_BIT function is not supported in Databricks SQL. The following example
-- shows how to convert it to a Databricks equivalent
>>>>>>> 98d95eb5 (TSQL: Improve function coverage (#455))
>>>>>>> databrickslabs-main
--
-- tsql sql:
SELECT GET_BIT(42, 7);

<<<<<<< HEAD
-- databricks sql:
SELECT GETBIT(42, 7);
=======
<<<<<<< HEAD
<<<<<<< HEAD
-- databricks sql:
<<<<<<< HEAD
<<<<<<< HEAD
SELECT GETBIT(42, 7);
=======
-- GET_BIT sql:
=======
-- databricks sql:
>>>>>>> bb50ebb6 (Better coverage reports (#456))
SELECT BIT_COUNT(42);
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
SELECT getbit(42, 7);;
>>>>>>> 98d95eb5 (TSQL: Improve function coverage (#455))
=======
SELECT GETBIT(42, 7);
>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
>>>>>>> databrickslabs-main
