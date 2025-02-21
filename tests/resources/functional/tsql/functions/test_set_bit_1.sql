-- ## SET_BIT
--
<<<<<<< HEAD
-- The SET_BIT function does not exist in Databricks SQL, so we must use bit functions
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
-- The SET_BIT function does not exist in Databricks SQL, so we must use bit functions
=======
-- The SET_BIT function is identical in TSql and Databricks.
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
-- The SET_BIT function is identical in TSql and Databricks, save for a renaming of the function.
>>>>>>> 98d95eb5 (TSQL: Improve function coverage (#455))
=======
-- The SET_BIT function does not exist in Databricks SQL, so we must use bit functions
>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
>>>>>>> databrickslabs-main
--
-- tsql sql:
SELECT SET_BIT(42, 7);

-- databricks sql:
<<<<<<< HEAD
SELECT 42 | SHIFTLEFT(1, 7);
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
SELECT 42 | SHIFTLEFT(1, 7);
=======
SELECT SET_BIT(42, 7);
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
SELECT SETBIT(42, 7);
>>>>>>> 98d95eb5 (TSQL: Improve function coverage (#455))
=======
SELECT 42 | SHIFTLEFT(1, 7);
>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
>>>>>>> databrickslabs-main
