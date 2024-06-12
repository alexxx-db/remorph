-- ## SET_BIT
--
<<<<<<< HEAD
<<<<<<< HEAD
-- The SET_BIT function does not exist in Databricks SQL, so we must use bit functions
=======
-- The SET_BIT function is identical in TSql and Databricks.
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
-- The SET_BIT function is identical in TSql and Databricks, save for a renaming of the function.
>>>>>>> 98d95eb5 (TSQL: Improve function coverage (#455))
--
-- tsql sql:
SELECT SET_BIT(42, 7);

-- databricks sql:
<<<<<<< HEAD
<<<<<<< HEAD
SELECT 42 | SHIFTLEFT(1, 7);
=======
SELECT SET_BIT(42, 7);
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
SELECT SETBIT(42, 7);
>>>>>>> 98d95eb5 (TSQL: Improve function coverage (#455))
