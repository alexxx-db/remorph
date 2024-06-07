-- ## SET_BIT
--
<<<<<<< HEAD
-- The SET_BIT function does not exist in Databricks SQL, so we must use bit functions
=======
-- The SET_BIT function is identical in TSql and Databricks.
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
--
-- tsql sql:
SELECT SET_BIT(42, 7);

-- databricks sql:
<<<<<<< HEAD
SELECT 42 | SHIFTLEFT(1, 7);
=======
SELECT SET_BIT(42, 7);
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
