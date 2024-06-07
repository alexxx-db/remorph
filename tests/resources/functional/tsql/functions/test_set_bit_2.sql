-- ## SET_BIT
--
<<<<<<< HEAD
-- The SET_BIT function is identical in TSql and Databricks, save for a renaming of the function.
=======
-- The SET_BIT function is identical in TSql and Databricks.
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
--
-- tsql sql:
SELECT SET_BIT(42, 7, 0);

-- databricks sql:
<<<<<<< HEAD
SELECT 42 & -1 ^ SHIFTLEFT(1, 7) | SHIFTRIGHT(0, 7);
=======
SELECT SET_BIT(42, 7, 0);
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
