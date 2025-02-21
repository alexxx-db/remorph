-- ## SET_BIT
--
<<<<<<< HEAD
-- The SET_BIT function is identical in TSql and Databricks, save for a renaming of the function.
=======
<<<<<<< HEAD
<<<<<<< HEAD
-- The SET_BIT function is identical in TSql and Databricks, save for a renaming of the function.
=======
-- The SET_BIT function is identical in TSql and Databricks.
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
-- The SET_BIT function is identical in TSql and Databricks, save for a renaming of the function.
>>>>>>> 98d95eb5 (TSQL: Improve function coverage (#455))
>>>>>>> databrickslabs-main
--
-- tsql sql:
SELECT SET_BIT(42, 7, 0);

-- databricks sql:
<<<<<<< HEAD
SELECT 42 & -1 ^ SHIFTLEFT(1, 7) | SHIFTRIGHT(0, 7);
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
SELECT 42 & -1 ^ SHIFTLEFT(1, 7) | SHIFTRIGHT(0, 7);
=======
SELECT SET_BIT(42, 7, 0);
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
SELECT SETBIT(42, 7, 0);
>>>>>>> 98d95eb5 (TSQL: Improve function coverage (#455))
=======
SELECT 42 & -1 ^ SHIFTLEFT(1, 7) | SHIFTRIGHT(0, 7);
>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
>>>>>>> databrickslabs-main
