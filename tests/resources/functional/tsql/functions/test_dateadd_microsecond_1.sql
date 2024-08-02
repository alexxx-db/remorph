-- ## DATEADD with the MICROSECOND keyword
--
-- Databricks SQL does not directly support `DATEADD`, so it is translated to the equivalent
-- INTERVAL increment MICROSECOND

-- tsql sql:
SELECT DATEADD(MICROSECOND, 7, col1) AS add_microsecond_col1 FROM tabl;

-- databricks sql:
<<<<<<< HEAD
<<<<<<< HEAD
SELECT col1 + INTERVAL 7 MICROSECOND AS add_microsecond_col1 FROM tabl;
=======
SELECT (col1 + INTERVAL 7 MICROSECOND) AS add_microsecond_col1 FROM tabl;
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
SELECT col1 + INTERVAL 7 MICROSECOND AS add_microsecond_col1 FROM tabl;
>>>>>>> 24a1e298 (Implement TSQL specific function call mapper (#765))
