-- ## LAG ignoring NULL values
--
-- The LAG function is identical in TSql and Databricks when IGNORING or RESPECTING NULLS (default).

-- tsql sql:
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> databrickslabs-main
SELECT col1, col2, LAG(col2, 1, 0) IGNORE NULLS OVER (ORDER BY col2 DESC) AS lag_value FROM tabl;

-- databricks sql:
SELECT col1, col2, LAG(col2, 1, 0) IGNORE NULLS OVER (ORDER BY col2 DESC) AS lag_value FROM tabl;
<<<<<<< HEAD
=======
=======
SELECT col1, col2, LAG(col2, 1, 0) OVER (ORDER BY col2 DESC) IGNORE NULLS AS lag_value FROM tabl;

-- databricks sql:
SELECT col1, col2, LAG(col2, 1, 0) OVER (ORDER BY col2 DESC) IGNORE NULLS AS lag_value FROM tabl;
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
SELECT col1, col2, LAG(col2, 1, 0) IGNORE NULLS OVER (ORDER BY col2 DESC) AS lag_value FROM tabl;

-- databricks sql:
SELECT col1, col2, LAG(col2, 1, 0) IGNORE NULLS OVER (ORDER BY col2 DESC) AS lag_value FROM tabl;
>>>>>>> 94c141e8 (Make coverage test fail CI in case of failure (#908))
>>>>>>> databrickslabs-main
