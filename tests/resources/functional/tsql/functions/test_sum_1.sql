-- ## SUM
--
-- The SUM function is identical in TSQL and Databricks

-- tsql sql:
SELECT sum(col1) AS sum_col1 FROM tabl;

-- databricks sql:
<<<<<<< HEAD
SELECT SUM(col1) AS sum_col1 FROM tabl;
=======
SELECT sum(col1) AS sum_col1 FROM tabl;
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
