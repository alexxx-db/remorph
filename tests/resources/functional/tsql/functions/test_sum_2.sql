-- ## SUM with DISTINCT
--
-- The SUM function is identical in TSQL and Databricks

-- tsql sql:
SELECT sum(DISTINCT col1) AS sum_col1 FROM tabl;

-- databricks sql:
<<<<<<< HEAD
SELECT SUM(DISTINCT col1) AS sum_col1 FROM tabl;
=======
<<<<<<< HEAD
<<<<<<< HEAD
SELECT SUM(DISTINCT col1) AS sum_col1 FROM tabl;
=======
SELECT sum(DISTINCT col1) AS sum_col1 FROM tabl;
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
SELECT SUM(DISTINCT col1) AS sum_col1 FROM tabl;
>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
>>>>>>> databrickslabs-main
