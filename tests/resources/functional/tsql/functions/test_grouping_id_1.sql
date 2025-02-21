-- ## GROUPING_ID
--
<<<<<<< HEAD
-- GROUPING_ID is directly equivalent in Databricks SQL and TSQL.
=======
<<<<<<< HEAD
<<<<<<< HEAD
-- GROUPING_ID is directly equivalent in Databricks SQL and TSQL.
=======
-- There is no direct equivalent of GROUPING_ID in Databricks SQL. The following suggested translation
-- will achieve the same effect, though there may be other approaches to this.
--
-- One such approach may be:
-- ```sql
-- SELECT (GROUPING(col1) << 1) + GROUPING(colb) AS grouping_id
-- FROM t1
-- GROUP BY col1, col2 WITH CUBE;
-- ```
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
-- GROUPING_ID is directly equivalent in Databricks SQL and TSQL.
>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
>>>>>>> databrickslabs-main

-- tsql sql:
SELECT GROUPING_ID(col1, col2) FROM t1 GROUP BY CUBE(col1, col2);

-- databricks sql:
<<<<<<< HEAD
SELECT GROUPING_ID(col1, col2) FROM t1 GROUP BY CUBE(col1, col2);
=======
<<<<<<< HEAD
<<<<<<< HEAD
SELECT GROUPING_ID(col1, col2) FROM t1 GROUP BY CUBE(col1, col2);
=======
SELECT CASE
           WHEN GROUPING(col1) = 1 AND GROUPING(col2) = 1 THEN 3
           WHEN GROUPING(col1) = 1 THEN 1
           WHEN GROUPING(col2) = 1 THEN 2
           ELSE 0
           END AS GROUPING_ID
FROM t1
GROUP BY CUBE(col1, col2);
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
=======
SELECT GROUPING_ID(col1, col2) FROM t1 GROUP BY CUBE(col1, col2);
>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
>>>>>>> databrickslabs-main
