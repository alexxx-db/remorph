-- ## WITH cte SELECT
--
-- The use of CTEs is generally the same in Databricks SQL as TSQL but there are some differences with
-- nesting CTE support.
--
-- tsql sql:
WITH cte AS (SELECT * FROM t) SELECT * FROM cte
<<<<<<< HEAD

-- databricks sql:
WITH cte AS (SELECT * FROM t) SELECT * FROM cte;
=======
<<<<<<< HEAD
<<<<<<< HEAD

-- databricks sql:
WITH cte AS (SELECT * FROM t) SELECT * FROM cte;
<<<<<<< HEAD
=======
GO
=======

>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
-- databricks sql:
WITH cte AS (SELECT * FROM t) SELECT * FROM cte
>>>>>>> 596a140d (TSQL: Implement WITH CTE (#443))
=======
>>>>>>> 94c141e8 (Make coverage test fail CI in case of failure (#908))
>>>>>>> databrickslabs-main
