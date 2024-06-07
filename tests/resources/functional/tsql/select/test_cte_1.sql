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
GO
-- databricks sql:
WITH cte AS (SELECT * FROM t) SELECT * FROM cte
>>>>>>> 596a140d (TSQL: Implement WITH CTE (#443))
