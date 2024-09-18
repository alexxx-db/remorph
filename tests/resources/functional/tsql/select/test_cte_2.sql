-- ## WITH cte SELECT
--
-- The use of CTEs is generally the same in Databricks SQL as TSQL but there are some differences with
-- nesting CTE support.
--
-- tsql sql:

WITH cteTable1 (col1, col2, col3count)
         AS
         (
             SELECT col1, fred, COUNT(OrderDate) AS counter
             FROM Table1
         ),
     cteTable2 (colx, coly, colxcount)
         AS
         (
             SELECT col1, fred, COUNT(OrderDate) AS counter
             FROM Table2
         )
SELECT col2, col1, col3count, cteTable2.colx, cteTable2.coly, cteTable2.colxcount
FROM cteTable1
<<<<<<< HEAD
<<<<<<< HEAD
=======
GO
>>>>>>> 596a140d (TSQL: Implement WITH CTE (#443))
=======
>>>>>>> 94c141e8 (Make coverage test fail CI in case of failure (#908))

-- databricks sql:
WITH cteTable1 (col1, col2, col3count)
         AS
         (
             SELECT col1, fred, COUNT(OrderDate) AS counter
             FROM Table1
         ),
     cteTable2 (colx, coly, colxcount)
         AS
         (
             SELECT col1, fred, COUNT(OrderDate) AS counter
             FROM Table2
         )
SELECT col2, col1, col3count, cteTable2.colx, cteTable2.coly, cteTable2.colxcount
<<<<<<< HEAD
<<<<<<< HEAD
FROM cteTable1;
=======
FROM cteTable1
>>>>>>> 596a140d (TSQL: Implement WITH CTE (#443))
=======
FROM cteTable1;
>>>>>>> 94c141e8 (Make coverage test fail CI in case of failure (#908))
