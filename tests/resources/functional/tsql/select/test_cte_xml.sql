-- ## WITH XMLWORKSPACES
--
-- Databricks SQL does not currently support XML workspaces, so for now, we cover the syntax without recommending
-- a translation.
--
-- tsql sql:
WITH XMLNAMESPACES ('somereference' as namespace)
SELECT col1 as 'namespace:col1',
       col2 as 'namespace:col2'
FROM  Table1
WHERE col2 = 'xyz'
FOR XML RAW ('namespace:namespace'), ELEMENTS;

-- databricks sql:
<<<<<<< HEAD
WITH XMLNAMESPACES ('somereference' as namespace) SELECT col1 as 'namespace:col1', col2 as 'namespace:col2' FROM  Table1 WHERE col2 = 'xyz' FOR XML RAW ('namespace:namespace'), ELEMENTS;
=======
<<<<<<< HEAD
<<<<<<< HEAD
WITH XMLNAMESPACES ('somereference' as namespace) SELECT col1 as 'namespace:col1', col2 as 'namespace:col2' FROM  Table1 WHERE col2 = 'xyz' FOR XML RAW ('namespace:namespace'), ELEMENTS;
=======

WITH XMLNAMESPACES ('somereference' as namespace)
SELECT col1 as 'namespace:col1',
       col2      as 'namespace:col2'
FROM  Table1
WHERE col2 = 'xyz'
FOR XML RAW ('namespace:namespace'), ELEMENTS;
>>>>>>> b6255e0b (TSQL: Update the SELECT statement to support XML workspaces (#451))
=======
WITH XMLNAMESPACES ('somereference' as namespace) SELECT col1 as 'namespace:col1', col2 as 'namespace:col2' FROM  Table1 WHERE col2 = 'xyz' FOR XML RAW ('namespace:namespace'), ELEMENTS;
>>>>>>> 8c55bd59 (TSQL: Improve transpilation coverage (#766))
>>>>>>> databrickslabs-main
