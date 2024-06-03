-- DATEADD with the YY keyword
--
-- Databricks SQl does not directly support DATEADD, so it is translated to the equivalent
-- ADD_MONTHS function with the number of months multiplied by 12.


-- tsql sql:
SELECT DATEADD(yy, 2, col1) AS add_years_col1 FROM tabl;

-- databricks sql:
SELECT ADD_MONTHS(col1, 2*12) AS add_years_col1 FROM tabl;
