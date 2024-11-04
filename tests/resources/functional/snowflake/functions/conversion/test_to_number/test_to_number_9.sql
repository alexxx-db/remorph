
-- snowflake sql:
select TO_NUMBER(EXPR) from test_tbl;

-- databricks sql:
<<<<<<< HEAD
SELECT CAST(EXPR AS DECIMAL(38, 0)) FROM test_tbl;
=======
SELECT CAST(EXPR AS DECIMAL(38, 0)) FROM test_tbl;
>>>>>>> cbea0c07 (TO_NUMBER/TO_DECIMAL/TO_NUMERIC without precision and scale (#1053))
