-- see https://docs.snowflake.com/en/sql-reference/functions/approx_percentile_accumulate

CREATE OR REPLACE TABLE test_table2 (c1 INTEGER);
-- Insert data.
INSERT INTO test_table2 (c1) VALUES (5), (6), (7), (8);