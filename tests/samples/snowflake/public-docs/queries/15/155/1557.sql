-- see https://docs.snowflake.com/en/sql-reference/functions/bitnot

SELECT bit1, bit2, BITNOT(bit1), BITNOT(bit2)
  FROM bits
  ORDER BY bit1;