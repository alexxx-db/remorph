-- see https://docs.snowflake.com/en/sql-reference/functions/parse_json

CREATE or replace TABLE jdemo2 (varchar1 VARCHAR, variant1 VARIANT,
    variant2 VARIANT);
INSERT INTO jdemo2 (varchar1) VALUES ('{"PI":3.14}');
UPDATE jdemo2 SET variant1 = PARSE_JSON(varchar1);