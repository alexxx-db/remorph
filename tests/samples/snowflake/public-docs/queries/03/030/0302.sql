-- see https://docs.snowflake.com/en/sql-reference/functions/regexp_like

CREATE OR REPLACE TABLE cities(city varchar(20));
INSERT INTO cities VALUES
    ('Sacramento'),
    ('San Francisco'),
    ('San Jose'),
    (null);