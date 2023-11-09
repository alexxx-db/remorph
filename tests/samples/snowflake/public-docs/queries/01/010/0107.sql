-- see https://docs.snowflake.com/en/sql-reference/functions/st_union

ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT = 'WKT';

SELECT ST_UNION(
  TO_GEOGRAPHY('POLYGON((0 0, 1 0, 2 1, 1 2, 2 3, 1 4, 0 4, 0 0))'),
  TO_GEOGRAPHY('POLYGON((3 0, 3 4, 2 4, 1 3, 2 2, 1 1, 2 0, 3 0))')
) AS union_of_objects;