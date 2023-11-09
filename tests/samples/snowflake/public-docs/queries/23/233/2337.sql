-- see https://docs.snowflake.com/en/sql-reference/functions/st_asgeojson

select st_asgeojson(g)::varchar
    from geospatial_table
    order by id;