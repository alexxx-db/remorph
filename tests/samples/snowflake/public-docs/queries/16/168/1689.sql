-- see https://docs.snowflake.com/en/sql-reference/functions/to_xml

SELECT object_col_1,
       TO_XML(object_col_1)
    FROM xml_03;