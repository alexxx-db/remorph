-- Note that here we have two commas in the select clause and teh TSQL grammar not
-- quite as bad as the Snowflake grammar, is able to see that it can delete

-- tsql sql:
select col1,, col2 from table_name;

-- databricks sql:
SELECT
<<<<<<< HEAD
  col1,
=======
    col1,
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
/* The following issues were detected:

   Unparsed input - ErrorNode encountered
    Unparsable text: unexpected extra input ',' while parsing a SELECT statement
<<<<<<< HEAD
    expecting one of: $Currency, 'String', @@Reference, @Local, Float, Identifier, Integer, Operator, Real, Statement, '$ACTION', '$NODE_ID'...
    Unparsable text: ,
 */
  col2
=======
    expecting one of: $Currency, 'String', @@Reference, @Local, Float, Identifier, Integer, Operator, Real, '$ACTION', '$NODE_ID', '$PARTITION'...
    Unparsable text: ,
 */
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
FROM
    table_name;
