-- The TSql parser is much better than the Snowflake one because it does not have the
-- super ambiguous LET statement that makes it impossible for batch level queries to
-- recover from syntax errors. Note here how TSQL has stupid grammar though as A and B
-- could be columns with a missing ',' but we cannot know.
-- tsql sql:
* ;
SELECT 1 ;
SELECT A B FROM C ;

-- databricks sql:
/* The following issues were detected:

   Unparsed input - ErrorNode encountered
    Unparsable text: unexpected extra input '*' while parsing a T-SQL batch
<<<<<<< HEAD
    expecting one of: End of batch, Identifier, Select Statement, Statement, '$NODE_ID', '(', ';', 'ACCOUNTADMIN', 'ALERT', 'ARRAY', 'BODY', 'BULK'...
=======
<<<<<<< HEAD
<<<<<<< HEAD
    expecting one of: End of batch, Identifier, Select Statement, Statement, '$NODE_ID', '(', ';', 'ACCOUNTADMIN', 'ALERT', 'ARRAY', 'BODY', 'BULK'...
=======
    expecting one of: End of batch, Identifier, Select Statement, Statement, '$NODE_ID', '(', ';', 'BULK', 'EXTERNAL', 'IDENTITY', 'RAW', 'REPLICATE'...
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
=======
    expecting one of: End of batch, Identifier, Select Statement, Statement, '$NODE_ID', '(', ';', 'ACCOUNTADMIN', 'ALERT', 'ARRAY', 'BODY', 'BULK'...
>>>>>>> c6baa47b (Generic stored procedure parsing (#1047))
>>>>>>> databrickslabs-main
    Unparsable text: *
 */
SELECT
  1;
SELECT
  A AS B
FROM
  C;
