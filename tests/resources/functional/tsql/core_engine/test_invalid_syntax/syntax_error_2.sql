-- tsql sql:
*

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
