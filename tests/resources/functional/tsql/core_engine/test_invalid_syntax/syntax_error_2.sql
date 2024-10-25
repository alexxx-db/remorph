-- tsql sql:
*

-- databricks sql:
/* The following issues were detected:

   Unparsed input - ErrorNode encountered
    Unparsable text: unexpected extra input '*' while parsing a T-SQL batch
<<<<<<< HEAD
    expecting one of: End of batch, Identifier, Select Statement, Statement, '$NODE_ID', '(', ';', 'ACCOUNTADMIN', 'ALERT', 'ARRAY', 'BODY', 'BULK'...
=======
    expecting one of: End of batch, Identifier, Select Statement, Statement, '$NODE_ID', '(', ';', 'BULK', 'EXTERNAL', 'IDENTITY', 'RAW', 'REPLICATE'...
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
    Unparsable text: *
 */
