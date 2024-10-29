-- snowflake sql:
*

-- databricks sql:
/* The following issues were detected:

   Unparsed input - ErrorNode encountered
    Unparsable text: unexpected extra input '*' while parsing a Snowflake batch
<<<<<<< HEAD
<<<<<<< HEAD
    expecting one of: End of batch, Select Statement, Statement, '(', ';', 'CALL', 'COMMENT', 'DECLARE', 'GET', 'LET', 'START', 'WITH'...
=======
    expecting one of: $Identifier, End of batch, Identifier, Select Statement, Statement, '""', '(', 'BODY', 'CALL', 'CHARACTER', 'CURRENT_TIME', 'DECLARE'...
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
=======
    expecting one of: End of batch, Select Statement, Statement, '(', ';', 'CALL', 'COMMENT', 'DECLARE', 'DESC', 'GET', 'LET', 'START'...
>>>>>>> 0acb9a51 (Improve Snowflake grammar to improve syntax error recovery (#1022))
    Unparsable text: *
 */
