<<<<<<< HEAD
<<<<<<< HEAD
=======
-- Because of the let command allows LET? it means that error recovery can't happen easily in Snowflake
-- because teh parser sees everything as valid.
-- TODO: TRy to rejig the parser so that error recovery can work athe top level batch
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
=======
>>>>>>> 0acb9a51 (Improve Snowflake grammar to improve syntax error recovery (#1022))
-- snowflake sql:
* ;
SELECT 1 ;
SELECT A B FROM C ;

-- databricks sql:
/* The following issues were detected:

   Unparsed input - ErrorNode encountered
<<<<<<< HEAD
<<<<<<< HEAD
    Unparsable text: unexpected extra input '*' while parsing a Snowflake batch
    expecting one of: End of batch, Select Statement, Statement, '(', ';', 'CALL', 'COMMENT', 'DECLARE', 'GET', 'LET', 'START', 'WITH'...
    Unparsable text: *
 */
SELECT
  1;
SELECT
  A AS B
FROM
  C;
=======
    Unparsable text: '*' was unexpected while parsing a Snowflake batch
    expecting one of: $Identifier, End of batch, Identifier, Select Statement, Statement, '""', '(', 'BODY', 'CALL', 'CHARACTER', 'CURRENT_TIME', 'DECLARE'...
=======
    Unparsable text: unexpected extra input '*' while parsing a Snowflake batch
    expecting one of: End of batch, Select Statement, Statement, '(', ';', 'CALL', 'COMMENT', 'DECLARE', 'DESC', 'GET', 'LET', 'START'...
>>>>>>> 0acb9a51 (Improve Snowflake grammar to improve syntax error recovery (#1022))
    Unparsable text: *
 */
<<<<<<< HEAD
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
=======
SELECT
  1;
SELECT
  A AS B
FROM
  C;
>>>>>>> 0acb9a51 (Improve Snowflake grammar to improve syntax error recovery (#1022))
