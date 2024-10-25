
-- snowflake sql:
ALTER SESSION SET QUERY_TAG = 'tag1';

-- databricks sql:
/* The following issues were detected:

<<<<<<< HEAD
<<<<<<< HEAD
   Unknown ALTER command variant
    ALTER SESSION SET QUERY_TAG = 'tag1'
 */
=======
   Unknown ALTER command variant:

   ALTER SESSION SET QUERY_TAG = 'tag1'
*/
>>>>>>> 2145b51f (Improve error recovery code to preserve text in AST (#1014))
=======
   Unknown ALTER command variant
    ALTER SESSION SET QUERY_TAG = 'tag1'
 */
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
