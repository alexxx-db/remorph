
-- snowflake sql:
ALTER SESSION SET QUERY_TAG = 'tag1';

-- databricks sql:
/* The following issues were detected:

<<<<<<< HEAD
   Unknown ALTER command variant
    ALTER SESSION SET QUERY_TAG = 'tag1'
 */
=======
   Unknown ALTER command variant:

   ALTER SESSION SET QUERY_TAG = 'tag1'
*/
>>>>>>> 2145b51f (Improve error recovery code to preserve text in AST (#1014))
