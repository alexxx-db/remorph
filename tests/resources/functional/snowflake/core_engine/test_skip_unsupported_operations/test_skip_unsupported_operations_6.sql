
-- snowflake sql:
ALTER STREAM mystream SET COMMENT = 'New comment for stream';

-- databricks sql:
/* The following issues were detected:

<<<<<<< HEAD
   Unknown ALTER command variant
    ALTER STREAM mystream SET COMMENT = 'New comment for stream'
 */
=======
<<<<<<< HEAD
<<<<<<< HEAD
   Unknown ALTER command variant
    ALTER STREAM mystream SET COMMENT = 'New comment for stream'
 */
=======
   Unknown ALTER command variant:

   ALTER STREAM mystream SET COMMENT = 'New comment for stream'
*/
>>>>>>> 2145b51f (Improve error recovery code to preserve text in AST (#1014))
=======
   Unknown ALTER command variant
    ALTER STREAM mystream SET COMMENT = 'New comment for stream'
 */
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
>>>>>>> databrickslabs-main
