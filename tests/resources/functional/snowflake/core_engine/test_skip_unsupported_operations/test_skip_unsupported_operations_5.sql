
-- snowflake sql:
CREATE STREAM mystream ON TABLE mytable;

-- databricks sql:
/* The following issues were detected:

<<<<<<< HEAD
   CREATE STREAM UNSUPPORTED
    CREATE STREAM mystream ON TABLE mytable
 */
=======
<<<<<<< HEAD
<<<<<<< HEAD
   CREATE STREAM UNSUPPORTED
    CREATE STREAM mystream ON TABLE mytable
 */
=======
   CREATE STREAM UNSUPPORTED:

   CREATE STREAM mystream ON TABLE mytable
*/
>>>>>>> 2145b51f (Improve error recovery code to preserve text in AST (#1014))
=======
   CREATE STREAM UNSUPPORTED
    CREATE STREAM mystream ON TABLE mytable
 */
>>>>>>> 34e4a547 (Implement text gatherers for additional error types (#1020))
>>>>>>> databrickslabs-main
