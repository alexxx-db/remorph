
-- snowflake sql:
!set exit_on_error = true;

-- databricks sql:
/* The following issues were detected:

<<<<<<< HEAD
   Unknown command in SnowflakeAstBuilder.visitSnowSqlCommand
    !set exit_on_error = true;
 */
=======
   Unknown command in SnowflakeAstBuilder.visitSnowSqlCommand:

   !set exit_on_error = true;
*/
>>>>>>> 2145b51f (Improve error recovery code to preserve text in AST (#1014))
