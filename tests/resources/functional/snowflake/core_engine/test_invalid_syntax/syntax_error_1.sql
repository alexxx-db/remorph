-- Note that here we have two commas in the select clause and although in other circumstances,
<<<<<<< HEAD
-- the parser could notice that is an additional comma, in this case it is not able to do so because
=======
-- the parser coudl notice that is an additional comma, in this case it is not able to do so because
>>>>>>> 2145b51f (Improve error recovery code to preserve text in AST (#1014))
-- what can be in between the comma is just about anything. Then because any ID is accepted as
-- possibly being some kind of command, then the parser has to assume that the following tokens
-- are some valid command.
-- Hence this error is thrown by a no viable alternative at input ',' and the parser recovers to something
-- that looks like it is a valid command because of the let rule where LET is optional and the next token
-- is an ID, which is therefore predicted and we will accumulate a lot of erroneous errors.

-- snowflake sql:
select col1,, col2 from table_name;

-- databricks sql:
/* The following issues were detected:

<<<<<<< HEAD
   Unparsed input - ErrorNode encountered
    Unparsable text: select col1,,
 */
/* The following issues were detected:

   Unparsed input - ErrorNode encountered
    Unparsable text: select
    Unparsable text: col1
    Unparsable text: ,
    Unparsable text: ,
    Unparsable text: col2
    Unparsable text: from
    Unparsable text: table_name
    Unparsable text: parser recovered by ignoring: select col1,, col2 from table_name;
 */
=======
   Unparsed input - ErrorNode encountered:

   Unparsable text: select col1,,


*/
/* The following issues were detected:

   Unparsed input - ErrorNode encountered:

   Unparsable text: select

Unparsable text: parser recovered by ignoring: select col1


*/
/* The following issues were detected:

   Unimplemented visitor accept in class SnowflakeCommandBuilder:

   Mocked string
*/
/* The following issues were detected:

   Unimplemented visitor accept in class SnowflakeCommandBuilder:

   col1,,
*/
/* The following issues were detected:

   Unimplemented visitor accept in class SnowflakeCommandBuilder:

   Mocked string
*/
/* The following issues were detected:

   Unimplemented visitor accept in class SnowflakeCommandBuilder:

   col2 from
*/
/* The following issues were detected:

   Unimplemented visitor accept in class SnowflakeCommandBuilder:

   Mocked string
*/
/* The following issues were detected:

   Unimplemented visitor accept in class SnowflakeCommandBuilder:

   table_name
*/
>>>>>>> 2145b51f (Improve error recovery code to preserve text in AST (#1014))
