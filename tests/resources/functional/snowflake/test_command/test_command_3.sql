
-- snowflake sql:
!set exit_on_error = true;
SELECT !(2 = 2) AS always_false;


-- databricks sql:
<<<<<<< HEAD
<<<<<<< HEAD
-- !set exit_on_error = true;
SELECT !(2 = 2) AS always_false
=======
-- snowsql command:!'set exit_on_error = true';
SELECT !(2 = 2) AS always_false
>>>>>>> 96c6764d (Added Translation Support for `!` as `commands` and `&` for `Parameters` (#771))
=======
-- !set exit_on_error = true;
SELECT !(2 = 2) AS always_false
>>>>>>> 1ab2645f (extra ";" generation has been taken care for Bang command (#858))
