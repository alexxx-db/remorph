-- snowflake sql:
select count(*) from &TEST_USER.EMP_TBL;

-- databricks sql:
<<<<<<< HEAD
select count(*) from $TEST_USER.EMP_TBL;
=======
select count(*) from $TEST_USER.EMP_TBL;
>>>>>>> 96c6764d (Added Translation Support for `!` as `commands` and `&` for `Parameters` (#771))
