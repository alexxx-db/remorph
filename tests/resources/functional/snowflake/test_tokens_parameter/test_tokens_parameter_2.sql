-- snowflake sql:
select emp_id from abc.emp where emp_id = $ids;

-- databricks sql:
<<<<<<< HEAD
select emp_id from abc.emp where emp_id = $ids;
=======
select emp_id from abc.emp where emp_id = $ids;
>>>>>>> 96c6764d (Added Translation Support for `!` as `commands` and `&` for `Parameters` (#771))
