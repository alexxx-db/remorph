-- snowflake sql:
ALTER TABLE employees ADD COLUMN first_name VARCHAR(50);

-- databricks sql:
<<<<<<< HEAD
ALTER TABLE employees ADD COLUMN first_name STRING;
=======
ALTER TABLE employees ADD COLUMN first_name STRING;
>>>>>>> c1b4afd1 (bug fix for alter table add multiple columns (#1179))
