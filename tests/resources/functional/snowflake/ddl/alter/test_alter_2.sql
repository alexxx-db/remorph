-- snowflake sql:
ALTER TABLE employees ADD COLUMN first_name VARCHAR(50) NOT NULL, age INT, hire_date DATE;

-- databricks sql:
<<<<<<< HEAD
ALTER TABLE employees ADD COLUMN first_name STRING NOT NULL, age DECIMAL(38, 0), hire_date DATE;
=======
<<<<<<< HEAD
<<<<<<< HEAD
ALTER TABLE employees ADD COLUMN first_name STRING NOT NULL, age DECIMAL(38, 0), hire_date DATE;
=======
ALTER TABLE employees ADD COLUMN first_name STRING NOT NULL, age DECIMAL(38, 0), hire_date DATE;
>>>>>>> c1b4afd1 (bug fix for alter table add multiple columns (#1179))
=======
ALTER TABLE employees ADD COLUMN first_name STRING NOT NULL, age DECIMAL(38, 0), hire_date DATE;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
