-- snowflake sql:
CREATE OR REPLACE TABLE employee as SELECT employee_id, name FROM employee_stage;

-- databricks sql:
<<<<<<< HEAD
CREATE OR REPLACE TABLE employee as SELECT employee_id, name FROM employee_stage;
=======
CREATE OR REPLACE TABLE employee as SELECT employee_id, name FROM employee_stage;
>>>>>>> b96aa6af (Create Command Extended (#1033))
