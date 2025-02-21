-- snowflake sql:
WITH employee_hierarchy AS (
    SELECT
        employee_id,
        manager_id,
        employee_name
    FROM
        employees
    WHERE
        manager_id IS NULL
)
SELECT *
FROM employee_hierarchy;

-- databricks sql:
<<<<<<< HEAD
WITH employee_hierarchy AS (SELECT employee_id, manager_id, employee_name FROM employees WHERE manager_id IS NULL) SELECT * FROM employee_hierarchy;
=======
<<<<<<< HEAD
<<<<<<< HEAD
WITH employee_hierarchy AS (SELECT employee_id, manager_id, employee_name FROM employees WHERE manager_id IS NULL) SELECT * FROM employee_hierarchy;
=======
WITH employee_hierarchy AS (SELECT employee_id, manager_id, employee_name FROM employees WHERE manager_id IS NULL) SELECT * FROM employee_hierarchy;
>>>>>>> 205595c7 (With Recursive (#1000))
=======
WITH employee_hierarchy AS (SELECT employee_id, manager_id, employee_name FROM employees WHERE manager_id IS NULL) SELECT * FROM employee_hierarchy;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
