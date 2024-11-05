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
WITH employee_hierarchy AS (SELECT employee_id, manager_id, employee_name FROM employees WHERE manager_id IS NULL) SELECT * FROM employee_hierarchy;
>>>>>>> 205595c7 (With Recursive (#1000))
