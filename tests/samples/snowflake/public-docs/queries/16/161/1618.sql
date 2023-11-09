-- see https://docs.snowflake.com/en/sql-reference/operators-subquery

SELECT department_id
FROM departments d
WHERE d.department_id NOT IN (SELECT e.department_id
                              FROM employees e);