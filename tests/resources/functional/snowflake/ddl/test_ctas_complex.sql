-- snowflake sql:
CREATE TABLE employee_summary AS
SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    e.salary,
    d.department_name,
    CASE
        WHEN e.salary > 100000 THEN 'High'
        WHEN e.salary BETWEEN 50000 AND 100000 THEN 'Medium'
        ELSE 'Low'
    END AS salary_range,
    YEAR(e.hire_date) AS hire_year
FROM
    employee e
JOIN
    department d ON e.department_id = d.department_id
;

-- databricks sql:
CREATE TABLE employee_summary AS
SELECT
  e.employee_id,
  e.first_name,
  e.last_name,
  e.salary,
  d.department_name,
  CASE
    WHEN e.salary > 100000
    THEN 'High'
    WHEN e.salary BETWEEN 50000 AND 100000
    THEN 'Medium'
    ELSE 'Low'
  END AS salary_range,
  YEAR(e.hire_date) AS hire_year
FROM employee AS e
JOIN department AS d
<<<<<<< HEAD
  ON e.department_id = d.department_id ;
=======
<<<<<<< HEAD
<<<<<<< HEAD
  ON e.department_id = d.department_id ;
=======
  ON e.department_id = d.department_id ;
>>>>>>> 6a06bc4e (Basic CTAS Implementation (#968))
=======
  ON e.department_id = d.department_id ;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
