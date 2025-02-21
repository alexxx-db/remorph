-- snowflake sql:
CREATE TABLE employee (employee_id INT,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  birth_date DATE,
  hire_date DATE,
  salary DECIMAL(10, 2),
<<<<<<< HEAD
  department_id INT,
  remarks VARIANT)
=======
<<<<<<< HEAD
<<<<<<< HEAD
  department_id INT,
  remarks VARIANT)
=======
  department_id INT)
>>>>>>> 2c98cd8a ([snowflake] cleanup functional tests (#831))
=======
  department_id INT,
  remarks VARIANT)
>>>>>>> cb75b481 (Add Variant Support (#998))
>>>>>>> databrickslabs-main
;

-- databricks sql:
CREATE TABLE employee (
  employee_id DECIMAL(38, 0),
  first_name STRING NOT NULL,
  last_name STRING NOT NULL,
  birth_date DATE,
  hire_date DATE,
  salary DECIMAL(10, 2),
<<<<<<< HEAD
  department_id DECIMAL(38, 0),
  remarks VARIANT
=======
<<<<<<< HEAD
<<<<<<< HEAD
  department_id DECIMAL(38, 0),
  remarks VARIANT
=======
  department_id DECIMAL(38, 0)
>>>>>>> 2c98cd8a ([snowflake] cleanup functional tests (#831))
=======
  department_id DECIMAL(38, 0),
  remarks VARIANT
>>>>>>> cb75b481 (Add Variant Support (#998))
>>>>>>> databrickslabs-main
);
