-- snowflake sql:
INSERT INTO foo (c1, c2, c3)
    SELECT x, y, z FROM bar WHERE x > z AND y = 'qux';

-- databricks sql:
INSERT INTO foo (
  c1,
  c2,
  c3
)
SELECT
  x,
  y,
  z
FROM bar
WHERE
<<<<<<< HEAD
  x > z AND y = 'qux';
=======
<<<<<<< HEAD
<<<<<<< HEAD
  x > z AND y = 'qux';
=======
  x > z AND y = 'qux';
>>>>>>> a989e005 ([snowflake] map more functions to Databricks SQL (#826))
=======
  x > z AND y = 'qux';
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
