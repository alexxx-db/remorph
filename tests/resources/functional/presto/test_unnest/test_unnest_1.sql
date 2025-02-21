-- presto sql:
SELECT
  *
FROM
  default.sync_gold
  CROSS JOIN UNNEST(engine_waits) t (col1, col2)
WHERE
  ("cardinality"(engine_waits) > 0);

-- databricks sql:
SELECT
  *
FROM
  default.sync_gold LATERAL VIEW EXPLODE(engine_waits) As col1,
  col2
WHERE
<<<<<<< HEAD
  (SIZE(engine_waits) > 0);
=======
<<<<<<< HEAD
<<<<<<< HEAD
  (SIZE(engine_waits) > 0);
=======
  (SIZE(engine_waits) > 0);
>>>>>>> 3163132f (Handling presto Unnest cross join to Databricks lateral view (#1209))
=======
  (SIZE(engine_waits) > 0);
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
