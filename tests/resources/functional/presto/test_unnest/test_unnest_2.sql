-- presto sql:
SELECT
  day,
  build_number,
  error,
  error_count
FROM
  sch.tab
  CROSS JOIN UNNEST(CAST(extra AS map(varchar, integer))) e (error, error_count)
WHERE
  (
    (event_type = 'fp_daemon_crit_errors_v2')
    AND (error_count > 0)
  );

-- databricks sql:
SELECT
  day,
  build_number,
  error,
  error_count
FROM
  sch.tab LATERAL VIEW EXPLODE(CAST(extra AS MAP<VARCHAR, INT>)) As error,
  error_count
WHERE
  (
    (
      event_type = 'fp_daemon_crit_errors_v2'
    )
    AND (error_count > 0)
<<<<<<< HEAD
<<<<<<< HEAD
  );
=======
  );
>>>>>>> 3163132f (Handling presto Unnest cross join to Databricks lateral view (#1209))
=======
  );
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
