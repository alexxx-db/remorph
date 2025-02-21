-- snowflake sql:
SELECT ARRAY_AGG(col2) WITHIN GROUP (ORDER BY col2 DESC) FROM test_table;

-- databricks sql:
SELECT
<<<<<<< HEAD
  SORT_ARRAY(ARRAY_AGG(col2), false)
=======
<<<<<<< HEAD
<<<<<<< HEAD
  SORT_ARRAY(ARRAY_AGG(col2), false)
=======
  SORT_ARRAY(ARRAY_AGG(col2), FALSE)
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
=======
  SORT_ARRAY(ARRAY_AGG(col2), false)
>>>>>>> c333275e (Improve coverage test success rate around snowflake's conversion functions (#841))
>>>>>>> databrickslabs-main
FROM test_table;
