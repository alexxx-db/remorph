-- snowflake sql:
SELECT
  col2,
  ARRAYAGG(col4) WITHIN GROUP (ORDER BY col3)
FROM test_table
WHERE col3 > 450000
GROUP BY col2
ORDER BY col2 DESC;

-- databricks sql:
 SELECT
    col2,
    TRANSFORM(
      ARRAY_SORT(
        ARRAY_AGG(NAMED_STRUCT('value', col4, 'sort_by_0', col3)),
        (left, right) -> CASE
                                WHEN left.sort_by_0 < right.sort_by_0 THEN -1
                                WHEN left.sort_by_0 > right.sort_by_0 THEN 1
                                ELSE 0
                            END
      ),
      s -> s.value
    )
  FROM test_table
  WHERE
    col3 > 450000
  GROUP BY
    col2
  ORDER BY
<<<<<<< HEAD
    col2 DESC NULLS FIRST;
=======
<<<<<<< HEAD
<<<<<<< HEAD
    col2 DESC NULLS FIRST;
=======
    col2 DESC NULLS FIRST;
>>>>>>> ea60dbb5 (Add an optimizer rule for SF's WITHIN GROUP clause (#844))
=======
    col2 DESC NULLS FIRST;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
