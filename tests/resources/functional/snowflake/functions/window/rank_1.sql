-- snowflake sql:
SELECT
  rank() over (
    partition by col1
    order by
      col2
  ) AS rank_col1
FROM
  tabl;

-- databricks sql:
SELECT
  RANK() OVER (
    PARTITION BY col1
    ORDER BY
<<<<<<< HEAD
      col2 ASC NULLS LAST
=======
<<<<<<< HEAD
<<<<<<< HEAD
      col2 ASC NULLS LAST
=======
      col2 ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
=======
      col2 ASC NULLS LAST
>>>>>>> 448ea6a0 (Some window functions does not support window frame conditions (#999))
>>>>>>> databrickslabs-main
  ) AS rank_col1
FROM
  tabl;
