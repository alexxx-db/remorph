-- snowflake sql:
SELECT
  dense_rank() OVER (
    PARTITION BY col1
    ORDER BY
      col2
  ) AS dense_rank_col1
FROM
  tabl;

-- databricks sql:
SELECT
  DENSE_RANK() OVER (
    PARTITION BY col1
    ORDER BY
<<<<<<< HEAD
      col2 ASC NULLS LAST
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      col2 ASC NULLS LAST
=======
      col2 ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING
>>>>>>> 8888a6a1 (Handling window frame of rank-related functions in snowflake (#833))
=======
      col2 ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
>>>>>>> ea60dbb5 (Add an optimizer rule for SF's WITHIN GROUP clause (#844))
=======
      col2 ASC NULLS LAST
>>>>>>> 448ea6a0 (Some window functions does not support window frame conditions (#999))
>>>>>>> databrickslabs-main
  ) AS dense_rank_col1
FROM
  tabl;
