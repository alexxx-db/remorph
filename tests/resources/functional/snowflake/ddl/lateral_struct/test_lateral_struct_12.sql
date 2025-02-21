-- snowflake sql:
SELECT
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main
   verticals.index AS index,
   verticals.value AS array_val
 FROM
   (
     select ARRAY_CONSTRUCT('value1', 'value2', 'value3') as col
   ) AS sample_data(array_column),
   LATERAL FLATTEN(input => sample_data.array_column, OUTER => true) AS verticals;
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
  verticals.index AS index,
  verticals.value AS value
FROM
  sample_data,
<<<<<<< HEAD
  LATERAL FLATTEN(input => array_column, OUTER => TRUE ) AS verticals;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
=======
  LATERAL FLATTEN(input => array_column, OUTER => true ) AS verticals;
>>>>>>> c333275e (Improve coverage test success rate around snowflake's conversion functions (#841))
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main

-- databricks sql:
SELECT
  verticals.index AS index,
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main
  verticals.value AS array_val
FROM (
  SELECT
    ARRAY('value1', 'value2', 'value3') AS col
) AS sample_data(array_column)
 LATERAL VIEW OUTER POSEXPLODE(sample_data.array_column) verticals AS index, value;
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
  verticals.value AS value
FROM sample_data
  LATERAL VIEW OUTER POSEXPLODE(array_column) verticals AS index, value;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main
