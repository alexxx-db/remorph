-- snowflake sql:
<<<<<<< HEAD
<<<<<<< HEAD
SELECT
  varchar1,
  CAST(float1 AS STRING) AS float1_as_string,
  CAST(variant1:Loan_Number AS STRING) AS loan_number_as_string
FROM
  (SELECT
    'example_varchar' AS varchar1,
    123.456 AS float1,
    PARSE_JSON('{"Loan_Number": "LN789"}') AS variant1
  ) AS tmp;
=======
select
  varchar1,
  float1::varchar,
  variant1:"Loan Number"::varchar from tmp;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
=======
SELECT
  varchar1,
  CAST(float1 AS STRING) AS float1_as_string,
  CAST(variant1:Loan_Number AS STRING) AS loan_number_as_string
FROM
  (SELECT
    'example_varchar' AS varchar1,
    123.456 AS float1,
    PARSE_JSON('{"Loan_Number": "LN789"}') AS variant1
  ) AS tmp;
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))

-- databricks sql:
SELECT
  varchar1,
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
  CAST(float1 AS STRING) AS float1_as_string,
  CAST(variant1:Loan_Number AS STRING) AS loan_number_as_string
FROM (
  SELECT
    'example_varchar' AS varchar1,
    123.456 AS float1,
    PARSE_JSON('{"Loan_Number": "LN789"}') AS variant1
) AS tmp;
<<<<<<< HEAD
=======
  CAST(float1 AS STRING),
  CAST(variant1.`Loan Number` AS STRING)
FROM tmp;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
