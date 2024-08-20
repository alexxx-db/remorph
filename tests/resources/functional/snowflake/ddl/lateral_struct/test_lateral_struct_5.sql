-- snowflake sql:
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

-- databricks sql:
SELECT
  varchar1,
<<<<<<< HEAD
  CAST(float1 AS STRING) AS float1_as_string,
  CAST(variant1:Loan_Number AS STRING) AS loan_number_as_string
FROM (
  SELECT
    'example_varchar' AS varchar1,
    123.456 AS float1,
    PARSE_JSON('{"Loan_Number": "LN789"}') AS variant1
) AS tmp;
=======
  CAST(float1 AS STRING),
  CAST(variant1.`Loan Number` AS STRING)
FROM tmp;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
