-- snowflake sql:
SELECT
  f.value:name AS "Contact",
  f.value:first,
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main
  CAST(p.col:a:info:id AS DOUBLE) AS "id_parsed",
  p.col:b:first,
  p.col:a:info
FROM
  (SELECT
    PARSE_JSON('{"a": {"info": {"id": 101, "first": "John" }, "contact": [{"name": "Alice", "first": "A"}, {"name": "Bob", "first": "B"}]}, "b": {"id": 101, "first": "John"}}')
  ) AS p(col)
, LATERAL FLATTEN(input => p.col:a:contact) AS f;
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> databrickslabs-main

-- databricks sql:
SELECT
  f.value:name AS `Contact`,
  f.value:first,
  CAST(p.col:a.info.id AS DOUBLE) AS `id_parsed`,
  p.col:b.first,
  p.col:a.info
FROM (
  SELECT
   PARSE_JSON('{"a": {"info": {"id": 101, "first": "John" }, "contact": [{"name": "Alice", "first": "A"}, {"name": "Bob", "first": "B"}]}, "b": {"id": 101, "first": "John"}}')
) AS p(col)
, LATERAL VARIANT_EXPLODE(p.col:a.contact) AS f;
<<<<<<< HEAD
=======
=======
  p.value:id::FLOAT AS "id_parsed",
  p.c:value:first,
  p.value
FROM persons_struct p, lateral flatten(input => ${p}.${c}, path => 'contact') f;

-- databricks sql:
SELECT
  f.name AS `Contact`,
  f.first,
  CAST(p.value.id AS DOUBLE) AS `id_parsed`,
  p.c.value.first,
  p.value
FROM persons_struct AS p LATERAL VIEW EXPLODE($p.$c.contact) AS f;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
=======

-- databricks sql:
SELECT
  f.value:name AS `Contact`,
  f.value:first,
  CAST(p.col:a.info.id AS DOUBLE) AS `id_parsed`,
  p.col:b.first,
  p.col:a.info
FROM (
  SELECT
   PARSE_JSON('{"a": {"info": {"id": 101, "first": "John" }, "contact": [{"name": "Alice", "first": "A"}, {"name": "Bob", "first": "B"}]}, "b": {"id": 101, "first": "John"}}')
) AS p(col)
, LATERAL VARIANT_EXPLODE(p.col:a.contact) AS f;
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main
