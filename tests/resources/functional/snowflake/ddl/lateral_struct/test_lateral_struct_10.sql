-- snowflake sql:
SELECT
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
    tt.id,
    lit.value: details AS details
FROM VALUES
    (1, '{"order": {"id": 101,"items": [{"item_id": "A1","quantity": 2,"details": {"color": "red"}},{"item_id": "B2","quantity": 5,"details": {"color": "blue"}}]}}'),
    (2, '{"order": {"id": 202,"items": [{"item_id": "C3","quantity": 4,"details": {"color": "green", "size": "L"}},{"item_id": "D4","quantity": 3,"details": {"color": "yellow", "size": "M"}}]}}')
AS tt(id, resp)
, LATERAL FLATTEN(input => PARSE_JSON(tt.resp):order.items) AS lit
<<<<<<< HEAD

-- databricks sql:
SELECT
tt.id,
lit.value:details AS details
FROM VALUES
    (1, '{"order": {"id": 101,"items": [{"item_id": "A1","quantity": 2,"details": {"color": "red"}},{"item_id": "B2","quantity": 5,"details": {"color": "blue"}}]}}'),
    (2, '{"order": {"id": 202,"items": [{"item_id": "C3","quantity": 4,"details": {"color": "green", "size": "L"}},{"item_id": "D4","quantity": 3,"details": {"color": "yellow", "size": "M"}}]}}')
AS tt(id, resp)
, LATERAL VARIANT_EXPLODE(PARSE_JSON(tt.resp):order.items) AS lit;
<<<<<<< HEAD
=======
tt.id
, PARSE_JSON(tt.details)
FROM prod.public.table tt
,  LATERAL FLATTEN (input=> PARSE_JSON(PARSE_JSON(tt.resp):items)) AS lit
,  LATERAL FLATTEN (input=> parse_json(lit.value:"details")) AS ltd;

-- databricks sql:
SELECT
  tt.id, FROM_JSON(tt.details, {TT.DETAILS_SCHEMA}) FROM prod.public.table AS tt
  LATERAL VIEW EXPLODE(FROM_JSON(FROM_JSON(tt.resp, {TT.RESP_SCHEMA}).items, {JSON_COLUMN_SCHEMA})) AS lit
  LATERAL VIEW EXPLODE(FROM_JSON(lit.value.details, {JSON_COLUMN_SCHEMA})) AS ltd;

-- experimental sql:
SELECT
  tt.id, PARSE_JSON(tt.details) FROM prod.public.table AS tt
  LATERAL VIEW EXPLODE(PARSE_JSON(PARSE_JSON(tt.resp).items)) AS lit
  LATERAL VIEW EXPLODE(PARSE_JSON(lit.value.details)) AS ltd;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
=======

-- databricks sql:
SELECT
tt.id,
lit.value:details AS details
FROM VALUES
    (1, '{"order": {"id": 101,"items": [{"item_id": "A1","quantity": 2,"details": {"color": "red"}},{"item_id": "B2","quantity": 5,"details": {"color": "blue"}}]}}'),
    (2, '{"order": {"id": 202,"items": [{"item_id": "C3","quantity": 4,"details": {"color": "green", "size": "L"}},{"item_id": "D4","quantity": 3,"details": {"color": "yellow", "size": "M"}}]}}')
AS tt(id, resp)
, LATERAL VARIANT_EXPLODE(PARSE_JSON(tt.resp):order.items) AS lit;
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
=======
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
