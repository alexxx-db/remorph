-- snowflake sql:
SELECT
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main
  d.col:display_position::NUMBER AS display_position,
  i.value:attributes::VARCHAR AS attributes,
  CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)) AS created_at,
  i.value:prop::FLOAT AS prop,
  d.col:candidates AS candidates
FROM
  (
    SELECT
      PARSE_JSON('{"display_position": 123, "impressions": [{"attributes": "some_attributes", "prop": 12.34}, {"attributes": "other_attributes", "prop": 56.78}], "candidates": "some_candidates"}') AS col,
      '2024-08-28' AS event_date,
      'store.replacements_view' AS event_name
  ) AS d,
  LATERAL FLATTEN(input => d.col:impressions, outer => true) AS i
WHERE
  d.event_date = '2024-08-28'
  AND d.event_name IN ('store.replacements_view');
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> databrickslabs-main

-- databricks sql:
SELECT
  CAST(d.col:display_position AS DECIMAL(38, 0)) AS display_position,
  CAST(i.value:attributes AS STRING) AS attributes,
  CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS created_at,
  CAST(i.value:prop AS DOUBLE) AS prop,
  d.col:candidates AS candidates
FROM (
  SELECT
    PARSE_JSON('{"display_position": 123, "impressions": [{"attributes": "some_attributes", "prop": 12.34}, {"attributes": "other_attributes", "prop": 56.78}], "candidates": "some_candidates"}') AS col,
    '2024-08-28' AS event_date,
    'store.replacements_view' AS event_name
) AS d
, LATERAL VARIANT_EXPLODE_OUTER(d.col:impressions) AS i
WHERE
  d.event_date = '2024-08-28' AND d.event_name IN ('store.replacements_view');
<<<<<<< HEAD
=======
=======
  d.value:display_position::NUMBER as display_position,
  i.value:attributes::VARCHAR as attributes,
  cast(current_timestamp() as timestamp_ntz(9)) as created_at,
  i.value:prop::FLOAT as prop,
  candidates
FROM dwh.vw  d,
LATERAL FLATTEN (INPUT => d.impressions, OUTER => true) i
WHERE event_date = '{start_date}' and event_name in ('store.replacements_view');
=======
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))

-- databricks sql:
SELECT
  CAST(d.col:display_position AS DECIMAL(38, 0)) AS display_position,
  CAST(i.value:attributes AS STRING) AS attributes,
  CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS created_at,
<<<<<<< HEAD
  CAST(i.prop AS DOUBLE) AS prop, candidates
FROM dwh.vw AS d LATERAL VIEW OUTER EXPLODE(d.impressions) AS i
WHERE event_date = '{start_date}' AND event_name IN ('store.replacements_view');
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
=======
  CAST(i.value:prop AS DOUBLE) AS prop,
  d.col:candidates AS candidates
FROM (
  SELECT
    PARSE_JSON('{"display_position": 123, "impressions": [{"attributes": "some_attributes", "prop": 12.34}, {"attributes": "other_attributes", "prop": 56.78}], "candidates": "some_candidates"}') AS col,
    '2024-08-28' AS event_date,
    'store.replacements_view' AS event_name
) AS d
, LATERAL VARIANT_EXPLODE_OUTER(d.col:impressions) AS i
WHERE
  d.event_date = '2024-08-28' AND d.event_name IN ('store.replacements_view');
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
>>>>>>> databrickslabs-main
