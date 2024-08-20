-- snowflake sql:
SELECT
<<<<<<< HEAD
  tt.col:id AS tax_transaction_id,
  CAST(tt.col:responseBody.isMpfState AS BOOLEAN) AS is_mpf_state,
  REGEXP_REPLACE(tt.col:requestBody.deliveryLocation.city, '-', '') AS delivery_city,
  REGEXP_REPLACE(tt.col:requestBody.store.storeAddress.zipCode, '=', '') AS store_zipcode
FROM (
  SELECT
    PARSE_JSON('{"id": 1, "responseBody": { "isMpfState": true }, "requestBody": { "deliveryLocation": { "city": "New-York" }, "store": {"storeAddress": {"zipCode": "100=01"}}}}')
    AS col
) AS tt;

-- databricks sql:
SELECT
  tt.col:id AS tax_transaction_id,
  CAST(tt.col:responseBody.isMpfState AS BOOLEAN) AS is_mpf_state,
  REGEXP_REPLACE(tt.col:requestBody.deliveryLocation.city, '-', '') AS delivery_city,
  REGEXP_REPLACE(tt.col:requestBody.store.storeAddress.zipCode, '=', '') AS store_zipcode
FROM (
  SELECT
    PARSE_JSON('{"id": 1, "responseBody": { "isMpfState": true }, "requestBody": { "deliveryLocation": { "city": "New-York" }, "store": {"storeAddress": {"zipCode": "100=01"}}}}')
    AS col
) AS tt;
=======
  tt.id AS tax_transaction_id,
  cast(tt.response_body:"isMpfState" AS BOOLEAN) AS is_mpf_state,
  REGEXP_REPLACE(tt.request_body:"deliveryLocation":"city", '""', '') AS delivery_city,
  REGEXP_REPLACE(tt.request_body:"store":"storeAddress":"zipCode", '""', '') AS
  store_zipcode
FROM tax_table tt;

-- databricks sql:
SELECT
  tt.id AS tax_transaction_id,
  CAST(tt.response_body.isMpfState AS BOOLEAN) AS is_mpf_state,
  REGEXP_REPLACE(tt.request_body.deliveryLocation.city, '""', '') AS delivery_city,
  REGEXP_REPLACE(tt.request_body.store.storeAddress.zipCode, '""', '') AS store_zipcode
FROM tax_table AS tt;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
