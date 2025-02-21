-- snowflake sql:
<<<<<<< HEAD
SELECT
  los.value:"objectDomain"::STRING AS object_type,
  los.value:"objectName"::STRING AS object_name,
  cols.value:"columnName"::STRING AS column_name,
  COUNT(DISTINCT lah:"query_token"::STRING) AS n_queries,
  COUNT(DISTINCT lah:"consumer_account_locator"::STRING) AS n_distinct_consumer_accounts
FROM
  (SELECT
    PARSE_JSON('{"query_date": "2022-03-02","query_token": "some_token","consumer_account_locator": "CONSUMER_ACCOUNT_LOCATOR","listing_objects_accessed": [{"objectDomain": "Table","objectName": "DATABASE_NAME.SCHEMA_NAME.TABLE_NAME","columns": [{"columnName": "column1"},{"columnName": "column2"}]}]}') AS lah
  ) AS src,
  LATERAL FLATTEN(input => src.lah:"listing_objects_accessed") AS los,
  LATERAL FLATTEN(input => los.value:"columns") AS cols
WHERE
  los.value:"objectDomain"::STRING IN ('Table', 'View') AND
  src.lah:"query_date"::DATE BETWEEN '2022-03-01' AND '2022-04-30' AND
  los.value:"objectName"::STRING = 'DATABASE_NAME.SCHEMA_NAME.TABLE_NAME' AND
  src.lah:"consumer_account_locator"::STRING = 'CONSUMER_ACCOUNT_LOCATOR'
GROUP BY 1, 2, 3;

-- databricks sql:
SELECT
  CAST(los.value:objectDomain AS STRING) AS object_type,
  CAST(los.value:objectName AS STRING) AS object_name,
  CAST(cols.value:columnName AS STRING) AS column_name,
  COUNT(DISTINCT CAST(lah:query_token AS STRING)) AS n_queries,
  COUNT(DISTINCT CAST(lah:consumer_account_locator AS STRING)) AS n_distinct_consumer_accounts
FROM (
  SELECT
    PARSE_JSON(
      '{"query_date": "2022-03-02","query_token": "some_token","consumer_account_locator": "CONSUMER_ACCOUNT_LOCATOR","listing_objects_accessed": [{"objectDomain": "Table","objectName": "DATABASE_NAME.SCHEMA_NAME.TABLE_NAME","columns": [{"columnName": "column1"},{"columnName": "column2"}]}]}'
    ) AS lah
) AS src
 , LATERAL VARIANT_EXPLODE(src.lah:listing_objects_accessed) AS los
 , LATERAL VARIANT_EXPLODE(los.value:columns) AS cols
WHERE
  CAST(los.value:objectDomain AS STRING) IN ('Table', 'View')
  AND CAST(src.lah:query_date AS DATE) BETWEEN '2022-03-01' AND '2022-04-30'
  AND CAST(los.value:objectName AS STRING) = 'DATABASE_NAME.SCHEMA_NAME.TABLE_NAME'
  AND CAST(src.lah:consumer_account_locator AS STRING) = 'CONSUMER_ACCOUNT_LOCATOR'
GROUP BY
  1,
  2,
  3;
=======
select
  los.value:"objectDomain"::string as object_type,
  los.value:"objectName"::string as object_name,
  cols.value:"columnName"::string as column_name,
  count(distinct lah.query_token) as n_queries,
  count(distinct lah.consumer_account_locator) as n_distinct_consumer_accounts
from SNOWFLAKE.DATA_SHARING_USAGE.LISTING_ACCESS_HISTORY as lah
join lateral flatten(input=>lah.listing_objects_accessed) as los
join lateral flatten(input=>los.value, path=>'columns') as cols
where true
  and los.value:"objectDomain"::string in ('Table', 'View')
  and query_date between '2022-03-01' and '2022-04-30'
  and los.value:"objectName"::string = 'DATABASE_NAME.SCHEMA_NAME.TABLE_NAME'
  and lah.consumer_account_locator = 'CONSUMER_ACCOUNT_LOCATOR'
group by 1,2,3;

-- databricks sql:
SELECT
  CAST(los.objectDomain AS STRING) AS object_type,
  CAST(los.objectName AS STRING) AS object_name,
  CAST(cols.columnName AS STRING) AS column_name,
  COUNT(DISTINCT lah.query_token) AS n_queries,
  COUNT(DISTINCT lah.consumer_account_locator) AS n_distinct_consumer_accounts
FROM SNOWFLAKE.DATA_SHARING_USAGE.LISTING_ACCESS_HISTORY AS lah
LATERAL VIEW EXPLODE(lah.listing_objects_accessed) AS los
LATERAL VIEW EXPLODE(los.value.columns) AS cols
WHERE true AND CAST(los.value.objectDomain AS STRING) IN ('Table', 'View') AND
query_date BETWEEN '2022-03-01' AND '2022-04-30' AND
CAST(los.value.objectName AS STRING) = 'DATABASE_NAME.SCHEMA_NAME.TABLE_NAME' AND
lah.consumer_account_locator = 'CONSUMER_ACCOUNT_LOCATOR' GROUP BY 1, 2, 3;
>>>>>>> b2dc8a94 ([chore] increase coverage by 8% (#827))
