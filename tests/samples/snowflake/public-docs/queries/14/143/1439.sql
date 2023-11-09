-- see https://docs.snowflake.com/en/sql-reference/functions/system_global_account_set_parameter

SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER('myorg.account1',
  'ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');

SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER('myorg.account2',
  'ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');