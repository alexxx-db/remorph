-- see https://docs.snowflake.com/en/sql-reference/account-usage/task_history

SELECT query_text, completed_time
FROM snowflake.account_usage.task_history
LIMIT 10;