-- see https://learn.microsoft.com/en-us/sql/t-sql/functions/base64-encode-transact-sql?view=azuresqldb-current

SELECT BASE64_ENCODE(0xCAFECAFE, 1);