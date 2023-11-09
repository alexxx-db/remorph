-- see https://learn.microsoft.com/en-us/sql/t-sql/functions/object-schema-name-transact-sql?view=sql-server-ver16

SELECT DISTINCT OBJECT_SCHEMA_NAME(object_id)  
FROM master.sys.objects;