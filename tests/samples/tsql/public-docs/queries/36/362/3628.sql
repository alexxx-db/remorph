-- see https://learn.microsoft.com/en-us/sql/t-sql/statements/create-login-transact-sql?view=sql-server-ver16

Use master
CREATE LOGIN [bob@contoso.com] FROM EXTERNAL PROVIDER
GO