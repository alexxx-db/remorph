-- see https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver16

CREATE TABLE dbo.T1
(
    c1 INT,
    c2 XML
)
WITH (XML_COMPRESSION = ON);