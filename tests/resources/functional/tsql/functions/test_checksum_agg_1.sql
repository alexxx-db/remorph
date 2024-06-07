-- ## CHECKSUM_AGG
--
-- There is no direct equivalent of CHECKSUM_AGG in Databricks SQL. The following
-- conversion is a suggestion and may not be perfectly functional.

-- tsql sql:
SELECT CHECKSUM_AGG(col1) FROM t1;

-- databricks sql:
<<<<<<< HEAD
SELECT MD5(CONCAT_WS(',', ARRAY_AGG(col1))) FROM t1;
=======
SELECT MD5(CONCAT_WS(',', COLLECT_LIST(col1))) FROM t1;
>>>>>>> 5126eaff (Improve test coverage for TSQL remorph (#439))
