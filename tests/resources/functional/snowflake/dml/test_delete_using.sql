-- snowflake sql:

DELETE FROM t1 USING t2 WHERE t1.c1 = t2.c2;

-- databricks sql:
<<<<<<< HEAD
<<<<<<< HEAD
MERGE INTO  t1 USING t2 ON t1.c1 = t2.c2 WHEN MATCHED THEN DELETE;
=======
MERGE INTO  t1 USING t2 ON t1.c1 = t2.c2 WHEN MATCHED THEN DELETE;
>>>>>>> ce7e4835 ([sql] generate `DELETE FROM ...` (#824))
=======
MERGE INTO  t1 USING t2 ON t1.c1 = t2.c2 WHEN MATCHED THEN DELETE;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
