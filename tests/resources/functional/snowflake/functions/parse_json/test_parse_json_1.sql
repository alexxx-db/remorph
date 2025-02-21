-- snowflake sql:
SELECT tt.id, PARSE_JSON(tt.details) FROM prod.public.table tt;

-- databricks sql:
<<<<<<< HEAD
SELECT tt.id, PARSE_JSON(tt.details) FROM prod.public.table AS tt;
=======
<<<<<<< HEAD
<<<<<<< HEAD
SELECT tt.id, PARSE_JSON(tt.details) FROM prod.public.table AS tt;
=======
SELECT tt.id, PARSE_JSON(tt.details) FROM prod.public.table AS tt;
>>>>>>> 30dc687c (Added support for `PARSE_JSON` and `VARIANT` datatype (#906))
=======
SELECT tt.id, PARSE_JSON(tt.details) FROM prod.public.table AS tt;
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
>>>>>>> databrickslabs-main
