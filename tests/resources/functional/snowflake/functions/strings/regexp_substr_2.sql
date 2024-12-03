-- snowflake sql:
select id, string1,
       regexp_substr(string1, 'the\\W+\\w+') as "SUBSTRING"
    from demo2;

-- databricks sql:

select id, string1,
<<<<<<< HEAD
<<<<<<< HEAD
	   REGEXP_EXTRACT(string1, 'the\\W+\\w+', 0) as `SUBSTRING`
    from demo2;
<<<<<<< HEAD
=======
	   REGEXP_EXTRACT(string1, 'the\\W+\\w+') as `SUBSTRING`
    from demo2;
>>>>>>> 4486e58a (Avoid processing escapes in interpolator arguments (#1167))
=======
	   REGEXP_EXTRACT(string1, 'the\\W+\\w+', 0) as `SUBSTRING`
    from demo2;
>>>>>>> d4ad0cd2 (Bump sqlglot from 25.8.1 to 25.35.0 (#1205))
=======
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
