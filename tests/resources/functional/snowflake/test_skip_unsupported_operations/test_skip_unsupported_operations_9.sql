
-- snowflake sql:

                CREATE TASK t1
                  SCHEDULE = '30 MINUTE'
                  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
                  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
                AS
                INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);
                ;

-- databricks sql:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
-- CREATE TASK t1 SCHEDULE = '30 MINUTE' TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' AS INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);
=======
-- CREATE TASK t1 SCHEDULE = '60 MINUTE' TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' AS INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP)
>>>>>>> 96c6764d (Added Translation Support for `!` as `commands` and `&` for `Parameters` (#771))
=======
-- CREATE TASK t1 SCHEDULE = '60 MINUTE' TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' AS INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP)
>>>>>>> 1ab2645f (extra ";" generation has been taken care for Bang command (#858))
=======
-- CREATE TASK t1 SCHEDULE = '30 MINUTE' TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' AS INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);
>>>>>>> c6f780af (unresolved commands `alter session | stream...` `create stream` `create task` (#864))
=======
-- CREATE TASK t1 SCHEDULE = '30 MINUTE' TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' AS INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
