-- Has to be executed on CDB
@./spoolhead.sql
-- spool results/config_instance.csv
select inst_id,instance_name,version,database_type from gv$instance
/
spool off
exit

