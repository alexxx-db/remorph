set serveroutput on
@./spoolhead.sql
--set timing off
--set heading off
--set termout off
--set verify off

-- spool scripts/main/perf_addon/script.sql

declare
  cursor c is select inst_id from gv$instance order by 1;
begin
  for i in c
  loop
    dbms_output.put_line('@./scripts/main/perf_addon/perf_hm_raw.sql '||i.inst_id);
    dbms_output.put_line(chr(13)||chr(10));
  end loop;
  dbms_output.put_line('exit');
end;
/

spool off

start ./scripts/main/perf_addon/script.sql
exit

