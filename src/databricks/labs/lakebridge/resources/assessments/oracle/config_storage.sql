@./spoolhead.sql
--  results/config_storage.csv
col tablespace_type for a20

select con_name,
--       sub.tablespace_name,
       case
         when tablespace_name in ('SYSTEM','SYSAUX') then 'SYSTEM'
         when tablespace_name in (select tablespace_name from cdb_tablespaces where contents='UNDO' and con_id=sub.con_id) then 'UNDO'
         -- when tablespace_name in (select tablespace_name from cdb_tablespaces where contents='TEMPORARY' and con_id=sub.con_id) then 'TEMP'
         ELSE 'USER_DATA'
       end as tablespace_type,
       sum(gb) gb,
       sum(freegb) freegb,
       sum(maxgb) maxgb
from
(
    select c.name as con_name,c.con_id,
           f.tablespace_name,
           f.bytes/1024/1024 mb, f.bytes/1024/1024/1024 gb,
           t.free_bytes/1024/1024 freemb, t.free_bytes/1024/1024/1024 freegb,
           f.maxbytes/1024/1024 maxmb, f.maxbytes/1024/1024/1024 maxgb
    from
    (select con_id,tablespace_name,bytes,maxbytes from cdb_data_files ) f,
    (select con_id,tablespace_name,sum(bytes) free_bytes from cdb_free_space group by con_id,tablespace_name ) t,
    (select distinct con_id,name from gv$containers) c
    where 1=1
          and t.con_id=f.con_id
          and t.con_id=c.con_id
          and t.tablespace_name=f.tablespace_name
) sub
group by con_name,
--tablespace_name
       case
         when tablespace_name in ('SYSTEM','SYSAUX') then 'SYSTEM'
         when tablespace_name in (select tablespace_name from cdb_tablespaces where contents='UNDO' and con_id=sub.con_id) then 'UNDO'
         -- when tablespace_name in (select tablespace_name from cdb_tablespaces where contents='TEMPORARY' and con_id=sub.con_id) then 'TEMP'
         ELSE 'USER_DATA'
       end
order by 1
/
spool off
exit

