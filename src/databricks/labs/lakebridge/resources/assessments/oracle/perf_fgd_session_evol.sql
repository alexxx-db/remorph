@./spoolhead.sql
-- spool results/perf_fgd_session_evol.csv

select con.name,
 sh.instance_number,u.username,
 to_char(sh.sample_time,'YYYY-MM-DD HH24:MI') as snap_time,
 count(distinct sh.session_id||','||sh.session_serial#) as foregd_session_cnt
from cdb_hist_active_sess_history sh,
 (
  select con_id,
 dbid,
 name
  from v$containers
  where name != 'PDB$SEED'
 ) con,
 (select distinct user_id,username
  from cdb_users
  where username not in ('SYS','SYSTEM','XS$NULL','OJVMSYS','LBACSYS','OUTLN','SYS$UMF','DBSNMP','APPQOSSYS','DBSFWUSER','GGSYS','ANONYMOUS','CTXSYS','DVF','DVSYS','GSMADMIN_INTERNAL','MDSYS','OLAPSYS','XDB','WMSYS',
'GSMCATUSER','MDDATA','REMOTE_SCHEDULER_AGENT','SYSBACKUP','GSMUSER','GSMROOTUSER','SYSRAC','SI_INFORMTN_SCHEMA','AUDSYS','DIP','ORDPLUGINS','ORDDATA','SYSKM','ORACLE_OCM','ORDSYS','SYSDG','
SYS','SYSTEM','XS$NULL','LBACSYS','OUTLN','DBSNMP','APPQOSSYS','DBSFWUSER','GGSYS','ANONYMOUS','CTXSYS','DVF','DVSYS','GSMADMIN_INTERNAL','MDSYS','OLAPSYS','XDB','WMSYS','GSMCATUSER','MDDATA
','REMOTE_SCHEDULER_AGENT','SYSBACKUP','GSMUSER','SYSRAC','OJVMSYS','SI_INFORMTN_SCHEMA','AUDSYS','DIP','ORDPLUGINS','ORDDATA','SYSKM','ORACLE_OCM','SYS$UMF','ORDSYS','SYSDG')
 ) u
where 1 = 1
 and sh.con_id = con.con_id
 and sh.dbid=con.dbid
 and u.user_id=sh.user_id
 and sh.session_type = 'FOREGROUND'
group by con.name,
 sh.instance_number,
 to_char(sh.sample_time,'YYYY-MM-DD HH24:MI'),
 u.username
order by 1, 4, 2
/

spool off
exit

