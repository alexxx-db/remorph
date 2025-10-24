@./spoolhead.sql
-- spool results/perf_hm.csv

SELECT TO_CHAR(mtime,'YYYY/MM/DD') mtime,
	      pdb_name,
	      instance_number,
	      TO_CHAR(mtime,'HH24') d,
	      core_nb,
	      LOAD AS value
	FROM
	      (SELECT to_date(mtime,'YYYY-MM-DD HH24') mtime,
		cont.name as pdb_name,
		instance_number ,
		core_nb,
		ROUND(SUM(c1),2) AAS_WAIT,
		ROUND(SUM(c2),2) AAS_CPU,
		ROUND(SUM(cnt),2) AAS,
		ROUND(SUM(load),2) LOAD
	      FROM
		(SELECT ash.instance_number,
			ash.con_id,
		  TO_CHAR(sample_time,'YYYY-MM-DD HH24') mtime,
		  cpu.core_nb,
		  DECODE(session_state,'WAITING',COUNT(*),0)/360 c1,
		  DECODE(session_state,'ON CPU',COUNT( *),0) /360 c2,
		  COUNT(			       *)/360 cnt,
		  COUNT(			       *)/360/cpu.core_nb load
		FROM cdb_hist_active_sess_history ash,
		  (select inst_id,stat_name,to_number(value) as core_nb from gv$osstat where stat_name='NUM_CPUS' ) cpu
		WHERE
		  1=1
		  and ash.instance_number=cpu.inst_id
		  -- and sample_time > sysdate - 30
		GROUP BY ash.instance_number,ash.con_id,cpu.core_nb,
		  TO_CHAR(sample_time,'YYYY-MM-DD HH24'),
		  session_state,
		  cpu.core_nb
		) q,
		(select distinct con_id,name from gv$containers) cont
	      WHERE cont.con_id=q.con_id
	      GROUP BY cont.name, mtime,instance_number,core_nb
	      order by 1,2,3
	      )
/

-- col "00-01_ " for 90.99
-- col "01-02_ " for 90.99
-- col "02-03_ " for 90.99
-- col "03-04_ " for 90.99
-- col "04-05_ " for 90.99
-- col "05-06_ " for 90.99
-- col "06-07_ " for 90.99
-- col "07-08_ " for 90.99
-- col "08-09_ " for 90.99
-- col "09-10_ " for 90.99
-- col "10-11_ " for 90.99
-- col "11-12_ " for 90.99
-- col "12-13_ " for 90.99
-- col "13-14_ " for 90.99
-- col "14-15_ " for 90.99
-- col "15-16_ " for 90.99
-- col "16-17_ " for 90.99
-- col "17-18_ " for 90.99
-- col "18-19_ " for 90.99
-- col "19-20_ " for 90.99
-- col "20-21_ " for 90.99
-- col "21-22_ " for 90.99
-- col "22-23_ " for 90.99
-- col "23-24_ " for 90.99

-- WITH t AS
--   (SELECT TO_CHAR(mtime,'YYYY/MM/DD') mtime,
--     TO_CHAR(mtime,'HH24') d,
--     LOAD AS value
--   FROM
--     (SELECT to_date(mtime,'YYYY-MM-DD HH24') mtime,
--       ROUND(SUM(c1),2) AAS_WAIT,
--       ROUND(SUM(c2),2) AAS_CPU,
--       ROUND(SUM(cnt),2) AAS,
--       ROUND(SUM(load),2) LOAD
--     FROM
--       (SELECT TO_CHAR(sample_time,'YYYY-MM-DD HH24') mtime,
--         DECODE(session_state,'WAITING',COUNT(*),0)/360 c1,
--         DECODE(session_state,'ON CPU',COUNT( *),0) /360 c2,
--         COUNT(                               *)/360 cnt,
--         COUNT(                               *)/360/cpu.core_nb load
--       FROM cdb_hist_active_sess_history,
--         (select stat_name,sum(to_number(value)) as core_nb from gv$osstat where stat_name='NUM_CPU_CORES' group by stat_name) cpu
--       WHERE
--         sample_time > sysdate - 30 and
--         con_id=(select distinct con_id from gv$containers where name='&1')
--       GROUP BY TO_CHAR(sample_time,'YYYY-MM-DD HH24'),
--         session_state,
--         cpu.core_nb
--       )
--     GROUP BY mtime
--     )
--   )
-- SELECT mtime,
--   NVL("00-01_ ",0) "00-01_ ",
--   NVL("01-02_ ",0) "01-02_ ",
--   NVL("02-03_ ",0) "02-03_ ",
--   NVL("03-04_ ",0) "03-04_ ",
--   NVL("04-05_ ",0) "04-05_ ",
--   NVL("05-06_ ",0) "05-06_ ",
--   NVL("06-07_ ",0) "06-07_ ",
--   NVL("07-08_ ",0) "07-08_ ",
--   NVL("08-09_ ",0) "08-09_ ",
--   NVL("09-10_ ",0) "09-10_ ",
--   NVL("10-11_ ",0) "10-11_ ",
--   NVL("11-12_ ",0) "11-12_ ",
--   NVL("12-13_ ",0) "12-13_ ",
--   NVL("13-14_ ",0) "13-14_ ",
--   NVL("14-15_ ",0) "14-15_ ",
--   NVL("15-16_ ",0) "15-16_ ",
--   NVL("16-17_ ",0) "16-17_ ",
--   NVL("17-18_ ",0) "17-18_ ",
--   NVL("18-19_ ",0) "18-19_ ",
--   NVL("19-20_ ",0) "19-20_ ",
--   NVL("20-21_ ",0) "20-21_ ",
--   NVL("21-22_ ",0) "21-22_ ",
--   NVL("22-23_ ",0) "22-23_ ",
--   NVL("23-24_ ",0) "23-24_ "
-- FROM t pivot( SUM(value) AS " " FOR d IN ('00' AS "00-01",'01' AS "01-02",'02' AS "02-03",'03' AS "03-04",'04' AS "04-05",'05' AS "05-06",'06' AS "06-07",'07' AS "07-08",
--                                           '08' AS "08-09",'09' AS "09-10",'10' AS "10-11", '11' AS "11-12",'12' AS "12-13",'13' AS "13-14",'14' AS "14-15",'15' AS "15-16",
--                                           '16' AS "16-17",'17' AS "17-18",'18' AS "18-19",'19' AS "19-20",'20' AS "20-21",'21' AS "21-22", '22' AS "22-23",'23' AS "23-24")
--             )
-- ORDER BY mtime
-- /
spool off
exit

