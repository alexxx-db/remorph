WHENEVER SQLERROR EXIT SQL.SQLCODE
WHENEVER OSERROR EXIT
set embedded on
set pagesize 0
set colsep ';'
set underline off
set echo off
set feedback off
set linesize 5000
set long 99999
set trimspool on
set headsep off
set verify off
alter session set NLS_NUMERIC_CHARACTERS='.,';

