/* -- width 20 --height 6 --order 1 --title 'Profiler Run Info' --type table */
select schema_name
from synapse-profiler-runs.information_schema.schemata
where schema_name not in (
  'default',
  'information_schema',
  'utils'
)
order by 1;
