/* -- width 20 --height 6 --order -1 --title 'Dedicated Session Requests' --type table */

WITH workspace_workspace_info as (select * from synapse-profiler-runs.run_name.workspace_workspace_info),
workspace_name_region as (
    select distinct name, location from workspace_workspace_info limit 1
    ),
dedicated_session_requests as (
    select *
    from synapse-profiler-runs.run_name.dedicated_session_requests
    qualify row_number() over (PARTITION BY pool_name, session_id, request_id order by end_time desc) = 1
)

select *
from
workspace_name_region,
(
    select
        pool_name,
        min(start_time) as start_ts,
        max(end_time) as end_ts,
        count(distinct to_date(start_time)) as days,
        count(distinct session_id) as sessions,
        count(*) as requests
    from dedicated_session_requests
    group by 1
) X
