-- =================================================================================
-- Select Paying User event data
-- =================================================================================

select userid,
eventsubtype,
timestamp,
concat(day,'-',month,'-',year) as dt
from ubd_events
where  eventAction = 'NavigatedTo'
and eventtype != 'ViewEvent'
and LENGTH(eventsubtype) != 0
-- and ( actor.roles = array['Learner'] or actor.roles = array['Instructor'])
AND actor.roles != array[]
and (actor.districtpid not like '91%' OR
     actor.districtpid not like '92%' OR
     actor.districtpid not like '79%' OR
     actor.districtpid not like '88%' )
and year = '2019'
and month = '05';


-- =================================================================================
-- Select user data for all paying users (Teachers & students) on Ed removing 
-- double click events  
-- =================================================================================

SELECT userid,
     eventsubtype,
     time,
     diff,
     dt
FROM
(
SELECT *,
    CASE
    WHEN LENGTH(CAST(time2 AS varchar)) > 0 THEN
    date_diff('millisecond', time2, time)
    ELSE 0
    END AS diff
FROM
(
  SELECT userid,
     eventtype,
     eventsubtype,
     Lead(eventsubtype,1) OVER(partition BY userid ORDER BY  eventtime) AS evnt_dup,
     from_iso8601_timestamp(eventtime) AS time, Lead(from_iso8601_timestamp(eventtime), 1) OVER(partition BY userid ORDER BY  eventtime) AS time2,
     timestamp,
     concat(day,'-',month,'-',year) AS dt
     FROM ubd_events
     WHERE eventAction = 'NavigatedTo'
        AND eventtype != 'ViewEvent'
        AND LENGTH(eventsubtype) != 0
        AND ( actor.roles = array['Learner'] OR actor.roles = array['Instructor'] )
        AND actor.districtpid NOT LIKE '91%'
        AND actor.districtpid NOT LIKE '92%'
        AND actor.districtpid NOT LIKE '79%'
        AND actor.districtpid NOT LIKE '88%'
        AND year = '2019'
        AND month = '05'
        AND day = '20'
ORDER BY  eventtime ASC

  )
WHERE eventsubtype<>evnt_dup

  )
WHERE diff NOT BETWEEN 0 AND 1000
GROUP BY  userid, dt, eventsubtype, time, diff
ORDER BY  userid, dt, time


-- =================================================================================
-- Select all trimmed out user events data for all paying users 
-- (Teachers & students) on Ed removing duplicate events and double click events.
-- Difference between clicks kept to be lesser than 600ms
-- =================================================================================

WITH
raw_data AS
 ( SELECT userid,
         eventtype,
         eventsubtype,
         Lead(eventsubtype,1) OVER(partition BY userid ORDER BY  eventtime) AS evnt_dup,
         from_iso8601_timestamp(eventtime) AS time, Lead(from_iso8601_timestamp(eventtime), 1) 
         								OVER(partition BY userid ORDER BY  eventtime) AS time2,
         timestamp,
         concat(day,'-',month,'-',year) AS dt
         FROM ubd_events
         WHERE userid = '0025b850-c53b-4e92-873f-c0693882a364' and
             eventAction = 'NavigatedTo'
            AND eventtype != 'ViewEvent'
            AND LENGTH(eventsubtype) != 0
            AND ( actor.roles = array['Learner'] OR actor.roles = array['Instructor'] )
            AND actor.districtpid NOT LIKE '91%'
            AND actor.districtpid NOT LIKE '92%'
            AND actor.districtpid NOT LIKE '79%'
            AND actor.districtpid NOT LIKE '88%'
            AND target.extensions['autoGenerated'] != 'true'
            AND year = '2019'
            AND month = '05'
            AND day = '20'
    ORDER BY  eventtime ASC),

big_data AS
  (SELECT *, LENGTH(evnt_dup) as dup_evnt_len,
        CASE
        WHEN LENGTH(CAST(time2 AS varchar)) > 0 THEN
        date_diff('millisecond', time2, time)
        ELSE 0
        END AS diff
    FROM raw_data),

data_common_events AS
  (SELECT distinct userid,
         eventsubtype,
         evnt_dup,
         time,
         time2,
         diff,
         timestamp,
         dt
    FROM big_data
  WHERE eventsubtype<>evnt_dup
  OR (eventsubtype = evnt_dup AND diff NOT BETWEEN 1 AND 600)
  OR dup_evnt_len = 0
  OR evnt_dup is null
  )
select userid,
       regexp_extract(eventsubtype, '^.*?[\.].*?(?=\.|$)') as eventsubtype,
       time,
       timestamp,
       dt
from data_common_events
GROUP BY  userid, dt, eventsubtype,time, timestamp
ORDER BY  userid, dt, time


-- =================================================================================
-- Select user event transition list data for all paying users (Teachers & students) 
-- on Ed for a particular month sessionized per day
-- =================================================================================

with   
-- Fetch the userid, event times and the navigation user events with the date  
-- for students and teachers (paying customers only) for a given period  
raw_data as (  
  select userid,  
  eventtime,  
  eventsubtype,  
  COALESCE(TRY(actor.roles[1]), null) AS roles,  
  timestamp,  
  concat(day, '-',month,'-',year) AS dt  
  FROM ubd_events  
    WHERE eventAction = 'NavigatedTo'  
            AND eventtype != 'ViewEvent'  
            AND LENGTH(eventsubtype) != 0  
            -- AND ( actor.roles = array['Learner'] OR actor.roles = array['Instructor'] )
            AND actor.roles != array[]  
    		AND actor.districtpid NOT LIKE '91%'  
            AND actor.districtpid NOT LIKE '92%'  
            AND actor.districtpid NOT LIKE '79%'  
            AND actor.districtpid NOT LIKE '88%'  
            AND target.extensions['autoGenerated'] = 'false'  
            AND year = '2019'  
            AND month = '04'
            -- AND day = '20'
            -- AND hour = '14'  
),  
-- Fetch the next event sub type and the next event time based on the userid and date.  
-- The next event and time should only be selected per user per day  
-- as the session is set to be the whole day  
base_data as(  
SELECT *,  
Lead(eventsubtype, 1) OVER(partition BY userid, dt ORDER BY  eventtime) AS next_event,   
Lead(CAST(to_unixtime(from_iso8601_timestamp(eventtime)) AS BIGINT), 1) OVER(partition BY userid, dt ORDER BY  eventtime) AS next_time  
from raw_data  
),  
  
big_data as(  
-- calculate the time difference between the next event and the current event, to find the time interval between events  
select *,  
         CASE  
         WHEN LENGTH(next_event) > 0   
         THEN date_diff('millisecond',from_iso8601_timestamp(eventtime), Lead(from_iso8601_timestamp(eventtime), 1) OVER(partition BY userid, dt ORDER BY  eventtime))  
         ELSE 0  
         END AS time_diff,  
         COALESCE(TRY(LENGTH(next_event)), 0) AS next_event_len  
         from base_data  
),  
-- Calculate how many hierarchies of an eventsubtype is to be kept  
final_data AS   
(  
    SELECT userid,  
        roles,  
-- if the eventsubtype starts with any of the below given events, then limit the event to max 3 hierarchies  
-- eg: an event 'datareporting.aaa.bbb.ccc.ddd.fff' will be trimmed to 'datareporting.aaa.bbb'  
-- if the event does not belong to the listed ones trim it to 2 hierarchies  
-- so an event 'aaa.bbb.ccc.ddd' will be trimmed to 'aaa.bbb'  
        CASE  
        WHEN regexp_extract(eventsubtype,'^.*?(?=[\.]|$)') IN ('datareporting', 'CreateAssignment', 'StudentAssignmentList', 'AssignmentList', 'ManualScoring')   
        THEN  
        regexp_extract(eventsubtype,'^.*?([\.]|$).*?([\.]|$).*?(?=\.|$)')  
        ELSE   
        regexp_extract(eventsubtype, '^.*?([\.]|$).*?(?=\.|$)')  
        END AS event,  
        CASE  
        WHEN regexp_extract(next_event,'^.*?(?=[\.]|$)') IN ('datareporting', 'CreateAssignment', 'StudentAssignmentList', 'AssignmentList', 'ManualScoring')   
        THEN  
        regexp_extract(next_event,'^.*?([\.]|$).*?([\.]|$).*?(?=\.|$)')  
        ELSE regexp_extract(next_event, '^.*?([\.]|$).*?(?=\.|$)')  
        END AS event_next, time_diff, dt  
    FROM big_data  
-- select only those events which are not similar to the next event as it would be a double click registered  
-- Or if they are similar check the time difference ti be greater than 900ms  
-- Only keep the values for which th time difference is in between 500ms and 8280000ms (~23 hrs)  
-- Remove all the records which have no next events  
    WHERE ( eventsubtype<>next_event OR (eventsubtype = next_event AND time_diff NOT BETWEEN 1 AND 900))  
    AND time_diff > 500  
    AND time_diff < 8280000  
    AND next_event is NOT NULL  
    AND next_event_len > 0  
)
select userid,
       roles,
       array_join(array_agg(event), ' -> ') as arr,
       dt
       from final_data
       group by 1,2, 4


-- =================================================================================
-- Select user event link sequences, number of user event transitions and user 
-- event transition timings for all paying users on Ed removing 
-- double click events and auto generated events  
-- =================================================================================

with   
-- Fetch the userid, event times and the navigation user events with the date  
-- for students and teachers (paying customers only) for a given period  
raw_data as (  
  select userid,  
  eventtime,  
  eventsubtype,  
  COALESCE(TRY(actor.roles[1]), null) AS roles,  
  timestamp,  
  concat(day, '-',month,'-',year) AS dt  
  FROM ubd_events  
    WHERE eventAction = 'NavigatedTo'  
            AND eventtype != 'ViewEvent'  
            AND LENGTH(eventsubtype) != 0  
            -- AND ( actor.roles = array['Learner'] OR actor.roles = array['Instructor'] )   
            AND actor.roles != array[]
    		AND actor.districtpid NOT LIKE '91%'  
            AND actor.districtpid NOT LIKE '92%'  
            AND actor.districtpid NOT LIKE '79%'  
            AND actor.districtpid NOT LIKE '88%'  
            AND target.extensions['autoGenerated'] = 'false'  
            AND year = '2019'  
            AND month = '" + str(last_month) + "'  
),  
-- Fetch the next event sub type and the next event time based on the userid and date.  
-- The next event and time should only be selected per user per day  
-- as the session is set to be the whole day  
base_data as(  
SELECT *,  
Lead(eventsubtype, 1) OVER(partition BY userid, dt ORDER BY  eventtime) AS next_event,   
Lead(CAST(to_unixtime(from_iso8601_timestamp(eventtime)) AS BIGINT), 1) OVER(partition BY userid, dt ORDER BY  eventtime) AS next_time  
from raw_data  
),  
  
big_data as(  
-- calculate the time difference between the next event and the current event, to find the time interval between events  
select *,  
         CASE  
         WHEN LENGTH(next_event) > 0   
         THEN date_diff('millisecond',from_iso8601_timestamp(eventtime), Lead(from_iso8601_timestamp(eventtime), 1) OVER(partition BY userid, dt ORDER BY  eventtime))  
         ELSE 0  
         END AS time_diff,  
         COALESCE(TRY(LENGTH(next_event)), 0) AS next_event_len  
         from base_data  
),  
-- Calculate how many hierarchies of an eventsubtype is to be kept  
final_data AS   
(  
    SELECT userid,  
        roles,  
-- if the eventsubtype starts with any of the below given events, then limit the event to max 3 hierarchies  
-- eg: an event 'datareporting.aaa.bbb.ccc.ddd.fff' will be trimmed to 'datareporting.aaa.bbb'  
-- if the event does not belong to the listed ones trim it to 2 hierarchies  
-- so an event 'aaa.bbb.ccc.ddd' will be trimmed to 'aaa.bbb'  
        CASE  
        WHEN regexp_extract(eventsubtype,'^.*?(?=[\.]|$)') IN ('datareporting', 'CreateAssignment', 'StudentAssignmentList', 'AssignmentList', 'ManualScoring')   
        THEN  
        regexp_extract(eventsubtype,'^.*?([\.]|$).*?([\.]|$).*?(?=\.|$)')  
        ELSE   
        regexp_extract(eventsubtype, '^.*?([\.]|$).*?(?=\.|$)')  
        END AS event,  
        CASE  
        WHEN regexp_extract(next_event,'^.*?(?=[\.]|$)') IN ('datareporting', 'CreateAssignment', 'StudentAssignmentList', 'AssignmentList', 'ManualScoring')   
        THEN  
        regexp_extract(next_event,'^.*?([\.]|$).*?([\.]|$).*?(?=\.|$)')  
        ELSE regexp_extract(next_event, '^.*?([\.]|$).*?(?=\.|$)')  
        END AS event_next, time_diff, dt  
    FROM big_data  
-- select only those events which are not similar to the next event as it would be a double click registered  
-- Or if they are similar check the time difference ti be greater than 900ms  
-- Only keep the values for which th time difference is in between 500ms and 8280000ms (~23 hrs)  
-- Remove all the records which have no next events  
    WHERE ( eventsubtype<>next_event OR (eventsubtype = next_event AND time_diff NOT BETWEEN 1 AND 900))  
    AND time_diff > 500  
    AND time_diff < 8280000  
    AND next_event is NOT NULL  
    AND next_event_len > 0  
),  
-- Fetch joined up event sequences with the time differences  
aggr_data AS  
(  
SELECT userid, roles, event,event_next,  
concat(event, '->',event_next) AS event_seq,  
       time_diff  
    from final_data  
)  
-- Finally select all the unique event links (event1->event2 link) with a list of time intervals for all the users and order the result based on the event sequence  
select event_seq,  
       cardinality(array_agg(time_diff)) as totsum,  
       array_join(array_agg(time_diff), ',') as arr  
FROM aggr_data  
GROUP BY event_seq  
order by event_seq  



-- =================================================================================
-- Select user details (state, organization, sections) for all entitled Teachers 
-- on Ed 
-- =================================================================================

WITH
raw_data AS (
select 
all_org.statecode      AS  "state_code",
org.ultimateorgrefid   AS  "parent_org",
org.ultimatename       AS  "parent_org_name",
org.ultimatepid        AS  "parent_pid",
org.ultimateorgtype    AS  "parent_type",
org.orgrefid           AS  "org",
org.name               AS  "child_name",
org.pid                AS  "child_pid",  
org.orgtype            AS  "org_type",
sec_user.userrefid     AS  "userrefid",
sec_user.usertype      AS  "user_type",
sec.sectionrefid       AS  "section_refid",
sec.name               AS  "section_name"
-- select count(*)
from v_organizations_enriched  org
join v_organizations           all_org    ON org.orgrefid          = all_org.orgrefid
join v_organizations_sections  org_sec    ON org.orgrefid          = org_sec.orgrefid
-- join v_organizations_users     org_users  ON org.orgrefid       = org_users.orgrefid
join v_sections_users          sec_user   ON org_sec.sectionrefid  = sec_user.sectionrefid
join v_sections                sec        ON sec_user.sectionrefid = sec.sectionrefid
JOIN (
   SELECT DISTINCT
            COALESCE(child_org.orgrefid,orgs.orgrefid) AS orgrefid
       FROM v_organizations_enriched orgs
       JOIN view_entitlements_consolidated ent ON (orgs.orgrefid = ent.org_sif_uuid)
  LEFT JOIN v_organizations_enriched child_org ON (child_org.ultimatepid = CASE WHEN orgs.ultimatepid = orgs.pid
                                                                                THEN orgs.ultimatepid
                                                                                ELSE NULL
                                                   END)
      WHERE -- orgs.ultimateorgrefid ='0caf1d47-94c8-4823-aad3-375053ad6ed9' AND 
            orgs.category = 'Paying' AND TRIM(orgs.pid) <> '' -- Get entitlements for live orgs
            AND ent.ent_is_active = '1.0000000000' -- Bring in active entitlements only
            AND ent.soli_is_fulfilled = '1' -- Bring in active entitlements only
            AND ent.soli_egood_flag_id = '18' -- SAP orders for ED
  )
   dist_orgs ON org.orgrefid = dist_orgs.orgrefid

where -- org.ultimateorgrefid ='0caf1d47-94c8-4823-aad3-375053ad6ed9' AND 
      org.category = 'Paying' AND
      trim(org.pid) <> '' AND
      sec_user.usertype = 'leadTeachers'
order by 1,2,6,10,12
  )
Select * from raw_data;

-- =================================================================================
-- Select group data (group duration, users in group, user duration in group)  
-- =================================================================================

WITH
base_query as (

select 
org_org.parentorgrefid as parent_org, 
org_org.parentorgtype as parent_org_type, 
org_org.parentpid as parent_pid, 
org_org.orgrefid as sub_org,
from_iso8601_timestamp(org_org."start") as org_start, 
-- org_org."end" as org_end, 
CASE
  WHEN LENGTH(org_org."end") = 0
  THEN from_iso8601_timestamp("date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d'))
  ELSE from_iso8601_timestamp(org_org."end")
  END AS org_end,

org_sec.sectionrefid as section,
from_iso8601_timestamp(org_sec.organizationsectionstart) as section_start,
-- org_sec.organizationsectionend section_end, 
CASE
  WHEN LENGTH(org_sec.organizationsectionend) = 0
  THEN from_iso8601_timestamp("date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d'))
  ELSE from_iso8601_timestamp(org_sec.organizationsectionend)
  END AS section_end,
  
sec_grp.grouprefid as parent_group,
from_iso8601_timestamp(sec_grp."start") as parent_group_start, 
-- sec_grp."end" as parent_group_end, 
CASE
  WHEN LENGTH(sec_grp."end") = 0
  THEN from_iso8601_timestamp("date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d'))
  ELSE from_iso8601_timestamp(sec_grp."end")
  END AS parent_group_end,

grp_subgrp.grouprefid as sub_group,
from_iso8601_timestamp(grp_subgrp."start") as subgroup_start, 
-- grp_subgrp."end" as subgroup_end, 
CASE
  WHEN LENGTH(grp_subgrp."end") = 0
  THEN from_iso8601_timestamp("date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d'))
  ELSE from_iso8601_timestamp(grp_subgrp."end")
  END AS subgroup_end,

grp_usr.userrefid as userid,
from_iso8601_timestamp(grp_usr."start") as group_user_start, 
-- grp_usr."end" as group_user_end
CASE
  WHEN LENGTH(grp_usr."end") = 0
  THEN from_iso8601_timestamp("date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d'))
  ELSE from_iso8601_timestamp(grp_usr."end")
  END AS group_user_end

from ids_organizations_parentorganizations_history org_org        --  organization parent organization
   join ids_organization_section_direct_history org_sec     --  organization section 
      on org_org.orgrefid = org_sec.orgrefid    
        
     join ids_sections_groups_history sec_grp                 --  section groups
      on org_sec.sectionrefid  = sec_grp.sectionrefid
        
     join ids_groups_subgroups_history grp_subgrp             --  group subgroup
        on sec_grp.grouprefid = grp_subgrp.parentgrouprefid 
        
     join ids_groups_users_history grp_usr                    --  group users
        on grp_subgrp.grouprefid = grp_usr.grouprefid
        
where org_org.parentorgrefid = '1fbe82dd-f2df-4e52-8e92-913cc916bc45' and
  org_org.parentpid not like '91*' 
  and org_org.parentpid not like '92*' 
  and org_org.parentpid not like '882*' 
  and org_org.parentpid not like '79*'
and org_org.dt     = "date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d')
and org_sec.dt     = "date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d')
and sec_grp.dt     = "date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d')
and grp_subgrp.dt  = "date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d')
and grp_usr.dt     = "date_format"((current_date - INTERVAL  '2' DAY), '%Y-%m-%d')
order by 1,4,7,10,13
),
time_diff_data as (
  select *,
         date_diff('second', subgroup_start, subgroup_end) as group_duration,
         date_diff('day', group_user_start, group_user_end) as user_grp_duration
         from base_query
)
select  parent_org, 
      	sub_org,
     	section,
        parent_group,
        sub_group,
        date_diff('day', subgroup_start, subgroup_end) as group_duration,
        count(userid) as users_in_group,
        array_agg(group_duration) as grp_duration,
        array_agg(userid) as users_in_group,
        array_agg(user_grp_duration) AS user_grp_duration
        from time_diff_data
        GROUP BY 1,2,3,4,5,6


-- =================================================================================
-- Select student group details to check whether group is recomended or 
-- manually created
-- =================================================================================

WITH 
group_data as (
select *,
  length(source) as src_len
  from staging_int.ids_groups 
  where source !='undefined' and dt = "date_format"((current_date - INTERVAL  '4' DAY), '%Y-%m-%d')
  ),
group_type as (
select 
group_data.grouprefid AS "parent_group",
group_data.name AS "parent_name", 
group_subgroup.grouprefid AS "subgroup",
group_data.source AS "parent_source", 
group_data.src_len AS "source_len", 
group_data.createdtime AS "parent_group_created_time"
from group_data
LEFT JOIN staging_int.ids_groups_subgroups_history group_subgroup
on group_data.grouprefid = group_subgroup.parentgrouprefid
where 
  group_data.src_len = 36 
  or group_data.src_len = 0
  and group_subgroup.dt = "date_format"((current_date - INTERVAL  '4' DAY), '%Y-%m-%d')
),
final_data as (
select
distinct parent_group,
parent_name,
subgroup,
parent_source,
CASE
  WHEN source_len = 36
  THEN 'Recommended Group'
  WHEN source_len = 0
  THEN 'Manual Group'
  END AS "group_type"
from group_type
)
select * from final_data



-- =================================================================================
-- User level Enriched organization details
-- =================================================================================


SELECT
     "d"."dt"
   , "org_users"."userrefid"
   , "org"."orgrefid"
   , (CASE WHEN ((("org_users"."type" = 'administrators') AND ("org"."orgtype" = 'district')) AND ("org"."ultimateorgtype" = 'district')) THEN 'District Admin' WHEN ("org_users"."type" = 'administrators') THEN 'School Admin' WHEN ("org_users"."type" = 'teachers') THEN 'Teachers' WHEN ("org_users"."type" = 'students') THEN 'Students' ELSE 'Unknown User Type' END) "user_type"
   , "org"."pid" "child_pid"
   , "org"."ultimatepid" "parent_pid"
   , "org"."orgtype" "child_org_type"
   , "org"."name" "child_org_name"
   , "org"."ultimateorgtype" "parent_org_type"
   , "org"."ultimatename" "parent_org_name"
   , "d"."statecode" "state_code"
   FROM
     ((v_organizations_users org_users
   LEFT JOIN v_organizations_enriched org ON ("org"."orgrefid" = "org_users"."orgrefid"))
   INNER JOIN v_organizations d ON ("d"."orgrefid" = "org"."orgrefid"))
   WHERE (("org"."category" = 'Paying') AND ("trim"("org"."pid") <> ''))


-- =================================================================================
-- Rostering details (Count) per user type per organization
-- =================================================================================

WITH orgs AS (
   SELECT
     "d"."dt"
   , "org_users"."userrefid"
   , "org"."orgrefid"
   , (CASE WHEN ((("org_users"."type" = 'administrators') AND ("org"."orgtype" = 'district')) AND ("org"."ultimateorgtype" = 'district')) THEN 'District Admin' WHEN ("org_users"."type" = 'administrators') THEN 'School Admin' WHEN ("org_users"."type" = 'teachers') THEN 'Teachers' WHEN ("org_users"."type" = 'students') THEN 'Students' ELSE 'Unknown User Type' END) "user_type"
   , "org"."pid" "child_pid"
   , "org"."ultimatepid" "parent_pid"
   , "org"."orgtype" "child_org_type"
   , "org"."name" "child_org_name"
   , "org"."ultimateorgtype" "parent_org_type"
   , "org"."ultimatename" "parent_org_name"
   , "d"."statecode" "state_code"
   FROM
     ((v_organizations_users org_users
   LEFT JOIN v_organizations_enriched org ON ("org"."orgrefid" = "org_users"."orgrefid"))
   INNER JOIN v_organizations d ON ("d"."orgrefid" = "org"."orgrefid"))
   WHERE (("org"."category" = 'Paying') AND ("trim"("org"."pid") <> ''))
)
SELECT
     "dt"
   , "orgrefid"
   , "child_pid"
   , "parent_pid"
   , "child_org_type"
   , "child_org_name"
   , "parent_org_type"
   , "parent_org_name"
   , "state_code"
   , "sum"((CASE WHEN ("user_type" = 'Teachers') THEN 1 ELSE 0 END)) "teacher_count"
   , "sum"((CASE WHEN (("user_type" = 'District Admin') AND ("child_org_type" = 'district')) THEN 1 ELSE 0 END)) "district_admin_count"
   , "sum"((CASE WHEN (("user_type" = 'School Admin') AND ("child_org_type" = 'school')) THEN 1 ELSE 0 END)) "school_admin_count"
   , "sum"((CASE WHEN ("user_type" = 'Students') THEN 1 ELSE 0 END)) "student_count"
   FROM
     orgs
   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9


-- =================================================================================
--  Programs purchased by entitled organization
-- =================================================================================

WITH
  orgs AS (
   SELECT
     "d"."dt"
   , "org_users"."userrefid"
   , "org"."orgrefid"
   , (CASE WHEN ((("org_users"."type" = 'administrators') AND ("org"."orgtype" = 'district')) AND ("org"."ultimateorgtype" = 'district')) THEN 'District Admin' WHEN ("org_users"."type" = 'administrators') THEN 'School Admin' WHEN ("org_users"."type" = 'teachers') THEN 'Teachers' WHEN ("org_users"."type" = 'students') THEN 'Students' ELSE 'Unknown User Type' END) "user_type"
   , "org"."pid" "child_pid"
   , "org"."ultimatepid" "parent_pid"
   , "org"."orgtype" "child_org_type"
   , "org"."name" "child_org_name"
   , "org"."ultimateorgtype" "parent_org_type"
   , "org"."ultimatename" "parent_org_name"
   , "d"."statecode" "state_code"
   FROM
     ((v_organizations_users org_users
   LEFT JOIN v_organizations_enriched org ON ("org"."orgrefid" = "org_users"."orgrefid"))
   INNER JOIN v_organizations d ON ("d"."orgrefid" = "org"."orgrefid"))
   WHERE (("org"."category" = 'Paying') AND ("trim"("org"."pid") <> ''))
)
, orgs_rostered AS (
   SELECT
     "dt"
   , "orgrefid"
   , "child_pid"
   , "parent_pid"
   , "child_org_type"
   , "child_org_name"
   , "parent_org_type"
   , "parent_org_name"
   , "state_code"
   , "sum"((CASE WHEN ("user_type" = 'Teachers') THEN 1 ELSE 0 END)) "teacher_count"
   , "sum"((CASE WHEN (("user_type" = 'District Admin') AND ("child_org_type" = 'district')) THEN 1 ELSE 0 END)) "district_admin_count"
   , "sum"((CASE WHEN (("user_type" = 'School Admin') AND ("child_org_type" = 'school')) THEN 1 ELSE 0 END)) "school_admin_count"
   , "sum"((CASE WHEN ("user_type" = 'Students') THEN 1 ELSE 0 END)) "student_count"
   FROM
     orgs
   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)
   SELECT DISTINCT
     COALESCE("child_org"."orgrefid", "org"."orgrefid") "orgrefid"
   , "lower"(COALESCE("mds"."programid", "ent_program_code", "soli_program_code")) "program_purchased"
   FROM
     (((v_organizations_enriched org
   INNER JOIN staging_prod.view_entitlements_consolidated ent ON ("org"."orgrefid" = "ent"."org_sif_uuid"))
   LEFT JOIN prod_mds_program_mappings_static mds ON ("trim"("mds"."isbn") = "ent"."soli_isbn"))
   LEFT JOIN v_organizations_enriched child_org ON ("child_org"."ultimatepid" = (CASE WHEN ("org"."ultimatepid" = "org"."pid") THEN "org"."ultimatepid" ELSE null END)))
   WHERE (((((("org"."category" = 'Paying') AND ("trim"("org"."pid") <> '')) AND ("ent_is_active" = '1.0000000000')) AND ("soli_is_fulfilled" = '1')) AND ("soli_egood_flag_id" = '18')) AND (("lower"(COALESCE("mds"."programid", "ent_program_code", "soli_program_code")) LIKE '%ela%ir%') OR ("lower"(COALESCE("mds"."programid", "ent_program_code", "soli_program_code")) LIKE 'im_%')))
