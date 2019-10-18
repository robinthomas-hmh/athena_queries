WITH
base_query as (

select 
org_org.parentorgrefid as parent_org, 
org_org.parentorgtype as parent_org_type, 
org_org.parentpid as parent_pid, 
org_org.orgrefid as sub_org,
org_org."start" as org_start, 
org_org."end" as org_end, 

org_sec.sectionrefid as section,
org_sec.organizationsectionstart section_start,
org_sec.organizationsectionend section_end, 

sec_grp.grouprefid as parent_group,
sec_grp."start" parent_group_start, 
sec_grp."end" as parent_group_end, 

grp_subgrp.grouprefid as sub_group,
grp_subgrp."start" as subgroup_start, 
grp_subgrp."end" as subgroup_end, 

grp_usr.userrefid as userid,
grp_usr."start" as group_user_start, 
grp_usr."end" as group_user_end

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
and org_org.dt     ='2019-09-15'
and org_sec.dt     ='2019-09-15'
and sec_grp.dt     ='2019-09-15'
and grp_subgrp.dt  ='2019-09-15'
and grp_usr.dt     ='2019-09-15'
order by org_org.parentorgrefid
),

group_data as (
select parent_org, 
     sub_org,
     section,
       parent_group,
     -- count(distinct parent_group) as total_groups,
       sub_group,
       count(userid) as users_in_group
     from base_query
     group by parent_org, sub_org, section,parent_group, sub_group
       order by parent_org, sub_org, section
  )
select parent_org, 
     sub_org,
     section,
       count(sub_group) as total_groups,
       array_join(array_agg(users_in_group), ',') as users_per_group
       from group_data
       group by parent_org, sub_org, section
       order by parent_org, sub_org, section
      
 #------========================================================================
 #------========================================================================
 #------========================================================================
 #------========================================================================






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

group_data as (
select parent_org, 
     sub_org,
     section,
       parent_group,
       sub_group,
       date_diff('second', subgroup_start, subgroup_end) as group_duration,
       count(userid) as users_in_group,
       array_agg(date_diff('second', group_user_start, group_user_end)) as user_grp_duration
     from base_query
     group by parent_org, sub_org, section,parent_group, sub_group
       order by parent_org, sub_org, section
  )
select parent_org, 
     sub_org,
     section,
       parent_group,
       count(sub_group) as total_groups,
       array_agg(group_duration) as group_duration,
       array_join(array_agg(users_in_group), ',') as users_per_group,
       user_grp_duration
       from group_data
       group by parent_org, sub_org, section,parent_group
       order by parent_org, sub_org, section,parent_group

#------========================================================================
#------========================================================================
#------========================================================================
#------========================================================================
  #------========================================================================
 #------========================================================================
 ### GROUPS per class
 #------========================================================================
 #------========================================================================

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
count_query as(
select parent_org as district,
     sub_org as school,
     section as class,
       count(sub_group) as counts 
       from base_query
       group by 1,2,3
)
-- select * from count_query
select district,
       school,
       array_agg(counts) as groups_per_class
       from count_query 
       group by 1,2

#------========================================================================
#------========================================================================
#------========================================================================
#------========================================================================
#------========================================================================
#------========================================================================
### group duration and users in group
#------========================================================================
#------========================================================================

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
         -- date_diff('second', subgroup_start, subgroup_end) as group_duration,
         date_diff('day', group_user_start, group_user_end) as user_grp_duration
         from base_query
),
group_data as (
select parent_org, 
     sub_org,
     section,
       parent_group,
       sub_group,
       date_diff('day', subgroup_start, subgroup_end) as group_duration,
       count(userid) as users_in_group,
       -- array_agg(group_duration) as grp_duration,
       array_agg(userid) as users_in_group,
       array_agg(user_grp_duration) AS user_grp_duration
     from time_diff_data
       GROUP BY 1,2,3,4,5,6
)
select * from group_data


#------========================================================================
##### Group manual / recommend
#------========================================================================
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

#------========================================================================
#------========================================================================
#------======= Group recoomend / not(edited or not)   ==========================
#------========================================================================
WITH 
group_data as (
select 
  grp.grouprefid AS "grouprefid",
  grp.name AS "name",
  grp.source AS "source",
  length(grp.source) as "src_len",
  grp.createdtime AS "createdtime",
  sec_grp.sectionrefid AS "section",
  org.orgrefid AS "orgid",
  org.name AS "org_name",
  org.orgtype AS "org_type",
  org.ultimateorgrefid AS "parent_org",
  org.ultimatename AS "parent_org_name",
  org.ultimateorgtype AS "parent_org_type",
  org_all.statename AS "state",
  org_all.statecode AS "state_id" 
  from v_groups grp
  LEFT JOIN v_sections_groups_history sec_grp ON grp.grouprefid = sec_grp.grouprefid
  LEFT JOIN v_organizations_sections_history org_sec ON sec_grp.sectionrefid = org_sec.sectionrefid
  LEFT JOIN v_organizations_enriched org ON org_sec.orgrefid = org.orgrefid
  LEFT JOIN v_organizations org_all ON org.orgrefid = org_all.orgrefid
  where grp.source !='undefined'
  AND org.category = 'Paying' 
  AND TRIM(org.pid) <> ''

),
group_type as (
  select 
    group_data.state,
    group_data.state_id,
    group_data.parent_org,
    group_data.parent_org_name,
    group_data.parent_org_type,
    group_data.orgid,
    group_data.org_name,
    group_data.org_type, 
    group_data.section,
    group_data.grouprefid AS "parent_group",
    group_data.name AS "parent_name", 
    group_subgroup.grouprefid AS "subgroup",
    group_data.source AS "parent_source", 
    group_data.src_len AS "source_len", 
    group_data.createdtime AS "parent_group_created_time"
    from group_data
    LEFT JOIN v_groups_subgroups_history group_subgroup
    on group_data.grouprefid = group_subgroup.parentgrouprefid
    where 
      group_data.src_len = 36 
      or group_data.src_len = 0
     -- and group_subgroup.dt = "date_format"((current_date - INTERVAL  '1' DAY), '%Y-%m-%d')

),
group_type_data as (
select
  state,
  state_id,
  parent_org,
  parent_org_name,
  parent_org_type,
  orgid,
  org_name,
  org_type, 
  section,  
  parent_group,
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

),
group_users_joined AS (
  select
    state,
    state_id,  
    parent_org,
    parent_org_name,
    parent_org_type,
    orgid,
    org_name,
    org_type, 
    section,
    gtype.parent_group AS "parent_group",
    gtype.parent_name AS "parent_name",
    gtype.subgroup AS "subgroup",
    gtype.group_type AS "group_type",
    gtype.parent_source AS "parent_source",
    grp_usr.grouprefid AS "grouprefid",
    grp_usr.userrefid AS "userrefid",
    grp_usr.start AS "user_start",
    grp_usr."end" AS "user_end"
    from group_type_data gtype
    LEFT JOIN v_groups_users_history grp_usr 
    ON gtype.subgroup = grp_usr.grouprefid
     -- where grp_usr.dt = "date_format"((current_date - INTERVAL  '1' DAY), '%Y-%m-%d')

),
group_all_data AS(
  select
    state,
    state_id,  
    parent_org,
    parent_org_name,
    parent_org_type,
    orgid,
    org_name,
    org_type, 
    section,
    parent_group,
    parent_name,
    subgroup,
    group_type,
    parent_source,
    array_agg(userrefid) AS "users",
    cardinality(array_agg(userrefid)) AS "users_in_group",
    array_distinct(array_agg(user_start)) AS "users_start",
    cardinality(array_distinct(array_agg(user_start))) AS "nos_start",
    array_distinct(array_agg(user_end)) AS "users_end",
    cardinality(array_distinct(array_agg(user_end))) AS "nos_end"
    from group_users_joined
    -- where group_type = 'Recommended Group'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

)
select
    state,
    state_id,
    parent_org,
    parent_org_name,
    parent_org_type,
    orgid,
    org_name,
    org_type, 
    section,  
    parent_group,
    parent_name,
    subgroup,
    parent_source,
    group_type,
    CASE
      WHEN nos_start > 1 OR nos_end > 1
      THEN 'Group Edited'
      END AS "group_status"
    from group_all_data

#------========================================================================
#------========================================================================

/*
The below SQL is used to find all the Groups and Subgroups and the related 
details( state, parent org, org, section) which have been assigned a material.
All the assignments which have a 
*/
select 
orgs.statecode, 
asgt.organisation_lea_refid, 
asgt.organisation_school_refid, 
asgt.section_refid, 
asgt.parent_group_refid,
asgt.student_group_refid,
asgt.so_attr_programid,
asgt.title,
asgt.teacher_assignment_refid,
asgt.student_assignment_refid,
CASE
WHEN asgt.student_assignment_status  = '0' THEN 'NOT_STARTED'
WHEN asgt.student_assignment_status  = '1' THEN 'IN_PROGRESS'
WHEN asgt.student_assignment_status  = '2' THEN 'COMPLETED'
WHEN asgt.student_assignment_status  = '3' THEN 'EXPIRED'
WHEN asgt.student_assignment_status  = '4' THEN 'NOT_SCORED'
WHEN asgt.student_assignment_status  = '5' THEN 'PEER_REVIEW_REQUIRED'
WHEN asgt.student_assignment_status  = '6' THEN 'READY_FOR_SCORING'
WHEN asgt.student_assignment_status  = '7' THEN 'TEACHER_ACTION_REQUIRED'
WHEN asgt.student_assignment_status  = '8' THEN 'TURNED_IN'
WHEN asgt.student_assignment_status  = '9' THEN 'SCORING_IN_PROGRESS'
END AS "student_assignment_status",
CASE
WHEN asgt.teacher_assignment_status  = '0' THEN 'NOT_STARTED'
WHEN asgt.teacher_assignment_status  = '1' THEN 'IN_PROGRESS'
WHEN asgt.teacher_assignment_status  = '2' THEN 'COMPLETED'
WHEN asgt.teacher_assignment_status  = '3' THEN 'EXPIRED'
WHEN asgt.teacher_assignment_status  = '4' THEN 'NOT_SCORED'
WHEN asgt.teacher_assignment_status  = '5' THEN 'PEER_REVIEW_REQUIRED'
WHEN asgt.teacher_assignment_status  = '6' THEN 'READY_FOR_SCORING'
WHEN asgt.teacher_assignment_status  = '7' THEN 'TEACHER_ACTION_REQUIRED'
WHEN asgt.teacher_assignment_status  = '8' THEN 'TURNED_IN'
WHEN asgt.teacher_assignment_status  = '9' THEN 'SCORING_IN_PROGRESS'
END AS "teacher_assignment_status"
from v_assignments asgt
JOIN v_organizations orgs ON asgt.organisation_lea_refid = orgs.orgrefid
where asgt.student_group_refid != ''
limit 10

#------========================================================================
#------========================================================================
#------========================================================================
#------========================================================================



