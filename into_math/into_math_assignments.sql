Into Maths Assignment SQL

WITH
dat_asgmt AS
(
select distinct 
teacher_assignment_refid, 
teacher_assignment_lea_refid AS "parent_orgrefid",
teacher_assignment_school_refid AS "orgrefid", 
title, 
value, 
so_attr_programid, 
teacher_assignment_created_date_time, 
teacher_assignment_updated_date_time, 
CASE 
    WHEN teacher_assignment_status = '0' THEN 'NOT_STARTED'
    WHEN teacher_assignment_status = '1' THEN 'IN_PROGRESS'
    WHEN teacher_assignment_status = '2' THEN 'COMPLETED'
    WHEN teacher_assignment_status = '3' THEN 'EXPIRED'
    WHEN teacher_assignment_status = '4' THEN 'NOT_SCORED'
    WHEN teacher_assignment_status = '5' THEN 'PEER_REVIEW_REQUIRED'
    WHEN teacher_assignment_status = '6' THEN 'READY_FOR_SCORING'
    WHEN teacher_assignment_status = '7' THEN 'TEACHER_ACTION_REQUIRED'
    WHEN teacher_assignment_status = '8' THEN 'TURNED_IN'
    WHEN teacher_assignment_status = '9' THEN 'SCORING_IN_PROGRESS'
    ELSE 'UNKNOWN'
    END AS teacher_assignment_status, 
CASE 
    WHEN student_assignment_status = '0' THEN 'NOT_STARTED'
    WHEN student_assignment_status = '1' THEN 'IN_PROGRESS'
    WHEN student_assignment_status = '2' THEN 'COMPLETED'
    WHEN student_assignment_status = '3' THEN 'EXPIRED'
    WHEN student_assignment_status = '4' THEN 'NOT_SCORED'
    WHEN student_assignment_status = '5' THEN 'PEER_REVIEW_REQUIRED'
    WHEN student_assignment_status = '6' THEN 'READY_FOR_SCORING'
    WHEN student_assignment_status = '7' THEN 'TEACHER_ACTION_REQUIRED'
    WHEN student_assignment_status = '8' THEN 'TURNED_IN'
    WHEN student_assignment_status = '9' THEN 'SCORING_IN_PROGRESS'
    ELSE 'UNKNOWN'
    END AS student_assignment_status
from "staging_prod"."v_assignments"  asgmt
where EXISTS
           (select 1 
            from "prod_analysis"."into_math_assignments" intomath
            where intomath.activity = asgmt.value)
),
dat_joined AS (
select distinct 
dt.parent_orgrefid                         AS "parent_orgrefid",
org.ultimatepid                            AS "parent_org_pid",     
org.ultimatename                           AS "parent_org_name",        
org.ultimateorgtype                        AS "parent_org_type",
dt.orgrefid                                AS "orgrefid",       
org.pid                                    AS "org_pid",    
org.name                                   AS "org_name",
org.orgtype                                AS "org_type",
dt.teacher_assignment_refid                AS "assignment_refid",  
dt.title                                   AS "assignment_title",
dt.value                                   AS "activity_code",
dt.so_attr_programid                       AS "programid",
mds.programname                            AS "program_name",
dt.teacher_assignment_created_date_time    AS "created_date",
dt.teacher_assignment_updated_date_time    AS "updated_date",
dt.teacher_assignment_status               AS "teacher_status",
dt.student_assignment_status               AS "student_status",
count(dt.student_assignment_status)        AS "student_status_count"
FROM dat_asgmt dt
JOIN "staging_prod"."v_organizations_enriched" org         ON dt.orgrefid = org.orgrefid
JOIN "staging_prod"."prod_mds_program_mappings_static" mds ON dt.so_attr_programid = mds.programid
WHERE org.category = 'Paying' AND
      trim(org.pid) <> ''
 GROUP BY 1,2,3,4,5,6,7,8, 9, 10, 11, 12, 13, 14, 15, 16, 17
  ),
  dat_agg AS (
  select
    parent_orgrefid,
    parent_org_pid,
    parent_org_name,
    parent_org_type,
    orgrefid,       
    org_pid,    
    org_name,
    org_type,
    assignment_refid,
    assignment_title,
    activity_code,
    programid,
    program_name,
    created_date,
    updated_date,
    teacher_status,
    map_agg(student_status, student_status_count) AS "student_status"
  from dat_joined
  GROUP BY 1,2,3,4,5,6,7,8, 9, 10, 11, 12, 13, 14, 15, 16
    )
    select
    parent_orgrefid,
    parent_org_pid,
    parent_org_name,
    parent_org_type,
    orgrefid,       
    org_pid,    
    org_name,
    org_type,
    assignment_refid,
    assignment_title,
    activity_code,
    programid,
    program_name,
    created_date,
    updated_date,
    teacher_status,
    student_status['NOT_STARTED'            ] AS "student_status:NOT_STARTED",
    student_status['IN_PROGRESS'            ] AS "student_status:IN_PROGRESS",
    student_status['COMPLETED'              ] AS "student_status:COMPLETED",
    student_status['EXPIRED'                ] AS "student_status:EXPIRED",
    student_status['NOT_SCORED'             ] AS "student_status:NOT_SCORED",
    student_status['PEER_REVIEW_REQUIRED'   ] AS "student_status:PEER_REVIEW_REQUIRED",
    student_status['READY_FOR_SCORING'      ] AS "student_status:READY_FOR_SCORING",
    student_status['TEACHER_ACTION_REQUIRED'] AS "student_status:TEACHER_ACTION_REQUIRED",
    student_status['TURNED_IN'              ] AS "student_status:TURNED_IN",
    student_status['SCORING_IN_PROGRESS'    ] AS "student_status:SCORING_IN_PROGRESS"
    from dat_agg
    ORDER BY 1,2,3,4,5,6,7,8, 9, 10, 11, 12