WITH
dat AS (
select 
all_org.statecode      AS  "state_code",
org.ultimateorgrefid   AS  "parent_org",
org.ultimatename       AS  "parent_org_name",
org.ultimatepid        AS  "parent_pid",
org.ultimateorgtype    AS  "parent_type",
org.orgrefid           AS  "child_org",
org.name               AS  "child_name",
org.pid                AS  "child_pid",  
org.orgtype            AS  "org_type",
sec_user.userrefid     AS  "userrefid",
sec_user.usertype      AS  "user_type",
sec.sectionrefid       AS  "section_refid",
sec.name               AS  "section_name"
from v_organizations_enriched  org
join v_organizations           all_org    ON org.orgrefid          = all_org.orgrefid
join v_organizations_sections  org_sec    ON org.orgrefid          = org_sec.orgrefid
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
      WHERE -- orgs.ultimateorgrefid = 'a9270a6e-df5b-4069-a02d-6e234c0d4dba' AND 
            orgs.category = 'Paying' AND TRIM(orgs.pid) <> '' -- Get entitlements for live orgs
            AND ent.ent_is_active = '1.0000000000' -- Bring in active entitlements only
            AND ent.soli_is_fulfilled = '1' -- Bring in active entitlements only
            AND ent.soli_egood_flag_id = '18' -- SAP orders for ED
  )
   dist_orgs ON org.orgrefid = dist_orgs.orgrefid

where -- org.ultimateorgrefid = 'a9270a6e-df5b-4069-a02d-6e234c0d4dba' AND 
      org.category = 'Paying' AND
      trim(org.pid) <> '' AND
      sec_user.usertype = 'students' -- 'leadTeachers' 
  order by 1,2,6,10,12
  ),
dat_students AS (
select *
from dat
where userrefid IN ( 
  select distinct userrefid 
  from v_sections_users 
  where usertype = 'students'
  group by 1
  having count(sectionrefid) = 1
  )
AND section_refid IN (
  select 
  sectionrefid 
  from v_sections_users sec_usr
  where usertype != 'students'
  AND userrefid NOT IN 
  ( 
    select userid 
    from "prod_analysis"."ubd_teachers_logins" ubd_teacher 
--    where ubd_teacher.userid = sec_usr.userrefid     
    )
 )
)
select * from dat_students

-- count(distinct section_refid),
-- count(distinct userrefid) 
-- from dat_students


-- ========================================================================================================================================
-- ========================================================================================================================================
-- ========================================================================================================================================

WITH
dat AS (
select 
all_org.statecode      AS  "state_code",
org.ultimateorgrefid   AS  "parent_org",
org.ultimatename       AS  "parent_org_name",
org.ultimatepid        AS  "parent_pid",
org.ultimateorgtype    AS  "parent_type",
org.orgrefid           AS  "child_org",
org.name               AS  "child_name",
org.pid                AS  "child_pid",  
org.orgtype            AS  "org_type",
sec_user.userrefid     AS  "userrefid",
sec_user.usertype      AS  "user_type",
sec.sectionrefid       AS  "section_refid",
sec.name               AS  "section_name"
from v_organizations_enriched  org
join v_organizations           all_org    ON org.orgrefid          = all_org.orgrefid
join v_organizations_sections  org_sec    ON org.orgrefid          = org_sec.orgrefid
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
      WHERE -- orgs.ultimateorgrefid = 'a9270a6e-df5b-4069-a02d-6e234c0d4dba' AND 
            orgs.category = 'Paying' AND TRIM(orgs.pid) <> '' -- Get entitlements for live orgs
            AND ent.ent_is_active = '1.0000000000' -- Bring in active entitlements only
            AND ent.soli_is_fulfilled = '1' -- Bring in active entitlements only
            AND ent.soli_egood_flag_id = '18' -- SAP orders for ED
  )
   dist_orgs ON org.orgrefid = dist_orgs.orgrefid

where -- org.ultimateorgrefid = 'a9270a6e-df5b-4069-a02d-6e234c0d4dba' AND 
      org.category = 'Paying' AND
      trim(org.pid) <> '' AND
      sec_user.usertype = 'students' -- 'leadTeachers' 
  order by 1,2,6,10,12
  ),
dat_students AS (
select *
from dat
where userrefid IN ( 
  select distinct userrefid 
  from v_sections_users 
  where usertype = 'students'
  group by 1
  having count(sectionrefid) = 1
  )
AND section_refid IN (
  select 
  distinct sectionrefid 
  from v_sections_users sec_usr
  where usertype != 'students'
  AND NOT EXISTS (select 1 from "prod_analysis"."ubd_user_program_data" ubd where sec_usr.userrefid = ubd.userid AND ubd.roles = 'Instructor') 
 )
)
select
 count(distinct userrefid) , count(distinct section_refid)
 from dat_students
 where NOT EXISTS (select 1 from "prod_analysis"."ubd_user_program_data" ubd where dat_students.userrefid = ubd.userid AND ubd.roles = 'Learner')
