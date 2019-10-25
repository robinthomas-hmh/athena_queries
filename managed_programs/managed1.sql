WITH
dat AS (
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
sec_user.userrefid     AS  "userid",
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
  ),
  final_data as
  (
  select * from dat where section_refid NOT IN (
    select sectionrefid from oac_managed_sections)
    )
    select * from final_data
   LEFT OUTER JOIN "devtest"."oac_managed_sections" oac ON dat.section_refid = oac.sectionrefid


================================

================================

WITH
dat AS (
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
  ),
final_data as
(
select * 
  from dat 
  where NOT EXISTS 
        (select 1 from "prod_analysis"."v_active_oac_managed_sections" oms 
                  where oms.sectionrefid = dat.section_refid )
)
  select distinct  -- count(distinct userrefid)
    final_data.state_code,
    final_data.parent_org,
    final_data.parent_org_name,
    final_data.parent_pid,
    final_data.parent_type,
    final_data.org,
    final_data.child_name,
    final_data.child_pid,
    final_data.org_type,
    final_data.userrefid,
    final_data.user_type,
    ids_stream.username,
    ids_stream.email,
    ids_stream.firstname,
    ids_stream.lastname,
    final_data.section_refid,
    final_data.section_name
    from final_data
    LEFT join "ids_streaming_events"."prod_hmheng_idm_production_ids_orgusers" ids_stream
    ON final_data.userrefid = ids_stream.refid
    
   /* where NOT EXISTS 
          ( select 1 
                   from "ids_streaming_events"."prod_hmheng_idm_production_ids_orgusers" ids_stream
                   where ids_stream.refid = fin.userrefid
                   -- and ids_stream.dt = '2019-09-25'
           )
  */
-- ====================================================================================================================
-- ====================================================================================================================
  /* SELECT all user details PII data */
-- ====================================================================================================================
-- ====================================================================================================================  
-- ==========================
-- first create PII data table
-- ==========================
CREATE TABLE current_ids_pii_data AS
select distinct t1.refid, t1.username, t1."email", t1.firstname, t1.lastname, t2.mxdat
from "ids_streaming_events"."prod_hmheng_idm_production_ids_orgusers" t1
INNER JOIN (
  select refid, max(dt) as mxdat
  from "ids_streaming_events"."prod_hmheng_idm_production_ids_orgusers"
  group by refid
  ) t2
ON t1.refid = t2.refid
AND t1.dt = t2.mxdat

-- and then run the query to fetch the data 

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
from "staging_prod"."v_organizations_enriched"  org
join "staging_prod"."v_organizations"           all_org    ON org.orgrefid          = all_org.orgrefid
join "staging_prod"."v_organizations_sections"  org_sec    ON org.orgrefid          = org_sec.orgrefid
join "staging_prod"."v_sections_users"          sec_user   ON org_sec.sectionrefid  = sec_user.sectionrefid
join "staging_prod"."v_sections"                sec        ON sec_user.sectionrefid = sec.sectionrefid
JOIN (
   SELECT DISTINCT
            COALESCE(child_org.orgrefid, orgs.orgrefid) AS orgrefid
       FROM "staging_prod"."v_organizations_enriched" orgs
       JOIN "staging_prod"."view_entitlements_consolidated" ent ON (orgs.orgrefid = ent.org_sif_uuid)
  LEFT JOIN "staging_prod"."v_organizations_enriched" child_org ON (child_org.ultimatepid = CASE WHEN orgs.ultimatepid = orgs.pid
                                                                                THEN orgs.ultimatepid
                                                                                ELSE NULL
                                                   END)
      WHERE orgs.category = 'Paying' AND TRIM(orgs.pid) <> '' -- Get entitlements for live orgs
            AND ent.ent_is_active = '1.0000000000' -- Bring in active entitlements only
            AND ent.soli_is_fulfilled = '1' -- Bring in active entitlements only
            AND ent.soli_egood_flag_id = '18' -- SAP orders for ED
  )
   dist_orgs ON org.orgrefid = dist_orgs.orgrefid

where org.category = 'Paying' AND
      trim(org.pid) <> '' AND
      sec_user.usertype = 'leadTeachers'
-- order by 1,2,6,10,12
  ),
final_data as
(
select * 
  from dat 
  where NOT EXISTS 
        (select 1 from "prod_analysis"."v_active_oac_managed_sections" oms 
                  where oms.sectionrefid = dat.section_refid )
),
non_managed AS (
  select distinct  -- count(distinct userrefid)
    final_data.userrefid,
    ids_stream.username,
    ids_stream."email",
    ids_stream.firstname,
    ids_stream.lastname
    from final_data
    LEFT join "ids_streaming_events"."prod_hmheng_idm_production_ids_orgusers" ids_stream
    ON final_data.userrefid = ids_stream.refid
    )
select 
 username,
 email,
 firstname,
 lastname
 from non_managed
 

 -- ===++=========++====+==+=+====+===++==++++===+=+=+=+=+===+==
 -- Select all users where teachers LOGIN / Not logged on to ED
 select 
 username,
 email,
 firstname,
 lastname
 from non_managed
 WHERE NOT EXISTS
          ( select 1 from "prod_analysis"."ubd_teachers_logins" ubd_teacher
                     where ubd_teacher.userid = non_managed.userrefid )
 order by 2
 
-- ===++=========++====+==+=+====+===++==++++===+=+=+=+=+===+==
 /*
  select count( distinct userrefid), 'Logged into Ed'
 from non_managed
 WHERE  EXISTS
          ( select 1 from "prod_analysis"."ubd_teachers_logins" ubd_teacher
                     where ubd_teacher.userid = non_managed.userrefid )
 UNION
 select count( distinct userrefid), 'Not Logged into Ed'
 from non_managed
 WHERE NOT EXISTS
          ( select 1 from "prod_analysis"."ubd_teachers_logins" ubd_teacher
                     where ubd_teacher.userid = non_managed.userrefid )

*/
 -- ===++=========++====+==+=+====+===++==++++===+=+=+=+=+===+==

 -- ====================================================================================================================
 -- ====================================================================================================================

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
from "staging_prod"."v_organizations_enriched"  org
join "staging_prod"."v_organizations"           all_org    ON org.orgrefid          = all_org.orgrefid
join "staging_prod"."v_organizations_sections"  org_sec    ON org.orgrefid          = org_sec.orgrefid
join "staging_prod"."v_sections_users"          sec_user   ON org_sec.sectionrefid  = sec_user.sectionrefid
join "staging_prod"."v_sections"                sec        ON sec_user.sectionrefid = sec.sectionrefid
JOIN (
   SELECT DISTINCT
            COALESCE(child_org.orgrefid, orgs.orgrefid) AS orgrefid
       FROM "staging_prod"."v_organizations_enriched" orgs
       JOIN "staging_prod"."view_entitlements_consolidated" ent ON (orgs.orgrefid = ent.org_sif_uuid)
  LEFT JOIN "staging_prod"."v_organizations_enriched" child_org ON (child_org.ultimatepid = CASE WHEN orgs.ultimatepid = orgs.pid
                                                                                THEN orgs.ultimatepid
                                                                                ELSE NULL
                                                   END)
      WHERE orgs.category = 'Paying' AND TRIM(orgs.pid) <> '' -- Get entitlements for live orgs
            AND ent.ent_is_active = '1.0000000000' -- Bring in active entitlements only
            AND ent.soli_is_fulfilled = '1' -- Bring in active entitlements only
            AND ent.soli_egood_flag_id = '18' -- SAP orders for ED
  )
   dist_orgs ON org.orgrefid = dist_orgs.orgrefid

where org.category = 'Paying' AND
      trim(org.pid) <> '' AND
      sec_user.usertype = 'leadTeachers'
-- order by 1,2,6,10,12
  ),
final_data as
(
select * 
  from dat 
  where NOT EXISTS 
        (select 1 from "prod_analysis"."v_active_oac_managed_sections" oms 
                  where oms.sectionrefid = dat.section_refid )
),
non_managed AS (
  select distinct  -- count(distinct userrefid)
    final_data.userrefid,
    ids_stream.username,
    ids_stream."email",
    ids_stream.firstname,
    ids_stream.lastname
    from final_data
    LEFT join "prod_analysis"."current_ids_pii_data" ids_stream
    ON final_data.userrefid = ids_stream.refid
    )
select 
 username,
 email,
 firstname,
 lastname
 from non_managed
 WHERE EXISTS
          ( select 1 from "prod_analysis"."ubd_user_program_data" ubd_teacher
                     where ubd_teacher.userid = non_managed.userrefid 
                     AND ubd_teacher.roles = 'Instructor'
          )
 order by 2
 
 
 -- ====================================================================================================================
 -- ====================================================================================================================
 -- ====================================================================================================================