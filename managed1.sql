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
  fin as
  (
  select * from dat where section_refid NOT IN (
    select sectionrefid from "devtest"."oac_managed_sections")
    )
    select * from fin
--   LEFT OUTER JOIN "devtest"."oac_managed_sections" oac ON dat.section_refid = oac.sectionrefid


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
  fin as
  (
  select * 
    from dat 
    where NOT EXISTS 
          (select 1 from "devtest"."oac_managed_sections" oms 
                    where oms.sectionrefid = dat.section_refid )
  )
  select distinct userrefid -- count(distinct userrefid) 
    from fin
    where NOT EXISTS 
          ( select 1 
                   from "ids_streaming_events"."prod_hmheng_idm_production_ids_orgusers" ids_stream
                   where ids_stream.refid = fin.userrefid
                   -- and ids_stream.dt = '2019-09-25'
           )