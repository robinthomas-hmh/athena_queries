with
org_data AS (
  select distinct
  a.orgrefid as orgid,
  a.orgtype as org_type,
  b.orgrefid,
  b.orgtype,
  b.parentorgrefid, 
  b.parentorgtype,
  c.orgrefid,
  c.orgtype,
  c.parentorgrefid, 
  c.parentorgtype
  from
  v_organizations a
  left join v_organizations_parentorganizations b ON a.orgrefid = b.orgrefid
  left join v_organizations_parentorganizations c ON a.orgrefid = c.parentorgrefid
  having
  b.orgrefid is null AND
  b.parentorgrefid is null AND
  c.orgrefid is null AND
  c.parentorgrefid is null AND
  a.orgtype = 'school'
),
base AS (
  SELECT DISTINCT
  org.orgrefid as orgrefid,
  org.orgtype as orgtype, 
  ent.org_sif_uuid as soli_orgrefid,
  ent.soli_product_bundle_title as product_title,
  ent.soli_egood_flag_id as platform_id
  FROM 
  v_organizations                      org
  INNER JOIN v_organizations_enriched  org_e  ON org.orgrefid = org_e.orgrefid
  INNER JOIN entitlements_all          ent    ON org.orgrefid = ent.org_sif_uuid
  WHERE
  org.orgrefid NOT IN ( SELECT orgid FROM org_data) AND
  org_e.category = 'Paying'
),
agg_data AS (
  SELECT DISTINCT
  orgrefid,
  orgtype,
  array_agg(distinct base.product_title) as programs,
  array_agg(distinct base.platform_id)   AS platforms
  FROM 
  base
  GROUP BY 1,2
),
data_final AS (
  SELECT distinct
  org_usr.userrefid     AS userid, 
  agg_data.orgrefid AS orgrefid,
  agg_data.orgtype,
  agg_data.programs,
  agg_data.platforms
  FROM v_organizations_users org_usr
  INNER JOIN agg_data ON agg_data.orgrefid = org_usr.orgrefid
  WHERE
  org_usr.type = 'teachers' 
  ORDER BY 1,2
  )
SELECT distinct
userid,
cardinality(array_agg(orgrefid)) as org_cnt,
array_agg(orgrefid) as orgids,
CASE
   WHEN cardinality(array_distinct(flatten(array_agg(programs)))) > 1
   THEN 'Yes'
   ELSE 'No'
END AS "different_programs_flag",
CASE
  WHEN cardinality(array_distinct(flatten(array_agg(platforms)))) > 1
  THEN 'Yes'
  ELSE 'No'
END AS "different_platform_flag"
FROM data_final
group by 1
HAVING
cardinality(array_agg(orgrefid)) > 1
ORDER BY 2
