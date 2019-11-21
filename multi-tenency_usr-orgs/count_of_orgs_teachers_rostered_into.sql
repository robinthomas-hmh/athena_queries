-- Count of teachers rostered into the number of schools
with
a_dat AS (
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
/*
b_dat AS (
  SELECT distinct
  a_dat.orgid as orgs
  FROM a_dat
  INNER JOIN v_organizations_enriched org_e   ON a_dat.orgid = org_e.orgrefid
  INNER JOIN v_organizations_users    org_usr ON org_e.orgrefid = org_usr.orgrefid
  WHERE 
  org_usr.type = 'teachers' AND
  org_e.category = 'Paying'
  ),
 
c_dat AS (
select distinct orgid from a_dat
),
*/
base AS (
SELECT DISTINCT
org_usr.userrefid     AS userid, 
count(distinct org_usr.orgrefid) org_cnt
FROM v_organizations_users                  org_usr
INNER JOIN v_organizations_enriched         org_e   ON org_usr.orgrefid = org_e.orgrefid
-- INNER JOIN v_organizations                  org     ON org_e.orgrefid = org.orgrefid
WHERE 
  org_usr.orgrefid NOT IN ( SELECT orgid FROM a_dat) AND
  org_usr.type = 'teachers' AND
  org_e.category = 'Paying'
GROUP BY 
1
HAVING count(distinct org_usr.orgrefid) > 1
ORDER BY 
2 desc, 1
)
/*
select 
count(distinct userid) as teacher_count
from base
*/
SELECT
count(distinct userid) as teacher_count,
org_cnt as num_of_orgs_rostered_to
from base
group by 2
order by 2