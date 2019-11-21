SELECT
count(distinct org_usr.userrefid)   AS all_teacher_count
FROM v_organizations_users                  org_usr
INNER JOIN v_organizations_enriched         org_e   ON org_usr.orgrefid = org_e.orgrefid
WHERE 
  org_usr.type = 'teachers' AND
  org_e.category = 'Paying'
