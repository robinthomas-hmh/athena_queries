WITH 
base_query AS (
SELECT DISTINCT 
enr.ultimateorgrefid  AS parent_org,
enr.ultimatename      AS parent_name,
enr.ultimatepid       AS parent_pid, 
enr.ultimateorgtype   AS parent_type, 
enr.orgrefid          AS org,
enr.name              AS org_name,
enr.pid               AS org_pid, 
enr.orgtype           AS org_type,
sect.userrefid        AS userid,
count(DISTINCT sect.sectionrefid) AS sec_count
FROM  
v_sections_users                    AS sect
INNER JOIN v_organizations_sections AS orgsec ON orgsec.sectionrefid = sect.sectionrefid
INNER JOIN v_organizations_enriched AS enr    ON enr.orgrefid = orgsec.orgrefid
WHERE 
  sect.usertype = 'students' AND
  enr.category  = 'Paying'
GROUP BY 
  1,2,3,3,4,5,6,7,8,9
HAVING count(DISTINCT sect.sectionrefid) > 20
ORDER BY
  10 desc,1,5,9
),
summary_dat AS (
SELECT DISTINCT
parent_org  AS district_refid,
parent_name AS district_name,
org         AS school_refid,
org_name    AS school_name,
count( userid) as user_count,
approx_percentile(sec_count,array[0.0,0.5,1.0]) AS "summary"
FROM base_query
WHERE
  parent_name NOT LIKE 'ZZ%' AND
  org_name NOT LIKE 'ZZ%' 
GROUP BY 
   1,2,3,4
)
SELECT
 district_refid,
 district_name,
 school_refid,
 school_name,
 user_count,
 array_join(summary,',') as "min, median, max"
 from summary_dat
 order by summary[3] desc