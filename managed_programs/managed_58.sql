WITH
dat_students AS 
(
select *
from all_entitled_user_section_data
where userrefid IN ( 
  select distinct userrefid 
  from "staging_prod"."v_sections_users" 
  where usertype = 'students'
  group by 1
  having count(sectionrefid) > 1
  )  
),
all_sections AS (
select 
  distinct dat_students.userrefid, 
  dat_students.section_refid,
  oac.sectionrefid,
  CASE WHEN oac.sectionrefid is null THEN 'non_managed'
       ELSE 'managed'
       END AS "flag"
  from dat_students
  LEFT JOIN oac_managed_sections oac
  ON dat_students.section_refid = oac.sectionrefid
  ),
  split AS (
  select distinct
  userrefid,
  array_agg(flag) as arr
  from all_sections
    group by userrefid
  order by userrefid
    ), final as (
   select userrefid, arr
   from split
   where contains(arr, 'managed')
   and  contains(arr, 'non_managed')
      )
      select count(distinct userrefid) from final

   
