/*Misc codes */
select * from (values(100),(200),(300))

select a,b,c from (values(100,200,300)) as t(a,b,c)

select abc from (values 100,200,300) as t(abc)

select a from UNNEST(sequence(1,10)) as t(a)

