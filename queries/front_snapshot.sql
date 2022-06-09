with container_placements as (
  select *, row_number() over(partition by time_15min, container_rank) as reverse_container_position
  from `trim-mechanism-126723.chmedia.tmp_aaz_15min_front_placements`
  where not container_rank is null
    and date(time_15min) = "2022-04-12"
),

front_placements as (
  select
    *,
    row_number() over(partition by time_15min order by container_rank asc, reverse_container_position asc) as front_position
  from container_placements
),

unique_articles as (
  select article_id
  from front_placements
  group by 1
)

select
  c.article_id,
  b.title,
  b.publication_date,
  -- if(length(b.sub_category)>0,b.department || " > " || b.sub_category, b.department) as section,
  -- "https://" || b.origin || "/" || a.article_id as url,
  a.* except(article_id)
from unique_articles as c
left join chmedia.articles b
  on c.article_id = b.article_id
left join front_placements a
  on c.article_id = a.article_id
  and a.time_15min = "2022-04-12 21:00:00"
