with home_views as (
    select time_15min, device, nviews as home_views
    from `trim-mechanism-126723.chmedia.tmp_aaz_15min_front`
    where page_id = "home"
),

container_placements as (
  select *, row_number() over(partition by time_15min, container_rank) as reverse_container_position
  from `trim-mechanism-126723.chmedia.tmp_aaz_15min_front_placements`
  where not container_rank is null
),

front_placements as (
  select
    *,
    row_number() over(partition by time_15min order by container_rank asc, reverse_container_position asc) as front_position
  from container_placements
),

bins as (
    select
        a.time_15min,
        a.device,
        nviews as article_views,
        container_rank,
        front_position,
        home_views
    from `trim-mechanism-126723.chmedia.tmp_aaz_15min_front` a
    left join home_views b
        on a.time_15min = b.time_15min
        and a.device = b.device
    inner join front_placements c
        on a.time_15min = c.time_15min
        and a.page_id = c.article_id
    where not page_id = "home"
        and not c.front_position is null
)

select
    device,
    front_position,
    sum(article_views)/sum(home_views) as ctr
from bins
where front_position <= 30
group by 1,2
order by 1,2
