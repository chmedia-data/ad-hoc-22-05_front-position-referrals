with home_views as (
    select time_15min, nviews as home_views
    from `trim-mechanism-126723.chmedia.tmp_aaz_15min_front`
    where page_id = "home"
),

bins as (
    select
        a.time_15min,
        nviews as article_views,
        container_rank,
        home_views
    from `trim-mechanism-126723.chmedia.tmp_aaz_15min_front` a
    left join home_views b
        on a.time_15min = b.time_15min
    inner join `trim-mechanism-126723.chmedia.tmp_aaz_15min_front_placements` c
        on a.time_15min = c.time_15min
        and a.page_id = c.article_id
    where not page_id = "home"
        and not c.container_rank is null
)

select
    container_rank,
    sum(article_views)/sum(home_views) as ctr
from bins
group by 1
order by 1
