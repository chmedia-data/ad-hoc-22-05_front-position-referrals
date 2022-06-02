with referrals as (
  select
    a.page_id as article_id,
    a.time_15min,
    a.device,
    sum(a.nviews) as front_referrals,
    sum(b.nviews) as home_views
  from chmedia.tmp_aaz_15min_front a
  left join chmedia.tmp_aaz_15min_front b
    on a.time_15min = b.time_15min
    and a.device = b.device
  where not a.page_id = "home"
    and b.page_id = "home"
    and a.device in ("mobile")
  group by 1,2,3
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
)

select
  a.article_id,
  date(a.time_15min) as day_dt,
  extract(hour from a.time_15min) as hour,
  sum(a.front_referrals) as front_referrals,
  sum(a.home_views) as home_views,
  sum(a.front_referrals)/sum(a.home_views) as referral_rate,
  avg(b.front_position) as avg_position
from referrals a
inner join front_placements b
  on a.article_id = b.article_id
  and a.time_15min = b.time_15min
  and extract(hour from a.time_15min) = 8
group by 1,2,3
