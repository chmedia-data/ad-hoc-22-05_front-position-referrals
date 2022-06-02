with referrals as (
  select
    a.page_id as article_id,
    a.time_15min,
    a.device,
    sum(a.nviews) as front_referrals,
    sum(b.nviews) as home_views,
    sum(a.nviews)/sum(b.nviews) as referral_rate
  from chmedia.tmp_aaz_15min_front a
  left join chmedia.tmp_aaz_15min_front b
    on a.time_15min = b.time_15min
    and a.device = b.device
  where a.page_id = "ld.2294255"
    and b.page_id = "home"
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
  a.*,
  b.front_position
from referrals a
left join front_placements b
  on a.article_id = b.article_id
  and a.time_15min = b.time_15min
order by a.time_15min
