with referral_rates as (
  select
    a.page_id as article_id,
    a.time_15min,
    sum(a.nviews) as front_referrals,
    sum(b.nviews) as home_views,
    sum(a.nviews)/sum(b.nviews) as referral_rate
  from chmedia.tmp_aaz_15min_front a
  left join chmedia.tmp_aaz_15min_front b
    on a.time_15min = b.time_15min
    and a.device = b.device
  where not a.page_id = "home"
    and b.page_id = "home"
    and date(a.time_15min) > date_add("2022-04-12",interval -29 day)
    and date(a.time_15min) <= "2022-04-12"
    and a.device = "mobile"
  group by 1,2
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

position_referral_rates as (
  select
    a.*,
    b.front_position
  from referral_rates a
  left join front_placements b
    on a.article_id = b.article_id
    and a.time_15min = b.time_15min
),

historical_referral_rates as (
  select front_position, sum(front_referrals)/sum(home_views) as historical_referral_rate
  from position_referral_rates
  where date(time_15min) < "2022-04-12"
  group by 1
)

select
    a.*,
    b.historical_referral_rate
from position_referral_rates a
left join historical_referral_rates b
  on a.front_position = b.front_position
where article_id = "ld.2276014"
  and date(a.time_15min) = "2022-04-12"
