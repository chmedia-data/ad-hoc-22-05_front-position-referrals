create table chmedia.tmp_aaz_15min_front as (

    with enriched as (
        select
            date(time,"Europe/Berlin") as day_dt,
            data.digitalData.page.category.primaryCategory = "Startseite" as is_home,
            data.digitalData.component.list[offset(0)].element.category.componentType = 'article' as is_article,
            data.digitalData.component.list[offset(0)].element.componentInfo.componentId as article_id,
            publish_path like "%news" as is_newsapp,
            data.ingress.referrer is null as missing_referrer,
            split(data.ingress.referrer,"?")[offset(0)] = "https://www.aargauerzeitung.ch/" as has_home_referrer,
            datetime_add(
                datetime_trunc(datetime(time,"Europe/Berlin"), hour),
                interval cast(floor(extract(minute from time)/15)*15 as int) minute
            ) as time_15min
        from `trim-mechanism-126723.chmedia.logs`
        where ( length(publish_path) = 8 or publish_path like "%news")
            and date(time,"Europe/Berlin") > "2021-05-01"
            and right(left(publish_path,8),3) = "aaz"
    )

    select
        day_dt,
        time_15min,
        if(is_home,"home",article_id) as page_id,
        count(*) as nviews
    from enriched
    where ( is_newsapp and
            ( is_home
            or
            (is_article and missing_referrer)
            )
        )
        or
        ( not is_newsapp and
            ( is_home
            or
            ( is_article and has_home_referrer)
            )
        )
    group by 1,2,3
    order by 4 desc
)
