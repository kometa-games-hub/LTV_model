ltv_query = '''
select day+1 as day, players, round(revenue) as revenue
from (
         select 1 as anchor, uniqExact(user_id) as players
         from production.User
         where date between {date_string}
         and user_id not in (select user_id from production.Cheaters)
         and os = {platform}
         and utm_source = {utm_source}
         and country in ('United States', 'France', 'Germany', 'United Kindgom', 'Canada', 'Australia', 'Italy', 'Australia', 'Netherlands',
        'Norway', 'Belgium', 'Puerto Rico', 'Slovenia', 'Sweden', 'Spain', 'Denmark', 'Turkey', 'France', 'Romania')
         ) as players
all right join
(
select 1 as anchor, day, if(revenue is null, 0, revenue) as revenue
from (
         select day
         from (
                  select range(cast(day, 'UInt32')) as day
                  from (
                        select today() -
                               (select min(date)
                                from production.Event
                                where date between {date_string}
                               ) as day
                           )
                  )
                  array join day
         ) days
all left join(
    select toUInt32(day) as day, sum(revenue) as revenue
    from (
          select user_id,
                 U.date             as register_date,
                 P.date             as pay_date,
                 P.date - U.date    as day,
                 gross as revenue
          from production.User U
                   all
                   inner join production.Payments P using (user_id)
          where register_date between {date_string}
          and os = {platform}
          --and utm_source = {utm_source}
          --and country in ('United States', 'France', 'Germany', 'United Kindgom', 'Canada', 'Australia', 'Italy', 'Australia', 'Netherlands',
           --'Norway', 'Belgium', 'Puerto Rico', 'Slovenia', 'Sweden', 'Spain', 'Denmark', 'Turkey', 'France', 'Romania')
          
          
            and user_id not in (select user_id from production.Cheaters)
             and (purchase_id not in (select purchase_id from production.VoidedPurchases group by purchase_id )
          or purchase_id is null)
          order by user_id, register_date, pay_date
             )
    group by day
    order by day
    ) pay_days using day
) pay_day using (anchor)
order by day
'''

ltv_payers_query = '''
select day+1 as day, players, round(revenue) as revenue
from(
    select day, players, revenue
    from (
             select toUInt32(day_first) as day, uniqExact(user_id) as players
             from (
                      select user_id,
                             U.date                                          as register_date,
                             P.date                                          as pay_date,
                             P.date - U.date                                 as day,
                             if(payments_order = 1, P.date, U.date) - U.date as day_first
                      from production.User U
                               all
                               inner join production.Payments P using (user_id)
                      where register_date between {date_string}
                      and os = {platform}
         and utm_source = {utm_source}
         and country in ('United States', 'France', 'Germany', 'United Kindgom', 'Canada', 'Australia', 'Italy', 'Australia', 'Netherlands',
        'Norway', 'Belgium', 'Puerto Rico', 'Slovenia', 'Sweden', 'Spain', 'Denmark', 'Turkey', 'France', 'Romania')
                      
                      
                        and user_id not in (select user_id from production.Cheaters)
                         and (purchase_id not in (select purchase_id from production.VoidedPurchases group by purchase_id)
                        or purchase_id is null)
                      group by user_id, register_date, pay_date, day, day_first
                      having day = day_first
                      order by user_id, register_date, pay_date
                      ) as players
             group by day
             order by day
             ) players_converted
    all right join
    (
    select day, if(revenue is null, 0, revenue) as revenue
    from (
             select day
             from (
                      select range(cast(day, 'UInt32')) as day
                      from (
                            select today() -
                                   (select min(date)
                                    from production.Event
                                    where date between {date_string}
                                   ) as day
                               )
                      )
                      array join day
             ) days
    all left join(
        select toUInt32(day) as day, sum(revenue) as revenue
        from (
              select user_id,
                     U.date             as register_date,
                     P.date             as pay_date,
                     P.date - U.date    as day,
                     if(payments_order=1, P.date, U.date) - U.date    as day_first,
                     gross as revenue
              from production.User U
                       all
                       inner join production.Payments P using (user_id)
              where register_date between {date_string}
              and os = {platform}
         and utm_source = {utm_source}
         and country in ('United States', 'France', 'Germany', 'United Kindgom', 'Canada', 'Australia', 'Italy', 'Australia', 'Netherlands',
        'Norway', 'Belgium', 'Puerto Rico', 'Slovenia', 'Sweden', 'Spain', 'Denmark', 'Turkey', 'France', 'Romania')
              
              and user_id not in (select user_id from production.Cheaters)
               and (purchase_id not in (select purchase_id from production.VoidedPurchases group by purchase_id )
                or purchase_id is null)
              order by user_id, register_date, pay_date
                 )
        group by day
        order by day
        ) pay_days using day
    ) pay_day using (day)
    order by day
)
'''