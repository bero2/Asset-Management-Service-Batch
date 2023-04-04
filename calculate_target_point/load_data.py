import asyncio
import configparser
from typing import *

import aiomysql
import pandas as pd

config = configparser.ConfigParser()
config.read(['../db_conf.ini',  '/var/jenkins_home/python-script/db_conf.ini'])
db_config: MutableMapping[str, str] = dict(config['mysql'])


async def load_target_company(loop: asyncio.AbstractEventLoop, target_date: str) -> pd.DataFrame:
    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                with filter1 as (
                    select
                        market_code
                        , market_date
                    from stock_info_a_year
                    where
                        (ema_60 < ema_112 and ema_112 < ema_224)
                        and (close_price >= leading_span1 or close_price >= leading_span2)
                        and (close_price between bollinger_band_high * 0.98 and bollinger_band_high * 1.02)
                        and close_price >= bollinger_band_high
                        and (close_price between ema_112 * 0.98 and ema_112 * 1.02 or close_price between ema_224 * 0.98 and ema_224 * 1.02)
                        and market_date = '{target_date}'
                )
                , filter2 as (
                    select
                        market_code
                        , volume_avg
                        , market_date
                    from (
                        select *
                        from (
                            select
                                a.*
                                , avg(volume) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 5 preceding and 1 preceding) AS volume_avg
                                , count(case when close_price > bollinger_band_high then 1 end) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 5 preceding and 0 preceding) AS b1
                                , count(case when close_price >= bollinger_band_high then 1 end) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 20 preceding and 0 preceding) AS b2
                                , count(case when high_price >= bollinger_band_high then 1 end) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 30 preceding and 0 preceding) AS b3
                            from stock_info_a_year as a
                            join filter1 as b on a.market_code = b.market_code and a.market_date <= b.market_date and a.market_date >= date_sub(b.market_date, interval 60 day)
                        ) as a
                        where
                            b1 >= 1 and b2 >= 1 and b3 >= 2
                    ) as a
                    where
                        market_date = '{target_date}'
                        and volume_avg >= 50000
                )
                , filter3 as (
                    select
                        market_code
                        , sum(is_accumulation_candle) as accumulation_candle_cnt
                    from (
                        select a.*
                             , case
                                   when a.volume >=
                                        avg(volume) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 5 preceding and 1 preceding)
                                       and round((a.high_price - a.low_price) / a.low_price * 100) >= 10
                                       then 1
                                   else 0 end as is_accumulation_candle
                        from stock_info_a_year as a
                        where
                            a.market_code in (select distinct market_code from filter2)
                            and a.market_date >= '{target_date}' - interval 120 day
                    ) as a
                    group by 1
                    having accumulation_candle_cnt >= 2
                )
                select distinct
                    a.*
                from stock_info_a_year as a
                join filter3 as b on a.market_code = b.market_code
                where
                    a.market_date between '{target_date}' - interval 60 day and '{target_date}' 
                order by market_code, market_date desc
            """)
        result = await cur.fetchall()
        columns = [x[0] for x in cur.description]

    pool.close()

    df = pd.DataFrame(
        data=result,
        columns=columns
    )
    return df

