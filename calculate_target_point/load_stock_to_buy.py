import asyncio
import configparser
from typing import *

import aiomysql
import pandas as pd
import re
from pykrx import stock
from calculate_target_point.target_filter import TargetFilter


config = configparser.ConfigParser()
config.read(['../db_conf.ini',  '/var/jenkins_home/python-script/db_conf.ini'])
db_config: MutableMapping[str, str] = dict(config['mysql'])


async def load_stock_to_buy(loop: asyncio.AbstractEventLoop, target_date: str) -> pd.DataFrame:
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
                        and (close_price between bollinger_band_low * 0.98 and bollinger_band_low * 1.10)
                        and (close_price between ema_112 and ema_112 * 1.03)
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
                                , count(case when close_price > ema_112 then 1 end) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 20 preceding and 0 preceding) AS e1
                                , count(case when close_price > bollinger_band_high then 1 end) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 20 preceding and 0 preceding) AS b1
                                , count(case when high_price > bollinger_band_high then 1 end) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 60 preceding and 0 preceding) AS b2
                            from stock_info_a_year as a
                            join filter1 as b on a.market_code = b.market_code and a.market_date <= b.market_date and a.market_date >= date_sub(b.market_date, interval 90 day)
                        ) as a
                        where 1=1
                            and e1
                            and b1 >= 1
                            and b2 >= 3
                    ) as a
                    where
                        market_date = '{target_date}'
                        and volume_avg >= 50000
                )
                , filter3 as (
                    select
                        market_code
                        , sum(case when is_accumulation_candle = 1 and is_accumulation_volume = 1 then 1 else 0 end) as accumulation_candle_cnt
                    from (
                        select
                            a.*
                            , case
                                when a.low_price = 0 then 0
                                when round((a.high_price - a.low_price) / a.low_price * 100) >= 15 then 1 else 0 end is_accumulation_candle
                            , case when a.volume >= avg(volume) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 5 preceding and 1 preceding) * 5 then 1 else 0 end as is_accumulation_volume
                        from stock_info_a_year as a
                        where
                            a.market_code in (select distinct market_code from filter2)
                            and a.market_date >= '{target_date}' - interval 120 day
                    ) as a
                    group by 1
                    having accumulation_candle_cnt >= 2
                )
                select distinct market_code from filter3;
            """)
        result = await cur.fetchall()
        columns = [x[0] for x in cur.description]

    pool.close()

    df = pd.DataFrame(
        data=result,
        columns=columns
    )
    return df
