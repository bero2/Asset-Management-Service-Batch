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


async def load_stock_to_buy(loop: asyncio.AbstractEventLoop, db_name: str, target_date: str) -> pd.DataFrame:
    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                with filter1 as (
                    select
                        market_code
                        , market_date
                    from {db_name}
                    where
                        (ema_60 < ema_112 and ema_112 < ema_224)
                        and (close_price >= leading_span1 or close_price >= leading_span2)
                        and (close_price between bollinger_band_low * 0.97 and bollinger_band_low * 1.05)
                        and (close_price < (bollinger_band_high + bollinger_band_low)/2)
                        and market_date = '{target_date}'
                )
                , filter2 as (
                    select
                        market_code
                        , market_date
                    from (
                        select
                            a.*
                            , avg(volume) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 5 preceding and 1 preceding) AS volume_avg
                            , count(case when high_price > bollinger_band_high then 1 end) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 60 preceding and 0 preceding) AS bollinger_high_break
                        from {db_name} as a
                        join filter1 as b on a.market_code = b.market_code and a.market_date <= b.market_date and a.market_date >= date_sub(b.market_date, interval 90 day)
                    ) as a
                    where
                        bollinger_high_break >= 2
                        and market_date = '{target_date}'
                        and volume_avg >= 50000
                )
                , accumulation_info as (
                    select market_code, market_date
                    from (
                        select
                            a.market_code
                            , market_date
                            , case
                                when a.low_price = 0 then 0
                                when round((a.high_price - a.low_price) / a.low_price * 100) >= 15 then 1 else 0 end is_accumulation_candle
                            , case when a.volume >= avg(volume) OVER (ORDER BY a.company_name, a.market_date ROWS BETWEEN 5 preceding and 1 preceding) * 5 then 1 else 0 end as is_accumulation_volume
                        from {db_name} as a
                        join (select distinct market_code from filter2) as b on a.market_code = b.market_code
                        where
                            a.market_date between '{target_date}' - interval 120 day and '{target_date}' 
                    ) as a
                    where
                        is_accumulation_candle = 1
                        and is_accumulation_volume = 1
                )
                , filter3 as (
                    select
                        a.market_code
                        , a.market_date
                        , count(*) as accumulation_candle_cnt
                    from filter2 as a
                    join accumulation_info as b on a.market_code = b.market_code
                    group by 1,2
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


async def load_stock_to_buy_for_simulation(loop: asyncio.AbstractEventLoop, db_name: str, target_date: str) -> pd.DataFrame:
    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                    select distinct market_code 
                    from {db_name}
                    where
                        market_date = '{target_date}'
                        and accumulation_candle_cnt >= 1
            """)
        result = await cur.fetchall()
        columns = [x[0] for x in cur.description]

    pool.close()

    df = pd.DataFrame(
        data=result,
        columns=columns
    )
    return df
