import argparse
import asyncio
import configparser
import datetime
from typing import *

import aiomysql
import pandas as pd

config = configparser.ConfigParser()
config.read(['./db_conf.ini', '/var/jenkins_home/python-script/db_conf.ini'])
db_config: MutableMapping[str, str] = dict(config['mysql'])


async def update_stock_to_buy(loop: asyncio.AbstractEventLoop, target_date: str, max_hold_period: int, target_profit_rate: float) -> None:
    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                replace into recommend_stock_info (market_code, company_name, current_price, find_date, first_find_date, buy_yn, buy_try_yn, additional_buy_yn, sell_yn, bucket, max_hold_period, target_profit_rate)
                with filter1 as (
                    select
                        market_code
                        , market_date
                    from stock_info_a_year
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
                        from stock_info_a_year as a
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
                        from stock_info_a_year as a
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
                select distinct
                    a.market_code
                    , a.company_name 
                    , a.close_price as current_price
                    , '{target_date}' as find_date 
                    , '{target_date}' as first_find_date
                    , 'N' as buy_yn
                    , 'N' as buy_try_yn
                    , 'N' as additional_buy_yn
                    , 'N' as sell_yn
                    , 1 as bucket
                    , {max_hold_period} as max_hold_period
                    , {target_profit_rate} as target_profit_rate
                from stock_info_a_year as a
                join filter3 as b on a.market_code = b.market_code and a.market_date = b.market_date
            """)
        await conn.commit()
    pool.close()


async def load_stock_to_additional_buy(loop: asyncio.AbstractEventLoop, target_date: str) -> pd.DataFrame:
    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                with base as (
                    select
                        a.market_code
                        , a.first_find_date
                        , b.last_average_price as average_price
                        , max_hold_period
                        , target_profit_rate
                        , parent_slack_channel_id
                        , parent_slack_channel_name
                        , parent_slack_thread_ts
                        , bucket
                    from recommend_stock_info as a
                    join (
                        select distinct 
                            market_code
                            , first_find_date
                            , first_value (find_date) over(partition by market_code, first_find_date order by find_date desc) as last_find_date
                            , first_value (average_price) over(partition by market_code, first_find_date order by find_date desc) as last_average_price
                        from recommend_stock_info
                        where
                            sell_date is null
                            and buy_yn = 'Y'
                    ) as b on a.market_code = b.market_code and a.find_date = b.last_find_date and a.first_find_date = b.first_find_date
                    where
                        bucket < 4
                        and buy_try_yn = 'Y'
                )
                select
                    a.market_code
                    , a.company_name
                    , a.market_date as find_date
                    , close_price as current_price
                    , max_hold_period
                    , target_profit_rate
                    , first_find_date
                    , average_price
                    , parent_slack_channel_id
                    , parent_slack_channel_name
                    , parent_slack_thread_ts
                    , bucket
                from stock_info_a_year as a
                join base as b on a.market_code = b.market_code
                where
                    a.market_date = '{target_date}'
                    and a.close_price <= (b.average_price * 0.8)
            """)
        result = await cur.fetchall()
        columns = [x[0] for x in cur.description]

    pool.close()

    df = pd.DataFrame(
        data=result,
        columns=columns
    )
    return df


async def update_stock_to_additional_buy(loop: asyncio.AbstractEventLoop, dataframe: pd.DataFrame) -> None:
    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany("""
                insert ignore into recommend_stock_info (
                    market_code, company_name, current_price, average_price,
                    max_hold_period, target_profit_rate, find_date, 
                    first_find_date, buy_yn, buy_try_yn,
                    bucket, additional_buy_yn, sell_yn,
                    parent_slack_channel_id, parent_slack_channel_name, parent_slack_thread_ts
                ) 
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, 
                    %s, %s, %s,
                    %s, %s, %s
                )
                """,
                dataframe[[
                    'market_code', 'company_name', 'current_price', 'average_price',
                    'max_hold_period', 'target_profit_rate', 'find_date',
                    'first_find_date', 'buy_yn', 'buy_try_yn',
                    'bucket', 'additional_buy_yn', 'sell_yn',
                    'parent_slack_channel_id', 'parent_slack_channel_name', 'parent_slack_thread_ts'
                ]].values.tolist()
                )
            await conn.commit()
    pool.close()


async def update_stock_to_expected_buy(loop: asyncio.AbstractEventLoop, target_date: str, max_hold_period: int, target_profit_rate: float) -> None:
    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                with base as (
                    select
                        market_code
                        , find_date
                        , first_find_date
                        , average_price
                        , additional_buy_yn
                        , bucket
                    from recommend_stock_info
                    where
                        expected_buy_date is null
                        and buy_try_yn = 'N'
                        and find_date < '{target_date}'
                )
                update recommend_stock_info as a
                join (
                    select
                        a.market_code
                        , b.find_date
                        , b.first_find_date
                        , case
                            when b.additional_buy_yn = 'N' and (a.open_price * 0.98) >= a.low_price then '{target_date}'
                            when b.additional_buy_yn = 'Y' then '{target_date}'
                            end as expected_buy_date                        
                        , case
                            when b.additional_buy_yn = 'N' and (a.open_price * 0.98) >= a.low_price then (a.open_price * 0.98)
                            when b.additional_buy_yn = 'Y' then a.open_price
                            end as expected_buy_price
                        , case
                            when b.additional_buy_yn = 'N' and (a.open_price * 0.98) >= a.low_price then (a.open_price * 0.98)
                            when b.additional_buy_yn = 'Y' then round((a.open_price + b.average_price) / 2)
                            end as average_price
                        , case
                            when b.additional_buy_yn = 'N' and (a.open_price * 0.98) >= a.low_price then 'Y'
                            when b.additional_buy_yn = 'Y' then 'Y'
                            else 'N'
                            end as buy_yn
                        , {max_hold_period} as max_hold_period
                        , {target_profit_rate} as target_profit_rate 
                    from stock_info_a_year as a
                    join base as b on a.market_code = b.market_code
                    where
                        a.market_date = '{target_date}'
                ) as b on a.market_code = b.market_code and b.find_date = b.find_date and a.first_find_date = b.first_find_date
                set a.expected_buy_date = b.expected_buy_date, a.expected_buy_price = b.expected_buy_price, 
                    a.average_price = b.average_price, a.buy_yn= b.buy_yn, a.buy_try_yn = 'Y',
                    a.max_hold_period = b.max_hold_period, a.target_profit_rate = b.target_profit_rate
                where
                    a.find_date < '{target_date}'
                    and buy_try_yn = 'N'
            """)
            await conn.commit()
    pool.close()


async def update_stock_to_sell(loop: asyncio.AbstractEventLoop, target_date) -> None:
    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                with base as (
                    select
                        a.market_code
                        , average_price
                        , max_hold_period
                        , target_profit_rate
                        , find_date
                        , a.first_find_date
                    from recommend_stock_info as a
                    join (
                        select
                            market_code
                            , first_find_date
                            , max(find_date) last_find_date
                        from recommend_stock_info
                        group by 1, 2
                    ) as b on a.market_code = b.market_code and a.find_date = b.last_find_date and a.first_find_date = b.first_find_date
                    where
                        sell_yn = 'N'
                )
                update recommend_stock_info as a
                join (
                    select
                        a.market_code
                        , b.find_date
                        , b.first_find_date
                        , case
                             when (b.average_price * (1 + b.target_profit_rate))<= a.high_price then round(b.target_profit_rate, 2)
                            when datediff(a.market_date, b.find_date) > b.max_hold_period then round((b.average_price - a.close_price) / a.close_price, 2)
                            end as profit_rate
                        , case
                            when (b.average_price * (1 + b.target_profit_rate))<= a.high_price then round(b.average_price * (1 + b.target_profit_rate))
                            when datediff(a.market_date, b.find_date) > b.max_hold_period then close_price
                            end as sell_price
                        , case
                            when (b.average_price * (1 + b.target_profit_rate)) <= a.high_price then 'NORMAL'
                            when datediff(a.market_date, b.find_date) > b.max_hold_period then 'EXCEEDED_PERIOD'
                            end as sell_type
                        , case
                            when (b.average_price * (1 + b.target_profit_rate)) <= a.high_price then 'Y'
                            when datediff(a.market_date, b.find_date) > b.max_hold_period then 'Y'
                            else 'N'
                            end as sell_yn
                        , case
                            when (b.average_price * (1 + b.target_profit_rate)) <= a.high_price then '{target_date}'
                            when datediff(a.market_date, b.find_date) > b.max_hold_period then '{target_date}'
                            end as sell_date
                    from stock_info_a_year as a
                    join base as b on a.market_code = b.market_code
                    where
                        a.market_date = '{target_date}'
                ) as b on a.market_code = b.market_code and a.find_date = b.find_date and a.first_find_date = b.first_find_date
                set a.profit_rate = b.profit_rate, a.sell_price = b.sell_price, a.sell_type = b.sell_type, a.sell_yn = b.sell_yn, a.sell_date = b.sell_date 
                where
                    b.sell_yn = 'Y';
                    
                update recommend_stock_info as a
                join (
                    select
                        market_code
                        , sell_yn
                        , sell_date
                        , first_find_date
                    from recommend_stock_info
                    where
                        sell_yn = 'Y'
                ) as b on a.market_code = b.market_code = b.first_find_date = b.first_find_date
                set a.sell_yn = b.sell_yn, a.sell_date = b.sell_date
                where
                    a.sell_yn = 'N';
            """)
            await conn.commit()
    pool.close()


class UpdateRecommendStock:
    def __init__(self, target_date: Optional[str], max_hold_period: Optional[int], target_profit_rate: Optional[float]):
        self.target_date = datetime.datetime.today().date() - datetime.timedelta(days=1) if target_date is None else target_date
        self.max_hold_period = 180 if max_hold_period is None else max_hold_period
        self.target_profit_rate = 0.15 if target_profit_rate is None else target_profit_rate

        self._update_buy()
        self._update_additional_buy()
        self._update_expected_buy_stock_info()
        self._update_sell()

    def _update_buy(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(update_stock_to_buy(loop, self.target_date, self.max_hold_period, self.target_profit_rate))

    def _update_additional_buy(self):
        loop = asyncio.get_event_loop()
        df = loop.run_until_complete(load_stock_to_additional_buy(loop, self.target_date))

        result = []
        for idx in df.index:
            result.append({
                'market_code': df['market_code'][idx],
                'company_name': df['company_name'][idx],
                'current_price': df['current_price'][idx],
                'average_price': df['average_price'][idx],
                'max_hold_period': df['max_hold_period'][idx],
                'target_profit_rate': df['target_profit_rate'][idx],
                'find_date': self.target_date,
                'first_find_date': df['first_find_date'][idx],
                'buy_yn': 'Y',
                'buy_try_yn': 'N',
                'sell_yn': 'N',
                'bucket': df['bucket'][idx] + 1,
                'additional_buy_yn': 'Y',
                'parent_slack_channel_id': df['parent_slack_channel_id'][idx],
                'parent_slack_channel_name': df['parent_slack_channel_name'][idx],
                'parent_slack_thread_ts': df['parent_slack_thread_ts'][idx]
            })
        df_result = pd.DataFrame(result)

        if not df_result.empty:
            loop.run_until_complete(update_stock_to_additional_buy(loop, df_result))

    def _update_expected_buy_stock_info(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(update_stock_to_expected_buy(loop, self.target_date, self.max_hold_period, self.target_profit_rate))

    def _update_sell(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(update_stock_to_sell(loop, self.target_date))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--target_date', type=str, default=None)
    parser.add_argument('--max_hold_period', type=str, default=None)
    parser.add_argument('--target_profit_rate', type=str, default=None)
    args = parser.parse_args()

    UpdateRecommendStock(args.target_date, args.max_hold_period, args.target_profit_rate)

