import argparse
import asyncio
import configparser
import datetime
from typing import *

import aiomysql
import numpy as np
import pandas as pd
import ta.momentum as ta_momentum
import ta.trend as ta_trend
import ta.volatility as ta_volatility
from dateutil.relativedelta import relativedelta

config = configparser.ConfigParser()
config.read(['./db_conf.ini', '/var/jenkins_home/python-script/db_conf.ini'])
db_config: MutableMapping[str, str] = dict(config['mysql'])


async def load_all_market_code_data(pool: aiomysql.Pool) -> Optional[list]:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                    select distinct market_code
                    from stock_info
                """)
        _result = await cur.fetchall()
        if len(_result) == 0:
            return []
        else:
            return [x[0] for x in _result]


async def load_stock_data(pool: aiomysql.Pool, market_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                select 
                    market_code
                    , company_name
                    , open_price
                    , high_price
                    , low_price
                    , close_price
                    , volume
                    , market_date 
                from stock_info
                where 
                    market_code = '{market_code}'
                    and market_date >= '{start_date}' - interval 1 year
                    and market_date <= '{end_date}'
                order by market_date
            """)
        _result = await cur.fetchall()
        if len(_result) == 0:
            return None
        else:
            return pd.DataFrame(
                data=_result,
                columns=[
                    'code',
                    'name',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume',
                    'date'
                ]
            ).astype(str)


async def update_indicator_data(pool: aiomysql.Pool, dataframe: pd.DataFrame) -> None:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany("""
                REPLACE INTO stock_info (
                    market_code, company_name, open_price, high_price, low_price, close_price, volume, market_date,
                    ema_5, ema_10, ema_20, ema_60, ema_112, ema_224, 
                    bollinger_band_low, bollinger_band_high, bollinger_band_low_cross, bollinger_band_high_cross,
                    leading_span1, leading_span2, ichimoku_base_line,
                    stochastic_k, stochastic_slow_d, rsi
                ) 
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                """,
                dataframe[[
                    'code', 'name', 'open', 'high', 'low', 'close', 'volume', 'date',
                    'ema_5', 'ema_10', 'ema_20', 'ema_60', 'ema_112', 'ema_224',
                    'bollinger_band_low', 'bollinger_band_high', 'bollinger_band_low_cross', 'bollinger_band_high_cross',
                    'leading_span1', 'leading_span2', 'ichimoku_base_line',
                    'stochastic_k', 'stochastic_slow_d', 'rsi'
                ]].values.tolist()
            )
            await conn.commit()


def calculate_indicator(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.astype({'open': float, 'high': float, 'low': float, 'close': float})

    # 5, 10, 20, 60, 112, 224일 지수이동평균
    ema_5 = ta_trend.ema_indicator(close=dataframe['close'], window=5).rename('ema_5')
    ema_10 = ta_trend.ema_indicator(close=dataframe['close'], window=10).rename('ema_10')
    ema_20 = ta_trend.ema_indicator(close=dataframe['close'], window=20).rename('ema_20')
    ema_60 = ta_trend.ema_indicator(close=dataframe['close'], window=60).rename('ema_60')
    ema_112 = ta_trend.ema_indicator(close=dataframe['close'], window=112).rename('ema_112')
    ema_224 = ta_trend.ema_indicator(close=dataframe['close'], window=224).rename('ema_224')

    # 볼린저밴드 하단, 상단, 종가 하단 돌파, 종가 상단 돌파
    bollinger_band = ta_volatility.BollingerBands(close=dataframe['close'].astype(int), window=40, window_dev=2)
    bb_low = bollinger_band.bollinger_lband().rename('bollinger_band_low')
    bb_high = bollinger_band.bollinger_hband().rename('bollinger_band_high')
    bb_low_cross = bollinger_band.bollinger_lband_indicator().rename('bollinger_band_low_cross')
    bb_high_cross = bollinger_band.bollinger_hband_indicator().rename('bollinger_band_high_cross')

    # 일목균형표 선행스팬1, 선행스팬2, 기준선 (9, 26, 52)
    ichimoku = ta_trend.IchimokuIndicator(high=dataframe['high'], low=dataframe['low'])
    ichimoku_span_a = ichimoku.ichimoku_a().rename('leading_span1')
    ichimoku_span_b = ichimoku.ichimoku_b().rename('leading_span2')
    ichimoku_base_line = ichimoku.ichimoku_base_line().rename('ichimoku_base_line')

    # 일목균형표 선행스팬1, 선행스팬2, 기준선 (18, 52, 104)
    ichimoku = ta_trend.IchimokuIndicator(high=dataframe['high'], low=dataframe['low'], window1=18, window2=52, window3=104)
    ichimoku_span_a = ichimoku.ichimoku_a().rename('leading_span1')
    ichimoku_span_b = ichimoku.ichimoku_b().rename('leading_span2')
    ichimoku_base_line = ichimoku.ichimoku_base_line().rename('ichimoku_base_line')

    # (12, 5, 5) 스토캐스틱 슬로우
    stochastic = ta_momentum.StochasticOscillator(high=dataframe['high'], low=dataframe['low'], close=dataframe['close'], window=12, smooth_window=5)
    stochastic_k = stochastic.stoch_signal().rename('stochastic_k')
    stochastic_d = stochastic_k.rolling(window=5).mean()
    stochastic_slow_d = stochastic_d.rolling(window=5).mean().rename('stochastic_slow_d')

    # RSI
    rsi = ta_momentum.rsi(close=dataframe['close'], window=14).rename('rsi')

    dataframe = pd.concat([
        dataframe,
        ema_5,
        ema_10,
        ema_20,
        ema_60,
        ema_112,
        ema_224,
        bb_low,
        bb_high,
        bb_low_cross,
        bb_high_cross,
        ichimoku_span_a,
        ichimoku_span_b,
        ichimoku_base_line,
        stochastic_k,
        stochastic_slow_d,
        rsi
    ], axis=1).replace({np.nan: None})

    return dataframe


def insert_indicator_data(start_date: Optional[str], end_date: Optional[str]) -> None:
    assert (start_date is not None and end_date is not None) or (start_date is None and end_date is None)

    if start_date is None:
        start_date = str(datetime.date.today() + relativedelta(days=-3))
    if end_date is None:
        end_date = str(datetime.date.today())

    loop = asyncio.get_event_loop()

    async def _insert_indicator_data(_loop: asyncio.AbstractEventLoop, start_date: str, end_date: str) -> None:
        pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

        market_code_list = await load_all_market_code_data(pool)

        tasks = []
        for market_code in market_code_list:
            df = await load_stock_data(pool, market_code, start_date, end_date)
            if df is not None:
                df = calculate_indicator(df)
                task = asyncio.ensure_future(
                    update_indicator_data(pool, df[(df['date'] >= start_date) & (df['date'] <= end_date)])
                )
                tasks.append(task)

        await asyncio.gather(*tasks)
        pool.close()

    loop.run_until_complete(_insert_indicator_data(loop, start_date, end_date))
    loop.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, default=None)
    parser.add_argument('--end_date', type=str, default=None)
    args = parser.parse_args()

    insert_indicator_data(start_date=args.start_date, end_date=args.end_date)
