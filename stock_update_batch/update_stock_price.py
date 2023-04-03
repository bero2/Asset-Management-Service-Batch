import argparse
import asyncio
import configparser
import datetime
import re
from typing import *

import FinanceDataReader as fdr
import aiomysql
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from pykrx import stock

config = configparser.ConfigParser()
config.read(['./db_conf.ini', '/var/jenkins_home/python-script/db_conf.ini'])
db_config: MutableMapping[str, str] = dict(config['mysql'])


async def insert_stock_data(pool: aiomysql.Pool, dataframe: pd.DataFrame) -> None:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                "REPLACE INTO stock_info (market_code, company_name, open_price, high_price, low_price, close_price, volume, market_date) VALUES (%s, %s, %s, %s,  %s, %s, %s, %s)",
                dataframe[['Code', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume', 'Date']].values.tolist()
            )
            await conn.commit()


def fetch_all_stock_data_by_fdr() -> None:
    loop = asyncio.get_event_loop()

    async def _fetch_all_stock_data(_loop: asyncio.AbstractEventLoop) -> None:
        pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

        stock_list = fdr.StockListing('KRX')[['Code', 'Name']].values.tolist()

        tasks = []
        for market_code, company_name in stock_list:
            df_stock_price_data = fdr.DataReader(market_code)
            df_stock_price_data.reset_index(inplace=True)
            df_stock_price_data = df_stock_price_data.replace({np.nan: None})
            df_stock_price_data = df_stock_price_data.astype(str)

            df_stock_price_data['Code'] = market_code
            df_stock_price_data['Name'] = company_name

            task = asyncio.ensure_future(insert_stock_data(pool, df_stock_price_data))
            tasks.append(task)
        await asyncio.gather(*tasks)
        pool.close()

    loop.run_until_complete(_fetch_all_stock_data(loop))
    loop.close()


def fetch_stock_data_by_pykrx(start_date: Optional[str], end_date: Optional[str]) -> None:
    assert (start_date is not None and end_date is not None) or (start_date is None and end_date is None)

    if start_date is None:
        start_date = str(datetime.date.today() + relativedelta(days=-3))
    if end_date is None:
        end_date = str(datetime.date.today())

    loop = asyncio.get_event_loop()

    async def _fetch_stock_data_by_pykrx(_loop: asyncio.AbstractEventLoop, start_date: str, end_date: str) -> None:
        start_date = re.sub('[^0-9]', '', start_date)
        end_date = re.sub('[^0-9]', '', end_date)
        pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)

        ticker_list = stock.get_market_ticker_list(market='ALL')
        stock_list = []
        for ticker in ticker_list:
            stock_list.append([ticker, stock.get_market_ticker_name(ticker)])

        tasks = []
        for date in stock.get_previous_business_days(fromdate=start_date, todate=end_date):
            df_stock_price_data = stock.get_market_ohlcv_by_ticker(date=str(date.date()), market="ALL")
            df_stock_price_data.reset_index(inplace=True)
            df_stock_price_data = df_stock_price_data.rename(
                columns={
                    '티커': 'Code',
                    '시가': 'Open',
                    '고가': 'High',
                    '저가': 'Low',
                    '종가': 'Close',
                    '거래량': 'Volume',
                }
            )
            df_stock_price_data['Date'] = date.date()
            df_stock_price_data = df_stock_price_data.replace({np.nan: None})
            df_stock_price_data = df_stock_price_data.astype(str)

            df_stock_list = pd.DataFrame(data=stock_list, columns=['Code', 'Name'])
            df_stock_price_data = pd.merge(df_stock_price_data, df_stock_list, how='inner', on='Code')

            task = asyncio.ensure_future(insert_stock_data(pool, df_stock_price_data))
            tasks.append(task)
        await asyncio.gather(*tasks)
        pool.close()

    loop.run_until_complete(_fetch_stock_data_by_pykrx(loop, start_date, end_date))
    loop.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, default=None)
    parser.add_argument('--end_date', type=str, default=None)
    args = parser.parse_args()

    fetch_stock_data_by_pykrx(start_date=args.start_date, end_date=args.end_date)
