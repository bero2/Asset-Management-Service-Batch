import asyncio
import configparser
import datetime
import re
from datetime import datetime
from typing import *

import aiomysql
import math
import pandas as pd
import time
from pykrx import stock

from calculate_target_point.load_stock_to_buy import load_stock_to_buy

config = configparser.ConfigParser()
config.read(['../db_conf.ini',  '/var/jenkins_home/python-script/db_conf.ini'])
db_config: MutableMapping[str, str] = dict(config['mysql'])


async def load_stocks_info(loop: asyncio.AbstractEventLoop, target_date: str, market_code_list: list) -> pd.DataFrame:
    assert len(market_code_list) > 0

    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)
    code_list = ', '.join(f"'{code}'" for code in market_code_list)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                select 
                    *
                from stock_info_a_year
                where
                    market_date = '{target_date}'
                    and market_code in ({code_list})
                    
            """)
        result = await cur.fetchall()
        columns = [x[0] for x in cur.description]

    pool.close()

    df = pd.DataFrame(
        data=result,
        columns=columns
    )
    return df


class Simulator:
    def __init__(
            self,
            start_date: str,
            end_date: str,
            seed: int,
            profit_rate: float,
            stop_loss_rate: Optional[int],
            loss_method: Optional[str],
            max_holding_period: int,
            buy_point_to_open_price: float,
            additional_buy_point_to_price: float
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.seed = seed
        self.account_balance = seed
        self.profit_rate = profit_rate
        self.stop_loss_rate = stop_loss_rate
        self.loss_method = loss_method
        self.max_holding_period = max_holding_period
        self.buy_point_to_open_price = buy_point_to_open_price
        self.additional_buy_point_to_price = additional_buy_point_to_price
        self.business_days = [str(date.date()) for date in (stock.get_previous_business_days(fromdate=re.sub('[^0-9]', '', self.start_date), todate=re.sub('[^0-9]', '', self.end_date)))]
        self.lv1_max_bucket_size = 9
        self.lv2_max_bucket_size = 8
        self.lv3_max_bucket_size = 4
        self.lv4_max_bucket_size = 2
        self.lv1_price_size = math.floor(self.seed * 0.03)
        self.lv2_price_size = self.lv1_price_size
        self.lv3_price_size = self.lv2_price_size * 2
        self.lv4_price_size = self.lv3_price_size * 2
        self.hold_stock_dic = dict()    # '000020' : {'매수일': '2023-01-01', '보유 기간': 1, '매수가':10000, '보유량': 20}
        self.profit = 0

    @staticmethod
    def _search_stock_to_buy(target_date: str) -> list:
        loop = asyncio.get_event_loop()
        df = loop.run_until_complete(load_stock_to_buy(loop, target_date))
        target_code_list = df['market_code'].values.tolist()

        return target_code_list

    @staticmethod
    def _load_stocks_info(target_date: str, market_code_list: List[str]) -> pd.DataFrame:
        loop = asyncio.get_event_loop()
        df = loop.run_until_complete(load_stocks_info(loop, target_date, market_code_list))
        return df

    @staticmethod
    def _get_date_diff(start_date: str, end_date: str) -> int:
        return (datetime.strptime(start_date, '%Y-%m-%d') - datetime.strptime(end_date, '%Y-%m-%d')).days

    def _get_hold_days_from_last_buy(self, market_code: str, target_date:str):
        last_buy_date = self.hold_stock_dic[market_code]['매수일'] if self.hold_stock_dic[market_code]['최종_매수일'] is None else self.hold_stock_dic[market_code]['최종_매수일']
        return self._get_date_diff(last_buy_date, target_date)

    def _get_remain_bucket_size(self, level: int) -> int:
        current_lv1_size = 0
        current_lv2_size = 0
        current_lv3_size = 0
        current_lv4_size = 0

        for value in self.hold_stock_dic.values():
            if value['level'] == 1:
                current_lv1_size += 1
            elif value['level'] == 2:
                current_lv2_size += 1
            elif value['level'] == 3:
                current_lv3_size += 1
            elif value['level'] == 4:
                current_lv4_size += 1

        if level == 1:
            return self.lv1_max_bucket_size - current_lv1_size
        elif level == 2:
            return self.lv2_max_bucket_size - current_lv2_size
        elif level == 3:
            return self.lv3_max_bucket_size - current_lv3_size
        elif level == 4:
            return self.lv4_max_bucket_size - current_lv4_size
        else:
            return 0

    def _get_price_size_for_bucket(self, level: int) -> int:
        if level == 1:
            return self.lv1_price_size
        elif level == 2:
            return self.lv2_price_size
        elif level == 3:
            return self.lv3_price_size
        elif level == 4:
            return self.lv4_price_size
        else:
            return 0

    def _check_is_hold_stock(self, market_code: str) -> bool:
        if self.hold_stock_dic.get(market_code) is None:
            return False
        else:
            return True

    def _hold_stock_before_date(self, target_date) -> List[str]:
        target_code_list = []
        for code in self.hold_stock_dic.keys():
            if self.hold_stock_dic[code]['매수일'] <= target_date:
                target_code_list.append(code)

        return target_code_list

    def _buy(self, current_date: str, next_date: str) -> None:
        if self._get_remain_bucket_size(level=1) > 0:
            target_code_list = self._search_stock_to_buy(current_date)

            if len(target_code_list) > 0:
                df_next_stocks_info = self._load_stocks_info(next_date, target_code_list)

                for idx in df_next_stocks_info.index:
                    market_code = df_next_stocks_info['market_code'][idx]
                    open_price = df_next_stocks_info['open_price'][idx]
                    low_price = df_next_stocks_info['low_price'][idx]
                    target_buy_price = math.floor(open_price * (1 + self.buy_point_to_open_price))

                    if target_buy_price >= low_price and self._get_remain_bucket_size(level=1) > 0 and self._check_is_hold_stock(market_code) is False:
                        self.hold_stock_dic.update({
                            market_code: {
                                '매수일': next_date,
                                '최종_매수일': None,
                                '보유기간': 1,
                                '평단가': target_buy_price,
                                '보유개수': self.lv1_price_size // target_buy_price,
                                '총금액': target_buy_price * (self.lv1_price_size // target_buy_price),
                                'level': 1
                            }
                        })

    def _additional_buy(self, current_date: str, next_date: str):
        hold_stock_code_list = self._hold_stock_before_date(current_date)
        target_code_list = []
        for code in hold_stock_code_list:
            if self.hold_stock_dic[code]['level'] <= 3:
                target_code_list.append(code)

        additional_buy_target_list = []
        if len(target_code_list) > 0:
            df_current_stock_info = self._load_stocks_info(current_date, target_code_list)
            for idx in df_current_stock_info.index:
                market_code = df_current_stock_info['market_code'][idx]
                close_price = df_current_stock_info['close_price'][idx]
                if close_price <= self.hold_stock_dic[market_code]['평단가'] * (1 + self.additional_buy_point_to_price):
                    additional_buy_target_list.append(market_code)

        if len(additional_buy_target_list) > 0:
            df_next_stocks_info = self._load_stocks_info(next_date, additional_buy_target_list)

            for idx in df_next_stocks_info.index:
                market_code = df_next_stocks_info['market_code'][idx]
                open_price = df_next_stocks_info['open_price'][idx]
                bucket_level = self.hold_stock_dic[market_code]['level']

                if self._get_remain_bucket_size(level=bucket_level+1) > 0:
                    self.hold_stock_dic[market_code]['최종_매수일'] = next_date
                    self.hold_stock_dic[market_code]['평단가'] = \
                        ((self.hold_stock_dic[market_code]['평단가'] * self.hold_stock_dic[market_code]['보유개수']) + ((self._get_price_size_for_bucket(level=bucket_level) // open_price) * open_price)) / \
                        (self.hold_stock_dic[market_code]['보유개수'] + self._get_price_size_for_bucket(level=bucket_level) // open_price)
                    self.hold_stock_dic[market_code]['보유개수'] += self._get_price_size_for_bucket(level=bucket_level) // open_price
                    self.hold_stock_dic[market_code]['총금액'] = self.hold_stock_dic[market_code]['평단가'] * self.hold_stock_dic[market_code]['보유개수']
                    self.hold_stock_dic[market_code]['level'] += 1

    def _sell(self, target_date):
        hold_stock_code_list = self._hold_stock_before_date(target_date)

        if len(hold_stock_code_list) > 0:
            df_current_stocks_info = self._load_stocks_info(target_date, hold_stock_code_list)
            for idx in df_current_stocks_info.index:
                market_code = df_current_stocks_info['market_code'][idx]
                high_price = df_current_stocks_info['high_price'][idx]
                close_price = df_current_stocks_info['close_price'][idx]
                target_sell_point = math.floor(self.hold_stock_dic[market_code]['평단가'] * (1 + self.profit_rate))

                if target_sell_point <= high_price:
                    profit = (target_sell_point * self.hold_stock_dic[market_code]['보유개수']) - (self.hold_stock_dic[market_code]['총금액'])
                    self.profit += profit
                    self.hold_stock_dic.pop(market_code)
                    continue

                if self._get_hold_days_from_last_buy(market_code, target_date) >= self.max_holding_period:
                    profit = (close_price - self.hold_stock_dic[market_code]['평단가']) * self.hold_stock_dic[market_code]['보유개수']
                    self.profit += profit
                    self.hold_stock_dic.pop(market_code)

    def _update_account_balance(self, target_date):
        hold_stock_code_list = self._hold_stock_before_date(target_date)

        close_price_sum = 0
        if len(hold_stock_code_list) > 0:
            df_current_stocks_info = self._load_stocks_info(target_date, hold_stock_code_list)

            for idx in df_current_stocks_info.index:
                market_code = df_current_stocks_info['market_code'][idx]
                close_price = df_current_stocks_info['close_price'][idx]
                print('>> 보유종목 :', market_code, '\t평단가 :', round(self.hold_stock_dic[market_code]['평단가']), '\t보유개수 :', self.hold_stock_dic[market_code]['보유개수'], '\t종가 :', close_price, '\t\tLV :', self.hold_stock_dic[market_code]['level'])
                close_price_sum += (close_price - self.hold_stock_dic[market_code]['평단가']) * self.hold_stock_dic[market_code]['보유개수']

        self.account_balance = self.seed + self.profit + close_price_sum

    def simulator(self):
        for current_date, next_date in zip(self.business_days, self.business_days[1:]):
            print(current_date)
            # 매수
            self._buy(current_date, next_date)

            # 추가 매수
            self._additional_buy(current_date, next_date)

            # 매도
            self._sell(current_date)

            # 계좌 잔액 업데이트
            self._update_account_balance(current_date)

            print('>> LV1 :', self._get_remain_bucket_size(level=1), '\tLV2 :', self._get_remain_bucket_size(level=2), '\tLV3 :', self._get_remain_bucket_size(level=3), '\tLV4 :', self._get_remain_bucket_size(level=4))
            print('>>> 평가 금액 :', self.account_balance, '\t수익률 :', round((self.account_balance - self.seed) / self.seed * 100, 3), '%')


if __name__ == "__main__":
    sim = Simulator(
        start_date='2022-05-01',
        end_date='2023-03-31',
        seed=100000000,
        profit_rate=0.10,
        stop_loss_rate=None,
        loss_method=None,
        max_holding_period=180,
        buy_point_to_open_price=-0.02,
        additional_buy_point_to_price=-0.20
    )
    sim.simulator()
