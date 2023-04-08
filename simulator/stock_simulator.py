import asyncio
import configparser
import datetime
import re
from datetime import datetime
from typing import *

import aiomysql
import math
import pandas as pd
from pykrx import stock
import time

from calculate_target_point.load_stock_to_buy import load_stock_to_buy_for_simulation

config = configparser.ConfigParser()
config.read(['../db_conf.ini', '/var/jenkins_home/python-script/db_conf.ini'])
db_config: MutableMapping[str, str] = dict(config['mysql'])


async def load_stocks_info_after_target_date(loop: asyncio.AbstractEventLoop, db_name: str, target_date: str, market_code_list: list) -> pd.DataFrame:
    assert len(market_code_list) > 0

    pool = await aiomysql.create_pool(host=db_config['host'], port=int(db_config['port']), user=db_config['user'], password=db_config['password'], db=db_config['db'], autocommit=True, loop=loop)
    code_list = ', '.join(f"'{code}'" for code in market_code_list)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                select 
                    *
                from {db_name}
                where
                    market_date >= '{target_date}'
                    and market_code in ({code_list})
                order by market_code, market_date
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
            additional_buy_point_to_price: float,
            price_db: str,
            target_db: str
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
        self.price_db = price_db
        self.target_db = target_db
        self.business_days = [str(date.date()) for date in (stock.get_previous_business_days(fromdate=re.sub('[^0-9]', '', self.start_date), todate=re.sub('[^0-9]', '', self.end_date)))]
        self._lv1_max_bucket_size = 9
        self._lv2_max_bucket_size = 8
        self._lv3_max_bucket_size = 4
        self._lv4_max_bucket_size = 2
        self._lv1_price_size = math.floor(self.seed * 0.03)  # (9,8,4,2 0.03)
        self._lv2_price_size = self._lv1_price_size
        self._lv3_price_size = self._lv2_price_size * 2
        self._lv4_price_size = self._lv3_price_size * 2
        self._hold_stock_dict = dict()
        self._hold_stock_price_dict = dict()
        self._profit = 0
        self.result: Dict = {}

        self._init_result_dataframe()
        self._simulator()

    @staticmethod
    def _get_date_diff(start_date: str, end_date: str) -> int:
        return (datetime.strptime(start_date, '%Y-%m-%d') - datetime.strptime(end_date, '%Y-%m-%d')).days

    @staticmethod
    def _list_to_str(list_data: list) -> str:
        return ', '.join(list_data)

    def _search_stock_to_buy(self, target_date: str) -> list:
        loop = asyncio.get_event_loop()
        df = loop.run_until_complete(load_stock_to_buy_for_simulation(loop, self.target_db, target_date))
        target_code_list = df['market_code'].values.tolist()

        return target_code_list

    def _load_stocks_info(self, target_date: str, market_code_list: List[str]) -> pd.DataFrame:
        loop = asyncio.get_event_loop()
        df = loop.run_until_complete(load_stocks_info_after_target_date(loop, self.price_db, target_date, market_code_list))
        return df

    def _init_result_dataframe(self):
        for business_day in self.business_days:
            self.result.update({
                business_day: {
                    '평가 금액': 0,
                    '매수 종목': [],
                    '추가 매수 종목': [],
                    '이익 매도 종목': [],
                    '기간 초과 매도 종목': [],
                    '매도 이익': 0,
                    '수익률': 0.0
                }
            })

    def _get_hold_days_from_last_buy(self, market_code: str, target_date: str):
        last_buy_date = self._hold_stock_dict[market_code]['매수일'] if self._hold_stock_dict[market_code]['최종_매수일'] is None else self._hold_stock_dict[market_code]['최종_매수일']
        return self._get_date_diff(last_buy_date, target_date)

    def _get_remain_bucket_size(self, level: int) -> int:
        current_lv1_size = 0
        current_lv2_size = 0
        current_lv3_size = 0
        current_lv4_size = 0

        for value in self._hold_stock_dict.values():
            if value['level'] == 1:
                current_lv1_size += 1
            elif value['level'] == 2:
                current_lv1_size += 1
                current_lv2_size += 1
            elif value['level'] == 3:
                current_lv1_size += 1
                current_lv2_size += 1
                current_lv3_size += 1
            elif value['level'] == 4:
                current_lv1_size += 1
                current_lv2_size += 1
                current_lv3_size += 1
                current_lv4_size += 1

        if level == 1:
            return self._lv1_max_bucket_size - current_lv1_size
        elif level == 2:
            return self._lv2_max_bucket_size - current_lv2_size
        elif level == 3:
            return self._lv3_max_bucket_size - current_lv3_size
        elif level == 4:
            return self._lv4_max_bucket_size - current_lv4_size
        else:
            return 0

    def _get_price_size_for_bucket(self, level: int) -> int:
        if level == 1:
            return self._lv1_price_size
        elif level == 2:
            return self._lv2_price_size
        elif level == 3:
            return self._lv3_price_size
        elif level == 4:
            return self._lv4_price_size
        else:
            return 0

    def _check_is_hold_stock(self, market_code: str) -> bool:
        if self._hold_stock_dict.get(market_code) is None:
            return False
        else:
            return True

    def _hold_stock_before_date(self, target_date) -> List[str]:
        target_code_list = []
        for code in self._hold_stock_dict.keys():
            if self._hold_stock_dict[code]['매수일'] <= target_date:
                target_code_list.append(code)

        return target_code_list

    def _buy(self, current_date: str, next_date: str) -> None:
        if self._get_remain_bucket_size(level=1) > 0:
            target_code_list = self._search_stock_to_buy(current_date)

            if len(target_code_list) > 0:
                df_stock_price_info = self._load_stocks_info(next_date, target_code_list).astype(str)

                for market_code in target_code_list:
                    filtered_df = df_stock_price_info[(df_stock_price_info['market_code'] == market_code) & (df_stock_price_info['market_date'] == next_date)]
                    if not filtered_df.empty:
                        open_price = filtered_df['open_price']
                    else:
                        continue

                    target_buy_price = math.floor(int(open_price) * (1 + self.buy_point_to_open_price))
                    low_price = int(filtered_df['low_price'])

                    if target_buy_price > 0 and target_buy_price >= low_price and self._get_remain_bucket_size(level=1) > 0 and self._check_is_hold_stock(market_code) is False:
                        self._hold_stock_dict.update({
                            market_code: {
                                '매수일': next_date,
                                '최종_매수일': None,
                                '보유기간': 1,
                                '평단가': target_buy_price,
                                '보유개수': self._lv1_price_size // target_buy_price,
                                '총금액': target_buy_price * (self._lv1_price_size // target_buy_price),
                                'level': 1
                            }
                        })
                        self._hold_stock_price_dict.update({
                            market_code: df_stock_price_info[df_stock_price_info['market_code'] == market_code]
                        })
                        self.result[next_date]['매수 종목'].append(market_code)

    def _additional_buy(self, current_date: str, next_date: str):
        hold_stock_code_list = self._hold_stock_before_date(current_date)
        target_code_list = []
        for code in hold_stock_code_list:
            if self._hold_stock_dict[code]['level'] <= 3:
                target_code_list.append(code)

        for market_code in target_code_list:
            df_hold_stock_price_info = self._hold_stock_price_dict[market_code]
            df_current_info = df_hold_stock_price_info[df_hold_stock_price_info['market_date'] == current_date]
            df_next_info = df_hold_stock_price_info[df_hold_stock_price_info['market_date'] == next_date]
            bucket_level = self._hold_stock_dict[market_code]['level']

            if df_current_info.empty or df_next_info.empty:
                continue

            if self._get_remain_bucket_size(level=bucket_level+1) > 0 and int(df_current_info['close_price']) <= self._hold_stock_dict[market_code]['평단가'] * (1 + self.additional_buy_point_to_price):
                next_open_price = int(df_next_info['open_price'])

                self._hold_stock_dict[market_code]['최종_매수일'] = next_date
                self._hold_stock_dict[market_code]['평단가'] = \
                    ((self._hold_stock_dict[market_code]['평단가'] * self._hold_stock_dict[market_code]['보유개수']) + ((self._get_price_size_for_bucket(level=bucket_level) // next_open_price) * next_open_price)) / \
                    (self._hold_stock_dict[market_code]['보유개수'] + self._get_price_size_for_bucket(level=bucket_level) // next_open_price)
                self._hold_stock_dict[market_code]['보유개수'] += self._get_price_size_for_bucket(level=bucket_level) // next_open_price
                self._hold_stock_dict[market_code]['총금액'] = self._hold_stock_dict[market_code]['평단가'] * self._hold_stock_dict[market_code]['보유개수']
                self._hold_stock_dict[market_code]['level'] += 1

                self.result[next_date]['추가 매수 종목'].append(market_code)

    def _sell(self, target_date):
        hold_stock_code_list = self._hold_stock_before_date(target_date)

        for market_code in hold_stock_code_list:
            df_hold_stock_price_info = self._hold_stock_price_dict[market_code]
            df_current_info = df_hold_stock_price_info[df_hold_stock_price_info['market_date'] == target_date]

            if not df_current_info.empty:
                high_price = int(df_current_info['high_price'])
                close_price = int(df_current_info['close_price'])
                target_sell_point = math.floor(self._hold_stock_dict[market_code]['평단가'] * (1 + self.profit_rate))

                if target_sell_point <= high_price:
                    profit = (target_sell_point * self._hold_stock_dict[market_code]['보유개수']) - (self._hold_stock_dict[market_code]['총금액'])
                    self._profit += profit
                    self._hold_stock_dict.pop(market_code)
                    self.result[target_date]['이익 매도 종목'].append(market_code)
                    self.result[target_date]['매도 이익'] += profit
                    self._hold_stock_price_dict.pop(market_code)
                    continue

                if self._get_hold_days_from_last_buy(market_code, target_date) >= self.max_holding_period:
                    profit = (close_price - self._hold_stock_dict[market_code]['평단가']) * self._hold_stock_dict[market_code]['보유개수']
                    self._profit += profit
                    self._hold_stock_dict.pop(market_code)
                    self._hold_stock_price_dict.pop(market_code)
                    self.result[target_date]['기간 초과 매도 종목'].append(market_code)
                    self.result[target_date]['매도 이익'] += profit

    def _update_account_balance(self, target_date):
        hold_stock_code_list = self._hold_stock_before_date(target_date)

        close_price_sum = 0
        for market_code in hold_stock_code_list:
            df_hold_stock_price_info = self._hold_stock_price_dict[market_code]
            df_current_info = df_hold_stock_price_info[df_hold_stock_price_info['market_date'] == target_date]

            if not df_current_info.empty:
                close_price = int(df_current_info['close_price'])
                print('>> 보유종목 :', market_code, '\t평단가 :', round(self._hold_stock_dict[market_code]['평단가']), '\t보유개수 :', self._hold_stock_dict[market_code]['보유개수'], '\t종가 :', close_price, '\t\tLV :', self._hold_stock_dict[market_code]['level'])
                close_price_sum += (close_price - self._hold_stock_dict[market_code]['평단가']) * self._hold_stock_dict[market_code]['보유개수']

        self.account_balance = self.seed + self._profit + close_price_sum
        self.result[target_date]['평가 금액'] = self.account_balance
        self.result[target_date]['수익률'] = round((self.account_balance - self.seed) / self.seed * 100, 3)

    def _simulator(self):
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

    def output(self):
        df = pd.DataFrame.from_dict(self.result, orient='index')
        df['매수 종목'] = df['매수 종목'].apply(lambda x: ', '.join(x))
        df['추가 매수 종목'] = df['추가 매수 종목'].apply(lambda x: ', '.join(x))
        df['이익 매도 종목'] = df['이익 매도 종목'].apply(lambda x: ', '.join(x))
        df['기간 초과 매도 종목'] = df['기간 초과 매도 종목'].apply(lambda x: ', '.join(x))

        df.reset_index(inplace=True)
        df = df.rename(columns={'index': '날짜'})
        return df[:-1]


if __name__ == "__main__":
    s = Simulator(
        start_date='2022-01-01',
        end_date='2023-04-06',
        seed=100000000,
        profit_rate=0.15,
        stop_loss_rate=None,
        loss_method=None,
        max_holding_period=180,
        buy_point_to_open_price=-0.02,
        additional_buy_point_to_price=-0.20,
        price_db='stock_info_2022_2023',
        target_db='stock_info_2022_2023_bollinger_5'
    )
    s.output().to_excel('./result.xlsx', index=False)