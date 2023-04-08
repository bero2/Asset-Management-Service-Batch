from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta


class TargetFilter:
    def __init__(self, dataframe: pd.DataFrame, target_date: str):
        self._table = dataframe.sort_values(['market_code', 'market_date'], ascending=True).reset_index(drop=True).astype(str)
        self.target_date = target_date
        self._market_code_list = self._table['market_code'].unique()

    def _golden_cross(self, new_column_name: str, base_column: str, target_column: str) -> None:
        row = []
        for market_code in self._market_code_list:
            df = self._table[self._table['market_code'] == market_code][['market_code', 'market_date', base_column, target_column]]
            df['shift_base_column'] = df[base_column].shift(1)
            df['shift_target_column'] = df[target_column].shift(1)

            for idx in df.index:
                if df['shift_base_column'][idx] is not None and df['shift_base_column'][idx] < df['shift_target_column'][idx] and df[base_column][idx] > df[target_column][idx]:
                    row.append(1)
                else:
                    row.append(0)

        self._table[new_column_name] = row

    def _is_exist_golden_cross_in_period_filter(self, target_date: str, target_column: str, period=20) -> list:
        market_code_list = []
        for market_code in self._market_code_list:
            df = self._table[
                (self._table['market_code'] == market_code) &
                (self._table['market_date'] <= str(target_date)) &
                (self._table['market_date'] >= str(datetime.strptime(target_date, '%Y-%m-%d').date() + relativedelta(days=-period))) &
                (self._table[target_column] == 1)
            ]

            if len(df) > 0:
                market_code_list.append(market_code)

        return market_code_list

    def output(self) -> pd.DataFrame:
        self._golden_cross('골든크로스_EMA112', 'close_price', 'ema_112')
        self._golden_cross('골든크로스_EMA224', 'close_price', 'ema_224')
        golden_cross_market_code_list = list(
            set(self._is_exist_golden_cross_in_period_filter(self.target_date, '골든크로스_EMA112', 20)) |
            set(self._is_exist_golden_cross_in_period_filter(self.target_date, '골든크로스_EMA224', 20))
        )

        target_table = self._table[
            (self._table['market_code'].isin(golden_cross_market_code_list)) &
            (self._table['market_date'] == self.target_date)
        ][['market_code', 'company_name', 'open_price', 'high_price', 'low_price', 'close_price']]

        return target_table.reset_index(drop=True)
