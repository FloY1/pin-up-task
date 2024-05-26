import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import getcontext, Decimal, InvalidOperation
from typing import Callable

import pandas as pd

logger = logging.getLogger(__name__)

getcontext().prec = 5


class TypeConverter:
    """
    A utility class that provides methods for converting data types.
    """

    DATE_FORMATS = ['%d/%m/%Y %H:%M', '%m%d%Y %I:%M %p']

    @staticmethod
    def to_numeric(value) -> int | None:
        try:
            return int(value)
        except ValueError:
            logger.warning(f'Failed to convert {value} to int')
            return None

    @staticmethod
    def to_decimal(value) -> Decimal | None:
        try:
            return Decimal(value)
        except InvalidOperation:
            logger.warning(f'Failed to convert {value} to decimal')
            return None

    @classmethod
    def to_date(cls, value) -> datetime | None:
        for format in cls.DATE_FORMATS:
            try:
                return datetime.strptime(value, format)
            except ValueError:
                continue
        logger.warning(f'Failed to convert {value} to date')
        return None


class DataProvider(ABC):
    """
    An abstract class that provides methods for getting data.
    """

    @abstractmethod
    def get_payments(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_bets(self) -> pd.DataFrame:
        pass


class CSVDataProvider(DataProvider):
    PAYMENTS_COLUMNS = {'Date': TypeConverter.to_date,
                        'player_id': TypeConverter.to_numeric,
                        'paid_amount': TypeConverter.to_decimal,
                        'transaction_type': str,
                        'status': str,
                        'paid_currency': str}

    PAYMENTS_DIRECTORY = 'payments'

    BETS_DIRECTORY = 'bets'

    BETS_COLUMNS = {'bet_id': str,
                    'accept_time': TypeConverter.to_date,
                    'result': str,
                    'price_change_policy': str,
                    'settlement_exchange_rate': str,
                    'currency': str,
                    'player_id': TypeConverter.to_numeric,
                    'amount': TypeConverter.to_decimal,
                    'profit': TypeConverter.to_decimal,
                    'payout': TypeConverter.to_decimal}

    NA_VALUES = ['na', 'NaN', 'error']

    @classmethod
    def get_payments(cls) -> pd.DataFrame:
        return cls._get_data(cls.PAYMENTS_DIRECTORY, cls.PAYMENTS_COLUMNS)

    @classmethod
    def get_bets(cls) -> pd.DataFrame:
        return cls._get_data(cls.BETS_DIRECTORY, cls.BETS_COLUMNS)

    @classmethod
    def _get_data(cls, folder_name: str, columns: dict[str, Callable]) -> pd.DataFrame:
        files = [f for f in os.listdir(folder_name) if f.endswith('.csv')]
        df = pd.concat([cls._read_csv(folder_name, file, columns) for file in files])
        return df.dropna()

    @classmethod
    def _read_csv(cls, folder_name: str, file_name: str, columns: dict[str, Callable]) -> pd.DataFrame:
        file_path = os.path.join(folder_name, file_name)
        df = pd.read_csv(file_path,
                         usecols=columns.keys(),
                         converters=columns,
                         na_values=cls.NA_VALUES)
        return df


class UserOperationsDetector:
    """
     A class that provides methods for identifying suspicious sequences of operations from a user.
    """

    def __init__(self, player_id, deposits: pd.DataFrame, withdrawals: pd.DataFrame, bets: pd.DataFrame):
        self._player_id: int = player_id
        self._deposits: pd.DataFrame = deposits
        self._withdrawals: pd.DataFrame = withdrawals
        self._bets: pd.DataFrame = bets

    def has_dep_bet_withd_sequence(self,
                                   bet_amount_range: Decimal = Decimal('0.1'),
                                   td_bw_dep_withd: pd.Timedelta = pd.Timedelta(hours=1)) -> bool:
        """
        Check if there is a sequence of deposit, bet, and withdrawal operations.
        :param bet_amount_range: The acceptable range for a bet amount as a proportion of the deposit amount.
                          For example, a bet_range of 0.1 allows bet amounts that are within 10% of the deposit amount.
        :param td_bw_dep_withd: The maximum acceptable time difference between a deposit and a withdrawal.
        :return: True if a sequence is found, False otherwise.
        """

        withdrawals_start = 0
        bets_start = 0

        for _, deposit in self._deposits.iterrows():
            withdrawals_start = self._next_withdrawal_index(withdrawals_start, deposit)
            for withdrawal_index in range(withdrawals_start, len(self._withdrawals)):
                withdrawal = self._withdrawals.iloc[withdrawal_index]
                if withdrawal['Date'] - deposit['Date'] < td_bw_dep_withd:
                    bets_start = self._next_bet_index(bets_start, deposit)
                    if self._has_bet(deposit, withdrawal, bets_start, bet_amount_range):
                        return True
                else:
                    break

        return False

    def _has_bet(self, deposit: pd.Series, withdrawal: pd.Series, bets_start: int, bet_range: Decimal) -> bool:
        """
        Check whether there is a bet that meets the condition between deposit and withdrawal.
        :param bets_start: The index of the first bet operation to consider.
        :param bet_range: The acceptable range for a bet amount as a proportion of the deposit amount.
        :return:
        """
        for bet_index in range(bets_start, len(self._bets)):
            bet = self._bets.iloc[bet_index]
            if bet['accept_time'] < withdrawal['Date']:
                break

            # TODO do something with the currency
            if bet['amount'] * (1 - bet_range) <= deposit['paid_amount'] <= bet['amount'] * (1 + bet_range):
                return True
        return False

    def _next_bet_index(self, bets_start: int, deposit: pd.Series) -> int:
        """
        Get the index of the next bet operation after the given deposit operation.
        """
        return self._next_index(bets_start,
                                self._bets,
                                lambda bet: bet['accept_time'] > deposit['Date'])

    def _next_withdrawal_index(self, withdrawals_start: int, deposit: pd.Series) -> int:
        """
        Get the index of the next withdrawal operation after the given deposit operation.
        """
        return self._next_index(withdrawals_start,
                                self._withdrawals,
                                lambda withdrawals: withdrawals['Date'] > deposit['Date'])

    @staticmethod
    def _next_index(start: int, df: pd.DataFrame(), predicate: Callable[[pd.Series], bool]) -> int:
        """
        Get the index of the next operation based on the given predicate.
        """
        for index in range(start, len(df)):
            row = df.iloc[index]
            if predicate(row):
                return index

        return len(df)


if __name__ == '__main__':
    print('Script 2 started')
