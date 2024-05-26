import unittest
from decimal import Decimal

import pandas as pd

from script2 import UserOperationsDetector


class TestUserOperationsDetector(unittest.TestCase):

    def test_should_find_deposit_bet_withdrawals_sequance(self):
        deposits = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2022 20:00', periods=3, freq='30min'),
            'paid_amount': [Decimal(100), Decimal(100), Decimal(100)],
            'paid_currency': ['USD', 'USD', 'USD']
        })

        withdrawals = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2022 20:03', periods=1, freq='30min'),
            'paid_amount': [100],
        })

        bets = pd.DataFrame({
            'accept_time': pd.date_range(start='1/1/2022 20:00', periods=3, freq='20min'),
            'settlement_exchange_rate': [Decimal('0.8'), Decimal('0.8'), Decimal('0.8')],
            'currency': ['USD', 'USD', 'USD'],
            'amount': [Decimal(100), Decimal(100), Decimal(300)],
        })

        detector = UserOperationsDetector(1, deposits, withdrawals, bets)
        self.assertTrue(detector.has_dep_bet_withd_sequence())
