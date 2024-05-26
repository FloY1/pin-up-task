import unittest
from decimal import Decimal

import pandas as pd

from script2 import UserOperationsDetector


class TestUserOperationsDetector(unittest.TestCase):

    def test_should_find_sequence(self):
        deposits = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2022 20:00', periods=3, freq='30min'),
            'paid_amount': [Decimal(100), Decimal(10), Decimal(20)],
            'paid_currency': ['USD', 'USD', 'USD']
        })

        withdrawals = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2022 20:30', periods=1, freq='30min'),
            'paid_amount': [100],
        })

        bets = pd.DataFrame({
            'accept_time': pd.date_range(start='1/1/2022 20:00', periods=6, freq='10min'),
            'settlement_exchange_rate': [Decimal('0.8') for _ in range(6)],
            'currency': ['USD' for _ in range(6)],
            'amount': [Decimal(100), Decimal(89), Decimal(110), Decimal(400), Decimal(300), Decimal(300)],
        })

        detector = UserOperationsDetector(deposits, withdrawals, bets)
        self.assertTrue(detector.has_dep_bet_withd_sequence())

    def test_should_find_sequence_different_currencies(self):
        deposits = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2022 20:00', periods=3, freq='30min'),
            'paid_amount': [Decimal("100"), Decimal(10), Decimal(20)],
            'paid_currency': ['USD', 'EUR', 'EUR']
        })

        withdrawals = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2022 20:30', periods=1, freq='30min'),
            'paid_amount': [100],
        })

        bets = pd.DataFrame({
            'accept_time': pd.date_range(start='1/1/2022 20:00', periods=6, freq='10min'),
            'settlement_exchange_rate': [Decimal('0.8') for _ in range(6)],
            'currency': ['EUR' for _ in range(6)],
            'amount': [Decimal(100), Decimal(85), Decimal(110), Decimal(400), Decimal(300), Decimal(300)],
        })

        detector = UserOperationsDetector(deposits, withdrawals, bets)
        self.assertTrue(detector.has_dep_bet_withd_sequence())

    def test_should_not_find_sequence(self):
        deposits = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2022 20:00', periods=3, freq='30min'),
            'paid_amount': [Decimal(100), Decimal(10), Decimal(20)],
            'paid_currency': ['USD', 'USD', 'USD']
        })

        withdrawals = pd.DataFrame({
            'Date': pd.date_range(start='1/1/2022 20:30', periods=1, freq='30min'),
            'paid_amount': [100],
        })

        bets = pd.DataFrame({
            'accept_time': pd.date_range(start='1/1/2022 20:00', periods=6, freq='10min'),
            'settlement_exchange_rate': [Decimal('0.8') for _ in range(6)],
            'currency': ['USD' for _ in range(6)],
            'amount': [Decimal(100), Decimal(89), Decimal(111), Decimal(400), Decimal(300), Decimal(300)],
        })

        detector = UserOperationsDetector(deposits, withdrawals, bets)
        self.assertFalse(detector.has_dep_bet_withd_sequence())

    def test_should_find_win_streak(self):
        bets = pd.DataFrame({
            'amount': [Decimal(100), Decimal(100), Decimal(100)],
            'payout': [Decimal(200), Decimal(200), Decimal(200)],
        })

        detector = UserOperationsDetector(None, None, bets)
        self.assertTrue(detector.has_win_streak(streak_len=3))

    def test_should_not_find_win_streak(self):
        bets = pd.DataFrame({
            'amount': [Decimal(100), Decimal(100), Decimal(100)],
            'payout': [Decimal(200), Decimal(150), Decimal(200)],
        })

        detector = UserOperationsDetector(None, None, bets)
        self.assertFalse(detector.has_win_streak(streak_len=3))