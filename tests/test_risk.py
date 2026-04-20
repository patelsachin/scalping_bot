"""Unit tests for RiskManager."""
from __future__ import annotations

from datetime import datetime

import pytest

from src.core.models import (
    ExitReason,
    Signal,
    SignalStrength,
    Trade,
    TradeStatus,
    TradeType,
)
from src.risk.risk_manager import RiskManager


@pytest.fixture
def risk():
    return RiskManager()


@pytest.fixture
def strong_signal():
    return Signal(
        timestamp=datetime(2024, 1, 15, 9, 30),
        trade_type=TradeType.LONG,
        strength=SignalStrength.STRONG,
        underlying="BANKNIFTY",
        underlying_price=50000.0,
        conditions_met=6,
        volume_ratio=1.8,
    )


@pytest.fixture
def medium_signal():
    return Signal(
        timestamp=datetime(2024, 1, 15, 9, 30),
        trade_type=TradeType.LONG,
        strength=SignalStrength.MEDIUM,
        underlying="BANKNIFTY",
        underlying_price=50000.0,
        conditions_met=5,
        volume_ratio=1.1,
    )


class TestPositionSizing:
    def test_strong_signal_allocation(self, risk, strong_signal):
        """Strong signal should allocate 30% of budget."""
        qty, lots, cap = risk.compute_position_size(
            strong_signal, option_premium=100.0, available_capital=100000.0
        )
        # 30% of 100k = 30k. At ₹100 premium and lot size 15: cost per lot = 1500
        # Max 20 lots affordable, but capped at max_lots (3). qty = 3 * 15 = 45
        assert lots <= risk.max_lots
        assert qty == lots * risk.lot_size
        assert cap == qty * 100.0

    def test_medium_signal_allocation(self, risk, medium_signal):
        """Medium signal should allocate 20% of budget."""
        qty, lots, cap = risk.compute_position_size(
            medium_signal, option_premium=100.0, available_capital=100000.0
        )
        assert lots <= risk.max_lots
        assert qty == lots * risk.lot_size

    def test_insufficient_capital(self, risk, strong_signal):
        """When capital is too low, returns zero."""
        qty, lots, cap = risk.compute_position_size(
            strong_signal, option_premium=500.0, available_capital=100.0
        )
        assert qty == 0
        assert lots == 0
        assert cap == 0.0

    def test_zero_premium_rejected(self, risk, strong_signal):
        qty, lots, cap = risk.compute_position_size(
            strong_signal, option_premium=0.0, available_capital=100000.0
        )
        assert qty == 0


class TestStopLoss:
    def test_long_sl_uses_structural_low(self, risk):
        # Structural low (95) is above the max_risk_pts floor, so we use structural
        sl = risk.initial_stop_loss(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            first_candle_low=95.0,
            first_candle_high=102.0,
        )
        # max_risk = 100 - 20 = 80. structural = 95. max(80, 95) = 95
        assert sl == 95.0

    def test_long_sl_capped_by_max_risk(self, risk):
        # Structural low way below max_risk_pts floor -> capped
        sl = risk.initial_stop_loss(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            first_candle_low=50.0,
            first_candle_high=102.0,
        )
        # max_risk = 100 - 20 = 80. structural = 50. max(80, 50) = 80
        assert sl == 80.0

    def test_target(self, risk):
        assert risk.initial_target(TradeType.LONG, 100.0) == 110.0
        assert risk.initial_target(TradeType.SHORT, 100.0) == 90.0


class TestTrailingSL:
    def test_no_trail_before_activation(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=95.0,
        )
        new_sl = risk.compute_trailing_sl(trade, current_price=103.0)  # only 3 pts profit
        # Activation is 5 pts, so no trail yet
        assert new_sl == 95.0

    def test_trail_activates_after_threshold(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=95.0,
        )
        # At 5 pts profit (price 105), trail activates
        new_sl = risk.compute_trailing_sl(trade, current_price=105.0)
        # First activation step: SL moves to entry (100.0)
        assert new_sl == 100.0

    def test_trail_steps_up(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=100.0,
        )
        new_sl = risk.compute_trailing_sl(trade, current_price=110.0)
        # Profit = 10 pts. Steps = ((10 - 5) // 5) + 1 = 2 -> SL = 100 + 5 = 105
        assert new_sl == 105.0

    def test_trail_never_goes_backward(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=108.0,  # Already trailed up
        )
        # Price pulls back - SL should not move down
        new_sl = risk.compute_trailing_sl(trade, current_price=106.0)
        assert new_sl == 108.0


class TestExitEvaluation:
    def test_long_sl_hit(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=95.0,
            target=110.0,
            status=TradeStatus.OPEN,
        )
        reason = risk.check_exit(trade, current_price=94.0, supertrend_dir=1)
        assert reason == ExitReason.STOP_LOSS

    def test_long_target_hit(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=95.0,
            target=110.0,
            status=TradeStatus.OPEN,
        )
        reason = risk.check_exit(trade, current_price=111.0, supertrend_dir=1)
        assert reason == ExitReason.TARGET_HIT

    def test_supertrend_flip_exits_long(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=95.0,
            target=110.0,
            status=TradeStatus.OPEN,
        )
        reason = risk.check_exit(trade, current_price=102.0, supertrend_dir=-1)
        assert reason == ExitReason.SUPERTREND_FLIP


class TestDailyLoss:
    def test_loss_within_cap(self, risk):
        assert risk.is_daily_loss_breached(-500.0) is False

    def test_loss_breached(self, risk):
        # Default cap: 1% of 100000 = 1000
        assert risk.is_daily_loss_breached(-1500.0) is True


class TestGapProtection:
    def test_long_gap_down_breaches_sl(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=95.0,
        )
        # Gap down opens at 93 - below SL
        assert risk.should_gap_exit(trade, market_open_price=93.0) is True

    def test_long_no_gap_exit_if_above_sl(self, risk):
        trade = Trade(
            trade_type=TradeType.LONG,
            entry_price=100.0,
            stop_loss=95.0,
            trailing_sl=95.0,
        )
        assert risk.should_gap_exit(trade, market_open_price=99.8) is False
