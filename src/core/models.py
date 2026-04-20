"""Core data models used across the bot."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4


class TradeType(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class TradeStatus(str, Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class ExitReason(str, Enum):
    TARGET_HIT = "TARGET_HIT"
    STOP_LOSS = "STOP_LOSS"
    TRAILING_SL = "TRAILING_SL"
    SUPERTREND_FLIP = "SUPERTREND_FLIP"
    SQUARE_OFF_EOD = "SQUARE_OFF_EOD"
    GAP_PROTECTION = "GAP_PROTECTION"
    MANUAL = "MANUAL"
    DAILY_LOSS_HIT = "DAILY_LOSS_HIT"


class SignalStrength(str, Enum):
    STRONG = "STRONG"
    MEDIUM = "MEDIUM"
    WEAK = "WEAK"
    NONE = "NONE"


@dataclass
class Signal:
    """A trading signal emitted by the strategy engine."""
    timestamp: datetime
    trade_type: TradeType
    strength: SignalStrength
    underlying: str           # e.g. "BANKNIFTY"
    underlying_price: float   # spot / future price at signal time
    reasons: list[str] = field(default_factory=list)
    conditions_met: int = 0   # count out of 6
    volume_ratio: float = 1.0  # current vol / avg vol


@dataclass
class Trade:
    """Represents a single scalping trade lifecycle."""
    trade_id: str = field(default_factory=lambda: str(uuid4())[:8])
    symbol: str = ""                 # e.g. "BANKNIFTY25JAN52000CE"
    underlying: str = ""             # "BANKNIFTY"
    trade_type: TradeType = TradeType.LONG
    signal_strength: SignalStrength = SignalStrength.MEDIUM

    # Entry
    entry_time: Optional[datetime] = None
    entry_price: float = 0.0
    quantity: int = 0
    lots: int = 0

    # Risk
    stop_loss: float = 0.0
    target: float = 0.0
    trailing_sl: float = 0.0
    max_risk_points: float = 0.0
    capital_used: float = 0.0

    # Exit
    exit_time: Optional[datetime] = None
    exit_price: float = 0.0
    exit_quantity: int = 0
    exit_reason: Optional[ExitReason] = None

    # P&L
    pnl: float = 0.0
    pnl_points: float = 0.0

    # State
    status: TradeStatus = TradeStatus.PENDING

    # Order IDs (live mode)
    entry_order_id: str = ""
    exit_order_id: str = ""

    # Flags
    is_paper: bool = True

    # Underlying price at entry (for context)
    underlying_entry_price: float = 0.0
    underlying_exit_price: float = 0.0

    def is_open(self) -> bool:
        return self.status == TradeStatus.OPEN

    def is_closed(self) -> bool:
        return self.status == TradeStatus.CLOSED

    def update_pnl(self, current_price: float) -> float:
        """Compute unrealised P&L at the given price."""
        if self.trade_type == TradeType.LONG:
            points = current_price - self.entry_price
        else:
            points = self.entry_price - current_price
        self.pnl_points = points
        self.pnl = points * self.quantity
        return self.pnl

    def finalise_pnl(self) -> None:
        """Compute final P&L after exit (options premium: long-only)."""
        # For option buying, we go long both CE (for bullish underlying) and PE (for bearish underlying).
        # The `trade_type` represents the VIEW on underlying, but at the option level it's always a long.
        # P&L = (exit_price - entry_price) * quantity
        self.pnl_points = self.exit_price - self.entry_price
        self.pnl = self.pnl_points * self.quantity

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "underlying": self.underlying,
            "trade_type": self.trade_type.value,
            "signal_strength": self.signal_strength.value,
            "entry_time": self.entry_time.isoformat() if self.entry_time else "",
            "entry_price": round(self.entry_price, 2),
            "quantity": self.quantity,
            "lots": self.lots,
            "stop_loss": round(self.stop_loss, 2),
            "target": round(self.target, 2),
            "trailing_sl": round(self.trailing_sl, 2),
            "capital_used": round(self.capital_used, 2),
            "exit_time": self.exit_time.isoformat() if self.exit_time else "",
            "exit_price": round(self.exit_price, 2),
            "exit_quantity": self.exit_quantity,
            "exit_reason": self.exit_reason.value if self.exit_reason else "",
            "pnl": round(self.pnl, 2),
            "pnl_points": round(self.pnl_points, 2),
            "status": self.status.value,
            "is_paper": self.is_paper,
        }


@dataclass
class Candle:
    """OHLCV candle."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: int = 0

    @property
    def is_green(self) -> bool:
        return self.close > self.open

    @property
    def is_red(self) -> bool:
        return self.close < self.open

    @property
    def body(self) -> float:
        return abs(self.close - self.open)
