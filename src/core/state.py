"""Shared in-memory state container. Accessed by the bot (writer) and the dashboard (reader)."""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Optional

from src.core.models import Trade


class BotState:
    """Thread-safe container for the bot's live state."""

    def __init__(self) -> None:
        self._lock = threading.RLock()

        self.mode: str = "paper"
        self.connected: bool = False
        self.started_at: Optional[datetime] = None

        self.daily_budget: float = 0.0
        self.capital_deployed: float = 0.0
        self.capital_available: float = 0.0
        self.realised_pnl: float = 0.0
        self.unrealised_pnl: float = 0.0

        self.open_trades: list[Trade] = []
        self.closed_trades: list[Trade] = []

        self.last_signal_time: Optional[datetime] = None
        self.last_candle_time: Optional[datetime] = None
        self.last_error: str = ""

        self.underlying_ltp: float = 0.0
        self.atm_strike: float = 0.0

        self.halted: bool = False
        self.halt_reason: str = ""

    # -------------- accessors ------------------
    def snapshot(self) -> dict:
        """Thread-safe snapshot dict for dashboard rendering."""
        with self._lock:
            total_pnl = self.realised_pnl + self.unrealised_pnl
            return {
                "mode": self.mode,
                "connected": self.connected,
                "started_at": self.started_at,
                "daily_budget": self.daily_budget,
                "capital_deployed": self.capital_deployed,
                "capital_available": self.capital_available,
                "capital_balance": self.daily_budget - self.capital_deployed,
                "realised_pnl": self.realised_pnl,
                "unrealised_pnl": self.unrealised_pnl,
                "total_pnl": total_pnl,
                "open_trades": list(self.open_trades),
                "closed_trades": list(self.closed_trades),
                "last_signal_time": self.last_signal_time,
                "last_candle_time": self.last_candle_time,
                "last_error": self.last_error,
                "underlying_ltp": self.underlying_ltp,
                "atm_strike": self.atm_strike,
                "halted": self.halted,
                "halt_reason": self.halt_reason,
            }

    # -------------- trade tracking -------------
    def add_open_trade(self, trade: Trade) -> None:
        with self._lock:
            self.open_trades.append(trade)
            self.capital_deployed += trade.capital_used

    def close_trade(self, trade: Trade) -> None:
        with self._lock:
            if trade in self.open_trades:
                self.open_trades.remove(trade)
            self.capital_deployed -= trade.capital_used
            self.capital_deployed = max(0.0, self.capital_deployed)
            self.realised_pnl += trade.pnl
            self.closed_trades.append(trade)

    def update_unrealised(self, total: float) -> None:
        with self._lock:
            self.unrealised_pnl = total

    def halt(self, reason: str) -> None:
        with self._lock:
            self.halted = True
            self.halt_reason = reason

    def reset_halt(self) -> None:
        with self._lock:
            self.halted = False
            self.halt_reason = ""


# module-level singleton
state = BotState()
