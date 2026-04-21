"""Intraday trade statistics computed from the closed trade list.

Provides win rate, average hold time, expectancy, and peak-to-trough
max drawdown. All values are calculated on demand from state.closed_trades
and cached for one second to avoid redundant work during rapid dashboard
refreshes.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from src.core.models import Trade


@dataclass
class DayStats:
    total_trades: int   = 0
    wins:         int   = 0
    losses:       int   = 0
    win_rate_pct: float = 0.0
    total_pnl:    float = 0.0
    avg_win:      float = 0.0       # average rupee gain on winning trades
    avg_loss:     float = 0.0       # average rupee loss on losing trades (positive number)
    expectancy:   float = 0.0       # (win_rate × avg_win) − (loss_rate × avg_loss)
    avg_hold_min: float = 0.0       # average hold time in minutes
    max_drawdown: float = 0.0       # peak-to-trough running P&L drawdown (positive ₹)


def compute_stats(trades: List[Trade]) -> DayStats:
    """Compute DayStats from a list of Trade objects (closed trades only)."""
    closed = [t for t in trades if t.exit_price > 0 and t.exit_time is not None]
    if not closed:
        return DayStats()

    wins   = [t for t in closed if t.pnl > 0]
    losses = [t for t in closed if t.pnl <= 0]

    win_rate  = len(wins) / len(closed)
    loss_rate = 1.0 - win_rate

    avg_win  = sum(t.pnl for t in wins)   / len(wins)   if wins   else 0.0
    avg_loss = sum(abs(t.pnl) for t in losses) / len(losses) if losses else 0.0

    expectancy = (win_rate * avg_win) - (loss_rate * avg_loss)

    # Average hold time (minutes)
    hold_times = [
        (t.exit_time - t.entry_time).total_seconds() / 60.0
        for t in closed
        if t.entry_time and t.exit_time
    ]
    avg_hold = sum(hold_times) / len(hold_times) if hold_times else 0.0

    # Max drawdown: peak-to-trough on running cumulative P&L
    sorted_trades = sorted(closed, key=lambda t: t.exit_time or datetime.min)
    running = 0.0
    peak    = 0.0
    max_dd  = 0.0
    for t in sorted_trades:
        running += t.pnl
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    return DayStats(
        total_trades = len(closed),
        wins         = len(wins),
        losses       = len(losses),
        win_rate_pct = win_rate * 100.0,
        total_pnl    = sum(t.pnl for t in closed),
        avg_win      = avg_win,
        avg_loss     = avg_loss,
        expectancy   = expectancy,
        avg_hold_min = avg_hold,
        max_drawdown = max_dd,
    )


class CachedStats:
    """Wraps compute_stats() with a 1-second TTL cache to avoid re-computing on every dashboard tick."""

    def __init__(self, ttl_seconds: float = 1.0) -> None:
        self._ttl    = ttl_seconds
        self._cache: DayStats = DayStats()
        self._last_at: float  = 0.0

    def get(self, trades: List[Trade]) -> DayStats:
        now = time.monotonic()
        if now - self._last_at >= self._ttl:
            self._cache   = compute_stats(trades)
            self._last_at = now
        return self._cache


# Module-level cache shared by the dashboard
cached_stats = CachedStats()
