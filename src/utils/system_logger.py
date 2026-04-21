"""System event CSV logger.

Writes non-trade events (connect, halt, kill switch, regime change, etc.) to
logs/system.csv so operators can audit what the bot did during a session without
digging through the full system.log.

Usage:
    from src.utils.system_logger import sys_log
    sys_log.event("CONNECTED", "Kite broker authenticated")
    sys_log.event("HALT", "Daily loss limit hit")
"""
from __future__ import annotations

import csv
import threading
from datetime import datetime
from pathlib import Path

from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)

# Well-known event types — kept consistent so the CSV is easy to filter
EVENT_STARTUP       = "STARTUP"
EVENT_CONNECTED     = "CONNECTED"
EVENT_DISCONNECTED  = "DISCONNECTED"
EVENT_RECONNECT     = "RECONNECT"
EVENT_HALT          = "HALT"
EVENT_KILL_SWITCH   = "KILL_SWITCH"
EVENT_REGIME_CHANGE = "REGIME_CHANGE"
EVENT_SHUTDOWN      = "SHUTDOWN"
EVENT_TOKEN_STALE   = "TOKEN_STALE"
EVENT_CONFIG_RELOAD = "CONFIG_RELOAD"


class SystemLogger:
    """Appends system events to logs/system.csv. Thread-safe."""

    HEADERS = ["timestamp", "event_type", "detail", "vix", "market_regime", "mode"]

    def __init__(self) -> None:
        log_path = config.project_root / config.get(
            "logging.system_csv_file", "logs/system.csv"
        )
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self.path: Path = log_path
        self._lock = threading.Lock()
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not self.path.exists() or self.path.stat().st_size == 0:
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self.HEADERS).writeheader()

    def event(self, event_type: str, detail: str = "") -> None:
        """Append one event row. Imports state lazily to avoid circular import."""
        try:
            from src.core.state import state  # lazy import
            vix    = f"{state.vix:.2f}"
            regime = state.market_regime
            mode   = state.mode
        except Exception:
            vix = regime = mode = ""

        row = {
            "timestamp":    datetime.now().isoformat(timespec="seconds"),
            "event_type":   event_type,
            "detail":       detail,
            "vix":          vix,
            "market_regime": regime,
            "mode":         mode,
        }
        with self._lock:
            try:
                with open(self.path, "a", newline="", encoding="utf-8") as f:
                    csv.DictWriter(f, fieldnames=self.HEADERS).writerow(row)
            except Exception as e:
                log.error(f"SystemLogger failed to write event '{event_type}': {e}")


# Module-level singleton
sys_log = SystemLogger()
