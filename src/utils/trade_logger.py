"""Trade logger: appends every closed trade to a CSV file.

The strategy column records which strategy generated each trade so multiple
instances running in parallel (e.g. scalping vs ichimoku) can be compared
in post-session analysis.

Header migration: if an existing trades.csv has an old schema (missing columns),
it is automatically backed up and a fresh file is created with the current schema.
"""
from __future__ import annotations

import csv
import time
from pathlib import Path

from src.core.models import Trade
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


class TradeLogger:
    """Appends trades to CSV. Thread-safe via file append mode."""

    HEADERS = [
        "trade_id",
        "symbol",
        "underlying",
        "trade_type",        # always LONG — system only buys options
        "option_type",       # CE (bullish view) or PE (bearish view)
        "signal_strength",
        "strategy",          # ← which strategy generated this trade
        "market",            # ← which market: india | us
        "entry_time",
        "entry_price",
        "quantity",
        "lots",
        "stop_loss",
        "target",
        "trailing_sl",
        "capital_used",
        "exit_time",
        "exit_price",
        "exit_quantity",
        "exit_reason",
        "pnl",
        "pnl_points",
        "status",
        "is_paper",
    ]

    def __init__(self) -> None:
        log_path = config.project_root / config.get(
            "logging.trade_log_file", "logs/trades.csv"
        )
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self.path: Path = log_path
        self._ensure_header()

    def _ensure_header(self) -> None:
        """Write CSV header. If the file exists but has a different schema
        (e.g. missing the strategy column after an upgrade), back it up and
        start a fresh file so the column layout stays consistent.
        """
        if self.path.exists() and self.path.stat().st_size > 0:
            try:
                with open(self.path, "r", newline="", encoding="utf-8") as f:
                    existing_headers = next(csv.reader(f), [])
                if existing_headers == self.HEADERS:
                    return  # schema matches — nothing to do
                # Schema mismatch: back up old file
                backup = self.path.with_name(
                    f"{self.path.stem}_backup_{int(time.time())}{self.path.suffix}"
                )
                self.path.rename(backup)
                log.warning(
                    f"Trade log schema changed (added 'strategy' column). "
                    f"Old file backed up -> {backup.name}. Starting fresh."
                )
            except Exception as e:
                log.error(f"Could not check trade log header: {e}")
                return

        with open(self.path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.HEADERS)
            writer.writeheader()

    def log_trade(self, trade: Trade) -> None:
        row   = trade.to_dict()
        clean = {k: row.get(k, "") for k in self.HEADERS}
        try:
            with open(self.path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                writer.writerow(clean)
            log.debug(
                f"Trade logged: {trade.trade_id} strategy={trade.strategy} P&L={trade.pnl:.2f}"
            )
        except Exception as e:
            log.error(f"Failed to log trade {trade.trade_id}: {e}")
