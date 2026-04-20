"""Trade logger: appends every closed trade to a CSV file."""
from __future__ import annotations

import csv
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
        "trade_type",
        "signal_strength",
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
        if not self.path.exists() or self.path.stat().st_size == 0:
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                writer.writeheader()

    def log_trade(self, trade: Trade) -> None:
        row = trade.to_dict()
        # Reorder / filter to match headers
        clean = {k: row.get(k, "") for k in self.HEADERS}
        try:
            with open(self.path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.HEADERS)
                writer.writerow(clean)
            log.debug(f"Trade logged: {trade.trade_id} P&L={trade.pnl:.2f}")
        except Exception as e:
            log.error(f"Failed to log trade {trade.trade_id}: {e}")
