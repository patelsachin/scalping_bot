"""Main entry point. Starts the trading engine and terminal dashboard in parallel."""
from __future__ import annotations

import argparse
import sys
import threading
import time

from src.core.engine import TradingEngine
from src.dashboard.terminal_dashboard import run_dashboard
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Siva Scalping Bot")
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Run without the terminal dashboard (log output only)",
    )
    parser.add_argument(
        "--dashboard-only",
        action="store_true",
        help="Run dashboard only (read-only view, for testing)",
    )
    parser.add_argument(
        "--poll",
        type=int,
        default=5,
        help="Main loop poll interval in seconds (default: 5)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    log.info(f"Python startup. Paper mode: {config.is_paper_mode()}")

    if args.dashboard_only:
        # Only show dashboard (useful for viewing past state)
        try:
            run_dashboard()
        except KeyboardInterrupt:
            pass
        return 0

    # Spin up bot
    engine = TradingEngine()

    if args.no_dashboard:
        # Run bot in foreground
        engine.run(poll_interval_sec=args.poll)
        return 0

    # Run dashboard in a thread, bot in main thread
    # (dashboard uses rich.Live which needs the main terminal; but we can flip this)
    # Strategy: run BOT in a background thread, DASHBOARD in main thread
    def bot_target():
        try:
            engine.run(poll_interval_sec=args.poll)
        except Exception as e:
            log.exception(f"Bot thread crashed: {e}")

    bot_thread = threading.Thread(target=bot_target, daemon=True, name="BotThread")
    bot_thread.start()

    # Give bot a moment to initialise
    time.sleep(2)

    try:
        run_dashboard()
    except KeyboardInterrupt:
        log.info("Dashboard exited. Shutting down bot.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
