"""KiteTicker WebSocket wrapper for streaming real-time tick data."""
from __future__ import annotations

import threading
from typing import Callable, Optional

from src.utils.logger import get_logger

log = get_logger(__name__)


class TickerManager:
    """Thin wrapper around KiteTicker that manages subscriptions and dispatches ticks.

    All callbacks (on_ticks, on_connect, on_close, on_error) are invoked from the
    KiteTicker background thread — handlers must be thread-safe.
    """

    def __init__(self, api_key: str, access_token: str) -> None:
        self._api_key = api_key
        self._access_token = access_token
        self._ticker = None
        self._tick_handlers: list[Callable[[list[dict]], None]] = []
        self._connect_handler: Optional[Callable[[], None]] = None
        self._close_handler: Optional[Callable[[int, str], None]] = None
        self._error_handler: Optional[Callable[[Exception], None]] = None
        self._subscriptions: dict[int, str] = {}  # token -> mode
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------
    def on_ticks(self, handler: Callable[[list[dict]], None]) -> None:
        self._tick_handlers.append(handler)

    def on_connect(self, handler: Callable[[], None]) -> None:
        self._connect_handler = handler

    def on_close(self, handler: Callable[[int, str], None]) -> None:
        self._close_handler = handler

    def on_error(self, handler: Callable[[Exception], None]) -> None:
        self._error_handler = handler

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------
    def subscribe(self, tokens: list[int], mode: str = "full") -> None:
        """Subscribe tokens. mode: 'ltp', 'quote', or 'full'."""
        with self._lock:
            for t in tokens:
                self._subscriptions[t] = mode
        if self._ticker is not None:
            self._ticker.subscribe(tokens)
            self._ticker.set_mode(mode, tokens)
            log.info(f"Subscribed {tokens} in {mode} mode")

    def unsubscribe(self, tokens: list[int]) -> None:
        with self._lock:
            for t in tokens:
                self._subscriptions.pop(t, None)
        if self._ticker is not None:
            self._ticker.unsubscribe(tokens)
            log.debug(f"Unsubscribed {tokens}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> bool:
        """Initialise KiteTicker and connect in a daemon thread."""
        try:
            from kiteconnect import KiteTicker
        except ImportError:
            log.error("kiteconnect not installed. Cannot start WebSocket.")
            return False

        self._ticker = KiteTicker(self._api_key, self._access_token)
        self._ticker.on_ticks = self._dispatch_ticks
        self._ticker.on_connect = self._handle_connect
        self._ticker.on_close = self._handle_close
        self._ticker.on_error = self._handle_error
        self._ticker.on_reconnect = self._handle_reconnect
        self._ticker.on_noreconnect = self._handle_noreconnect
        # threaded=True: KiteTicker runs connect() in a daemon thread and returns immediately
        self._ticker.connect(threaded=True)
        log.info("KiteTicker starting (threaded=True)")
        return True

    def stop(self) -> None:
        if self._ticker is not None:
            try:
                self._ticker.close()
            except Exception:
                pass
        log.info("KiteTicker stopped")

    # ------------------------------------------------------------------
    # Internal KiteTicker callbacks
    # ------------------------------------------------------------------
    def _handle_connect(self, ws, response) -> None:
        log.info("WebSocket connected")
        with self._lock:
            subs = dict(self._subscriptions)
        # Group by mode and re-subscribe everything
        by_mode: dict[str, list[int]] = {}
        for token, mode in subs.items():
            by_mode.setdefault(mode, []).append(token)
        for mode, tokens in by_mode.items():
            ws.subscribe(tokens)
            ws.set_mode(mode, tokens)
            log.debug(f"Subscribed {len(tokens)} tokens in {mode} mode on connect")
        if self._connect_handler:
            self._connect_handler()

    def _handle_close(self, ws, code, reason) -> None:
        log.warning(f"WebSocket closed: code={code} reason={reason}")
        if self._close_handler:
            self._close_handler(code, reason)

    def _handle_error(self, ws, code, reason) -> None:
        log.error(f"WebSocket error: code={code} reason={reason}")
        if self._error_handler:
            self._error_handler(Exception(f"{code}: {reason}"))

    def _handle_reconnect(self, ws, attempts_count) -> None:
        log.info(f"WebSocket reconnecting (attempt {attempts_count})")

    def _handle_noreconnect(self, ws) -> None:
        log.error("WebSocket: max reconnects reached — no further reconnection attempts.")

    def _dispatch_ticks(self, ws, ticks: list[dict]) -> None:
        for handler in self._tick_handlers:
            try:
                handler(ticks)
            except Exception as e:
                log.exception(f"Tick handler raised: {e}")
