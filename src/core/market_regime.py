"""VIX-based market regime classifier.

Three regimes:
  TRENDING  — VIX in the normal zone (range_max ≤ VIX < volatile_min)
               Two-candle signals are accepted as-is.
  RANGE     — VIX below range_max (very calm market, low directional conviction)
               Entries blocked: scalp momentum is absent.
  VOLATILE  — VIX at or above volatile_min (premium explosion, stop-outs frequent)
               Entries blocked: risk-reward collapses.

If VIX data is unavailable (vix == 0.0) the filter is bypassed so the bot
never silently stops trading due to a missing data feed.

Config keys (config/settings.yaml under ``market_regime``):
  enabled:         true
  vix_range_max:   13.0   # VIX < this  → RANGE  (too calm)
  vix_volatile_min: 25.0  # VIX ≥ this  → VOLATILE (too wild)
"""
from __future__ import annotations

import threading
from enum import Enum

from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


class MarketRegime(str, Enum):
    TRENDING = "TRENDING"   # entries allowed
    RANGE    = "RANGE"      # entries blocked — market too calm
    VOLATILE = "VOLATILE"   # entries blocked — market too wild
    UNKNOWN  = "UNKNOWN"    # VIX feed not yet received


class MarketRegimeFilter:
    """Thread-safe VIX price store + regime classifier.

    Usage:
        regime_filter = MarketRegimeFilter()
        regime_filter.update_vix(price)          # called from ticker thread
        ok, reason = regime_filter.is_tradeable() # called from candle-close handler
    """

    def __init__(self) -> None:
        cfg = config.get("market_regime", {})
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.range_max: float = float(cfg.get("vix_range_max", 13.0))
        self.volatile_min: float = float(cfg.get("vix_volatile_min", 25.0))

        self._vix: float = 0.0
        self._lock = threading.Lock()

        log.info(
            f"MarketRegimeFilter: enabled={self.enabled} "
            f"range_max={self.range_max} volatile_min={self.volatile_min}"
        )

    # ------------------------------------------------------------------
    # VIX feed
    # ------------------------------------------------------------------
    def update_vix(self, vix: float) -> None:
        """Called on every India VIX tick. Thread-safe."""
        with self._lock:
            prev = self._vix
            self._vix = vix
        if prev == 0.0 and vix > 0:
            log.info(f"India VIX feed established: {vix:.2f}")
        regime = self.classify()
        if regime != MarketRegime.TRENDING:
            log.debug(f"VIX={vix:.2f} → regime={regime.value}")

    @property
    def vix(self) -> float:
        with self._lock:
            return self._vix

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------
    def classify(self) -> MarketRegime:
        """Classify the current VIX level into a market regime."""
        v = self.vix
        if v == 0.0:
            return MarketRegime.UNKNOWN
        if v >= self.volatile_min:
            return MarketRegime.VOLATILE
        if v < self.range_max:
            return MarketRegime.RANGE
        return MarketRegime.TRENDING

    # ------------------------------------------------------------------
    # Entry gate
    # ------------------------------------------------------------------
    def is_tradeable(self) -> tuple[bool, str]:
        """Return (allowed, reason_if_blocked).

        Returns (True, "") when:
          - filter is disabled, OR
          - VIX feed not yet received (fail-open), OR
          - regime is TRENDING
        """
        if not self.enabled:
            return True, ""

        v = self.vix
        if v == 0.0:
            # Feed not available — fail-open rather than silently blocking the bot
            return True, ""

        regime = self.classify()

        if regime == MarketRegime.VOLATILE:
            return False, f"VIX {v:.1f} ≥ {self.volatile_min} (VOLATILE — entries blocked)"
        if regime == MarketRegime.RANGE:
            return False, f"VIX {v:.1f} < {self.range_max} (RANGE — entries blocked)"

        return True, ""
