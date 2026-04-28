"""Configuration loader for settings and credentials."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


class Config:
    """Singleton config loader - loads settings.yaml and credentials.yaml.

    After loading, the active market's config block (markets.india or markets.us)
    is deep-merged into the top-level settings so that existing code using
    config.get("instrument.symbol"), config.get("session.market_open"), etc.
    continues to work unchanged regardless of which market is active.
    """

    _instance: "Config | None" = None
    _settings: dict[str, Any] = {}
    _credentials: dict[str, Any] = {}

    def __new__(cls) -> "Config":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance

    def _load(self) -> None:
        """Load settings and credentials from YAML files."""
        project_root = Path(__file__).resolve().parents[2]

        settings_path = project_root / "config" / "settings.yaml"
        creds_path = project_root / "config" / "credentials.yaml"

        if not settings_path.exists():
            raise FileNotFoundError(f"Settings file not found: {settings_path}")

        with open(settings_path, "r", encoding="utf-8") as f:
            self._settings = yaml.safe_load(f) or {}

        if creds_path.exists():
            with open(creds_path, "r", encoding="utf-8") as f:
                self._credentials = yaml.safe_load(f) or {}
        else:
            self._credentials = {}

        self._project_root = project_root
        self._merge_active_market()

    def _merge_active_market(self) -> None:
        """Deep-merge the active market block into top-level settings.

        markets.india (or markets.us) keys override top-level defaults so
        that config.get("instrument.symbol"), config.get("session.market_open")
        etc. automatically return the right values for the selected market.
        """
        active = self._settings.get("active_market", "india")
        markets = self._settings.get("markets", {})
        market_cfg = markets.get(active, {})
        if not market_cfg:
            return

        for key, value in market_cfg.items():
            if isinstance(value, dict) and isinstance(self._settings.get(key), dict):
                # Deep merge: market overrides individual sub-keys
                self._settings[key] = {**self._settings.get(key, {}), **value}
            else:
                self._settings[key] = value

    @property
    def settings(self) -> dict[str, Any]:
        return self._settings

    @property
    def credentials(self) -> dict[str, Any]:
        return self._credentials

    @property
    def project_root(self) -> Path:
        return self._project_root

    def get(self, path: str, default: Any = None) -> Any:
        """Dot-path getter: config.get('capital.daily_budget')."""
        keys = path.split(".")
        node: Any = self._settings
        for key in keys:
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                return default
        return node

    def active_market(self) -> str:
        return self._settings.get("active_market", "india")

    def is_paper_mode(self) -> bool:
        return self.get("mode.trading_mode", "paper").lower() == "paper"

    def reload(self) -> None:
        """Reload config from disk (useful for daily access_token updates)."""
        self._load()


# Convenience singleton
config = Config()
