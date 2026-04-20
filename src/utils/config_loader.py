"""Configuration loader for settings and credentials."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


class Config:
    """Singleton config loader - loads settings.yaml and credentials.yaml."""

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
            # Credentials optional for paper mode
            self._credentials = {}

        self._project_root = project_root

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

    def is_paper_mode(self) -> bool:
        return self.get("mode.trading_mode", "paper").lower() == "paper"

    def reload(self) -> None:
        """Reload config from disk (useful for daily access_token updates)."""
        self._load()


# Convenience singleton
config = Config()
