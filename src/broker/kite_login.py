"""Daily Kite access token generator.
Usage: python -m src.broker.kite_login

The access_token from Kite Connect changes every trading day.
This utility:
1. Prints the login URL
2. Waits for you to paste the request_token from the redirect
3. Exchanges it for an access_token
4. Updates config/credentials.yaml

You only need to run this once per trading day.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


def main() -> int:
    try:
        from kiteconnect import KiteConnect
    except ImportError:
        print("kiteconnect not installed. Run: pip install kiteconnect")
        return 1

    creds = config.credentials.get("kite", {})
    api_key = creds.get("api_key", "")
    api_secret = creds.get("api_secret", "")

    if not api_key or not api_secret:
        print(
            "Please set kite.api_key and kite.api_secret in config/credentials.yaml first."
        )
        return 1

    kite = KiteConnect(api_key=api_key)
    login_url = kite.login_url()

    print("=" * 60)
    print("STEP 1: Open the following URL in your browser and log in:")
    print()
    print(login_url)
    print()
    print("STEP 2: After login you'll be redirected to a URL like:")
    print("        https://your-redirect.example?action=login&request_token=XXXX&status=success")
    print()
    print("STEP 3: Copy the request_token from the URL and paste it below.")
    print("=" * 60)

    request_token = input("\nEnter request_token: ").strip()
    if not request_token:
        print("No token provided. Aborting.")
        return 1

    try:
        data = kite.generate_session(request_token, api_secret=api_secret)
    except Exception as e:
        print(f"Session generation failed: {e}")
        return 1

    access_token = data.get("access_token", "")
    user_id = data.get("user_id", "")

    if not access_token:
        print("No access_token in response.")
        return 1

    # Save to credentials file
    creds_path = config.project_root / "config" / "credentials.yaml"
    current = {}
    if creds_path.exists():
        with open(creds_path, "r", encoding="utf-8") as f:
            current = yaml.safe_load(f) or {}

    current.setdefault("kite", {})
    current["kite"]["access_token"] = access_token
    current["kite"]["user_id"] = user_id
    current["kite"]["api_key"] = api_key
    current["kite"]["api_secret"] = api_secret

    with open(creds_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(current, f, default_flow_style=False)

    print()
    print(f"Access token saved for user: {user_id}")
    print(f"File updated: {creds_path}")
    print("You can now run the bot with: python main.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
