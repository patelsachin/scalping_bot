# Siva Scalping Bot

An automated options scalping bot for **BankNifty** based on **Sivakumar Jayachandran's Two Candle Theory**.
Supports paper trading and live execution via Zerodha Kite Connect.

---

## Features

- **Two Candle Theory** entry signals with 6 indicator confirmation (VWAP, SuperTrend, RSI, PSAR, Volume, OI)
- **Signal strength grading**: STRONG / MEDIUM (weak signals ignored)
- **Smart capital allocation** — capital deployed based on signal strength
- **Dual trailing SL** — points-based trail + SuperTrend flip, whichever triggers first
- **Gap protection** — immediate square off on overnight gap-SL breach
- **Daily loss cap** — halts trading at 1% of daily budget
- **15-min SuperTrend re-entry filter** — only re-enters if higher timeframe agrees
- **EOD square off** — closes all positions 10 minutes before market close
- **Paper / live toggle** — single config change
- **Rich terminal dashboard** — green/red P&L, live refresh
- **CSV trade log** + rotating system log
- **Multi-trade support** — capital-limited, not count-limited

---

## Project Structure

```
scalping_bot/
├── main.py                              # Entry point
├── requirements.txt                     # Python dependencies
├── config/
│   ├── settings.yaml                    # All trading rules & parameters
│   ├── credentials.yaml.template        # Template for API credentials
│   └── credentials.yaml                 # (created by you, gitignored)
├── logs/
│   ├── system.log                       # Rotating system log
│   └── trades.csv                       # One row per closed trade
├── src/
│   ├── core/
│   │   ├── engine.py                    # Main orchestrator loop
│   │   ├── models.py                    # Trade, Signal, Candle dataclasses
│   │   └── state.py                     # Thread-safe shared state
│   ├── indicators/
│   │   └── technical.py                 # VWAP, ST, RSI, PSAR, Volume
│   ├── strategy/
│   │   └── two_candle.py                # Two Candle Theory engine
│   ├── risk/
│   │   └── risk_manager.py              # Sizing, SL, trailing, daily loss
│   ├── broker/
│   │   ├── base.py                      # Abstract broker interface
│   │   ├── kite_broker.py               # Live Zerodha Kite
│   │   ├── paper_broker.py              # Paper mode (live prices, sim orders)
│   │   └── kite_login.py                # Daily access_token generator
│   ├── dashboard/
│   │   └── terminal_dashboard.py        # Rich terminal UI
│   └── utils/
│       ├── config_loader.py             # YAML loader
│       ├── logger.py                    # Central logging
│       ├── market_calendar.py           # IST, market hours
│       └── trade_logger.py              # CSV trade writer
└── tests/
    ├── test_indicators.py
    ├── test_strategy.py
    └── test_risk.py
```

---

## Setup (Windows)

### 1. Python

Install **Python 3.11** (recommended) or 3.10 / 3.12.
Get it from [python.org](https://www.python.org/downloads/) — make sure to tick **"Add Python to PATH"** during install.

Verify:
```cmd
python --version
```

### 2. Get the project into VS Code

Unzip the project, then open it in VS Code:
```cmd
cd C:\path\to\scalping_bot
code .
```

### 3. Create virtual environment

In the VS Code terminal (Ctrl + `):
```cmd
python -m venv venv
venv\Scripts\activate
```

You should see `(venv)` in your terminal prompt.

### 4. Install dependencies

```cmd
pip install -r requirements.txt
```

### 5. Configure credentials

Copy the template:
```cmd
copy config\credentials.yaml.template config\credentials.yaml
```

Edit `config/credentials.yaml` and add:
- `api_key` — from [developers.kite.trade](https://developers.kite.trade)
- `api_secret` — from the same dashboard
- Leave `access_token` empty for now (generated next step)

### 6. Generate daily access token

Zerodha's access token **expires every morning**. Run this once per trading day:

```cmd
python -m src.broker.kite_login
```

Follow the prompts:
1. Open the printed URL in your browser
2. Login to Zerodha
3. Copy the `request_token` from the redirected URL
4. Paste it back into the terminal

The new token is saved automatically to `credentials.yaml`.

### 7. Configure the bot

Open `config/settings.yaml` and edit:

- `mode.trading_mode` — `paper` or `live`
- `capital.daily_budget` — your capital for the day (default ₹100,000)
- `instrument.max_lots_per_trade` — default 3

All other values match Siva's system and should normally not need editing.

### 8. Run

```cmd
python main.py
```

You'll see the live terminal dashboard update every second.
Press **Ctrl + C** to shut down — all open positions will be squared off automatically.

---

## Running Modes

### Paper mode (default, safe)

```yaml
# config/settings.yaml
mode:
  trading_mode: paper
```

Uses live market prices from Kite but simulates all orders. No real money at risk.

### Live mode

```yaml
mode:
  trading_mode: live
```

**Real orders will be placed.** Make sure paper mode worked for a few days first.

### Command line flags

```cmd
python main.py                 # bot + dashboard (default)
python main.py --no-dashboard  # bot only, log output to console
python main.py --dashboard-only # dashboard only (view current state)
python main.py --poll 3        # tighter polling (default 5 sec)
```

---

## Daily Workflow

Every trading day:

1. **Before 9:15 AM IST** — run `python -m src.broker.kite_login` to refresh access token
2. **At 9:15 AM** — run `python main.py`
3. **Monitor** the dashboard through the day
4. **At 3:20 PM** the bot squares off all positions automatically
5. **After 3:30 PM** — stop the bot with Ctrl + C; review `logs/trades.csv`

---

## Trading Rules (Siva's system)

### Entry (Two Candle Theory)

**LONG** (buy ATM CE):
- 2 consecutive green candles on 3-min chart
- Volume ≥ 50,000 per candle
- RSI between 50 and 80 (period = 14)
- Price above VWAP
- SuperTrend (10, 2) = green
- PSAR (0.02, 0.2) dots below

**SHORT** (buy ATM PE):
- 2 consecutive red candles
- Volume ≥ 50,000 per candle
- RSI between 20 and 50
- Price below VWAP
- SuperTrend = red
- PSAR dots above

### Signal strength
- **STRONG** — all 6 conditions met, allocates 30% of daily budget
- **MEDIUM** — 5 of 6 met, allocates 20%
- **WEAK** — ignored (per your requirement)

### Risk
- Max risk per trade: 20 points on premium
- Target: 10 points (1:1 RR)
- Trailing SL: activates at 5 pts profit, trails by 5 pts
- Exit also on SuperTrend flip
- Daily loss cap: 1% of daily budget
- Cooldown after SL: 2 candles (6 min)
- Re-entry requires 15-min SuperTrend agreement

---

## Files Generated

- `logs/system.log` — all bot activity, rotated at 10MB
- `logs/trades.csv` — one row per closed trade with all fields required

CSV columns:
```
trade_id, symbol, underlying, trade_type, signal_strength,
entry_time, entry_price, quantity, lots, stop_loss, target, trailing_sl,
capital_used, exit_time, exit_price, exit_quantity, exit_reason,
pnl, pnl_points, status, is_paper
```

---

## Safety Notes

- **Start with paper mode** for at least a week before going live
- **No leverage** is used — max exposure = daily_budget
- Bot does **NOT carry positions overnight**
- All open trades are squared off at 15:20 IST (configurable)
- Gap-down/gap-up immediate square-off if SL is breached at open
- Daily loss cap halts trading automatically

---

## Troubleshooting

**"kiteconnect not installed"**
→ Run `pip install -r requirements.txt` inside the venv

**"Kite authentication failed"**
→ Your access token has expired. Re-run `python -m src.broker.kite_login`

**"Not a trading day"**
→ Bot exits on weekends. NSE holiday calendar integration is a TODO.

**Dashboard shows old/zero data**
→ Check `logs/system.log` for broker errors. Kite has a 3 req/sec rate limit.

---

## Future Enhancements

- Telegram notifications (scaffolding already in `settings.yaml`)
- NSE holiday calendar integration
- Open Interest signal filtering (OI Pulse style)
- Pyramiding / averaging rules from Siva's advanced course
- Web dashboard
- Backtesting module

---

## Disclaimer

This is educational software. Options trading involves substantial risk.
Test thoroughly in paper mode. Past performance does not guarantee future results.
The authors are not SEBI-registered advisors.
