"""Terminal dashboard using rich. Renders live bot state at a configurable refresh rate."""
from __future__ import annotations

from datetime import datetime

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.core.models import TradeStatus
from src.core.state import state
from src.utils.config_loader import config
from src.utils.trade_stats import cached_stats


def _fmt_money(val: float) -> Text:
    color = "green" if val > 0 else ("red" if val < 0 else "white")
    sign = "+" if val > 0 else ""
    return Text(f"{sign}₹{val:,.2f}", style=color)


def _fmt_pnl(val: float) -> Text:
    return _fmt_money(val)


def _header_panel(snap: dict) -> Panel:
    mode = snap["mode"].upper()
    mode_color = "yellow" if mode == "PAPER" else "bold red"
    conn_color = "green" if snap["connected"] else "red"
    conn_text = "CONNECTED" if snap["connected"] else "DISCONNECTED"

    status_line = Text()
    status_line.append("Mode: ", style="dim")
    status_line.append(mode, style=mode_color)
    status_line.append("  |  ", style="dim")
    status_line.append("Broker: ", style="dim")
    status_line.append(conn_text, style=conn_color)
    status_line.append("  |  ", style="dim")
    status_line.append(f"BANKNIFTY: ₹{snap['underlying_ltp']:,.2f}", style="cyan")
    status_line.append(f"  |  ATM: {int(snap['atm_strike'])}", style="cyan")
    status_line.append(f"  |  Strategy:", style="dim")
    status_line.append(f"  {config.get('strategy.type', 'scalping').upper()}", style="white")

    vix = snap.get("vix", 0.0)
    regime = snap.get("market_regime", "UNKNOWN")
    if vix > 0:
        regime_color = {"TRENDING": "green", "RANGE": "yellow", "VOLATILE": "bold red"}.get(regime, "dim")
        status_line.append("  |  VIX: ", style="dim")
        status_line.append(f"{vix:.1f} ", style="white")
        status_line.append(regime, style=regime_color)

    if snap["halted"]:
        status_line.append("  |  ", style="dim")
        status_line.append(f"HALTED: {snap['halt_reason']}", style="bold red")

    now = datetime.now().strftime("%H:%M:%S")
    title = Text.assemble(("Siva Scalping Bot", "bold magenta"), ("  ", ""), (now, "dim"))
    return Panel(Align.center(status_line), title=title, border_style="magenta")


def _capital_panel(snap: dict) -> Panel:
    table = Table.grid(padding=(0, 2), expand=True)
    table.add_column(justify="left", style="dim", ratio=1)
    table.add_column(justify="right", ratio=1)

    table.add_row("Daily Budget", Text(f"₹{snap['daily_budget']:,.2f}", style="white"))
    table.add_row("Capital Deployed", Text(f"₹{snap['capital_deployed']:,.2f}", style="yellow"))
    table.add_row("Capital Balance", Text(f"₹{snap['capital_balance']:,.2f}", style="cyan"))
    table.add_row("", "")
    table.add_row("Realised P&L", _fmt_pnl(snap["realised_pnl"]))
    table.add_row("Unrealised P&L", _fmt_pnl(snap["unrealised_pnl"]))
    table.add_row("Total P&L", _fmt_pnl(snap["total_pnl"]))

    return Panel(table, title="Capital & P&L", border_style="cyan")


def _open_trades_panel(snap: dict) -> Panel:
    table = Table(expand=True, show_lines=False, header_style="bold cyan")
    table.add_column("ID", style="dim", width=8)
    table.add_column("Symbol", style="white")
    table.add_column("Side", justify="center", width=6)
    table.add_column("Qty", justify="right", width=6)
    table.add_column("Entry", justify="right", width=9)
    table.add_column("SL", justify="right", width=9)
    table.add_column("Tgt", justify="right", width=9)
    table.add_column("Trail", justify="right", width=9)
    table.add_column("P&L", justify="right", width=12)
    table.add_column("Time", style="dim", width=8)

    trades = snap["open_trades"]
    if not trades:
        empty = Text("No open positions", style="dim italic", justify="center")
        return Panel(Align.center(empty, vertical="middle"), title="Open Trades", border_style="yellow", height=8)

    for t in trades:
        side_style = "green" if t.trade_type.value == "LONG" else "red"
        side_mark = "▲ LONG" if t.trade_type.value == "LONG" else "▼ SHORT"
        pnl_txt = _fmt_pnl(t.pnl)
        entry_time = t.entry_time.strftime("%H:%M:%S") if t.entry_time else ""
        trail_val = t.trailing_sl if t.trailing_sl else t.stop_loss

        table.add_row(
            t.trade_id,
            t.symbol,
            Text(side_mark, style=side_style),
            str(t.quantity),
            f"₹{t.entry_price:.2f}",
            f"₹{t.stop_loss:.2f}",
            f"₹{t.target:.2f}",
            f"₹{trail_val:.2f}",
            pnl_txt,
            entry_time,
        )

    return Panel(table, title=f"Open Trades ({len(trades)})", border_style="yellow")


def _closed_trades_panel(snap: dict) -> Panel:
    max_rows = int(config.get("dashboard.max_trades_display", 20))

    table = Table(expand=True, show_lines=False, header_style="bold cyan")
    table.add_column("ID", style="dim", width=8)
    table.add_column("Symbol", style="white")
    table.add_column("Side", justify="center", width=6)
    table.add_column("Qty", justify="right", width=5)
    table.add_column("Entry", justify="right", width=9)
    table.add_column("Exit", justify="right", width=9)
    table.add_column("Reason", justify="left", width=14)
    table.add_column("P&L", justify="right", width=12)
    table.add_column("Entry T", style="dim", width=8)
    table.add_column("Exit T", style="dim", width=8)

    trades = snap["closed_trades"][-max_rows:]
    if not trades:
        empty = Text("No closed trades yet", style="dim italic", justify="center")
        return Panel(Align.center(empty, vertical="middle"), title="Closed Trades", border_style="green", height=8)

    for t in reversed(trades):  # most recent first
        side_style = "green" if t.trade_type.value == "LONG" else "red"
        side_mark = "▲ L" if t.trade_type.value == "LONG" else "▼ S"
        entry_t = t.entry_time.strftime("%H:%M:%S") if t.entry_time else ""
        exit_t = t.exit_time.strftime("%H:%M:%S") if t.exit_time else ""
        reason = t.exit_reason.value if t.exit_reason else ""

        table.add_row(
            t.trade_id,
            t.symbol,
            Text(side_mark, style=side_style),
            str(t.exit_quantity or t.quantity),
            f"₹{t.entry_price:.2f}",
            f"₹{t.exit_price:.2f}",
            reason,
            _fmt_pnl(t.pnl),
            entry_t,
            exit_t,
        )

    return Panel(table, title=f"Closed Trades (last {len(trades)})", border_style="green")


def _stats_panel(snap: dict) -> Panel:
    """Day-stats panel: win rate, expectancy, avg hold, drawdown."""
    stats = cached_stats.get(snap["closed_trades"])

    table = Table.grid(padding=(0, 2), expand=True)
    table.add_column(justify="left", style="dim", ratio=1)
    table.add_column(justify="right", ratio=1)

    win_color = "green" if stats.win_rate_pct >= 50 else "red"
    exp_color = "green" if stats.expectancy >= 0 else "red"

    table.add_row("Trades", Text(str(stats.total_trades), style="white"))
    table.add_row(
        "Win Rate",
        Text(f"{stats.win_rate_pct:.1f}%", style=win_color),
    )
    table.add_row(
        "Expectancy",
        Text(f"₹{stats.expectancy:,.2f}", style=exp_color),
    )
    table.add_row("Avg Win", _fmt_money(stats.avg_win))
    table.add_row("Avg Loss", _fmt_money(stats.avg_loss))
    table.add_row(
        "Avg Hold",
        Text(f"{stats.avg_hold_min:.1f} min", style="white"),
    )
    table.add_row(
        "Max DD",
        Text(f"₹{stats.max_drawdown:,.2f}", style="red" if stats.max_drawdown < 0 else "dim"),
    )

    return Panel(table, title="Day Stats", border_style="blue")


def _footer_panel(snap: dict) -> Panel:
    info_bits: list[str] = []
    if snap["last_candle_time"]:
        info_bits.append(f"Last candle: {snap['last_candle_time'].strftime('%H:%M:%S')}")
    if snap["last_signal_time"]:
        info_bits.append(f"Last signal: {snap['last_signal_time'].strftime('%H:%M:%S')}")
    if snap["last_error"]:
        info_bits.append(f"[red]Error: {snap['last_error']}[/red]")

    body = "  |  ".join(info_bits) if info_bits else "Waiting for market activity..."
    hint = (
        "[bold yellow][Q][/bold yellow] [dim]Quit & square off all[/dim]"
        "  |  "
        "[bold red][Ctrl+K][/bold red] [dim]Kill switch (suspend trading)[/dim]"
    )
    text = Text.from_markup(f"{body}\n{hint}", justify="center")
    return Panel(text, border_style="dim")


def build_layout() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=3),
    )
    layout["left"].split(
        Layout(name="capital", ratio=3),
        Layout(name="stats", ratio=2),
    )
    layout["right"].split(
        Layout(name="open_trades", ratio=1),
        Layout(name="closed_trades", ratio=2),
    )
    return layout


def render(layout: Layout) -> Layout:
    snap = state.snapshot()
    layout["header"].update(_header_panel(snap))
    layout["capital"].update(_capital_panel(snap))
    layout["stats"].update(_stats_panel(snap))
    layout["open_trades"].update(_open_trades_panel(snap))
    layout["closed_trades"].update(_closed_trades_panel(snap))
    layout["footer"].update(_footer_panel(snap))
    return layout


def run_dashboard() -> None:
    """Blocking call: run the live dashboard. Press Q to quit gracefully."""
    import msvcrt
    import time

    from src.core.state import state as _state

    refresh = int(config.get("dashboard.refresh_interval_sec", 1))
    console = Console()
    layout = build_layout()

    with Live(render(layout), console=console, refresh_per_second=max(1, 1 // max(refresh, 1) if refresh else 4), screen=True) as live:
        try:
            while True:
                live.update(render(layout))

                # Non-blocking key check — works on Windows even inside Rich's alternate screen
                if msvcrt.kbhit():
                    key = msvcrt.getch()
                    if key in (b"q", b"Q", b"\x03"):   # Q, q, or Ctrl+C
                        _state.shutdown_requested = True
                        break
                    elif key == b"\x0b":                # Ctrl+K — kill switch
                        _state.kill_switch_active = True
                        # Don't break — app stays running; engine will square off + halt

                time.sleep(max(refresh, 1))
        except KeyboardInterrupt:
            _state.shutdown_requested = True
