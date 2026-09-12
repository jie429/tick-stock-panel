from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass(frozen=True)
class DragonAccountConfig:
    initial_cash: float = 100_000.0
    candidate_top_n: int = 5
    candidate_lookback_days: int = 3
    max_positions: int = 5
    min_score: float = 50.0
    min_amount: float = 200_000_000.0
    min_turnover: float = 5.0
    strong_turnover_min: float = 8.0
    strong_turnover_max: float = 35.0
    max_open_gap: float = 7.0
    max_close_to_ma5: float = 12.0
    divergence_enabled: bool = True
    divergence_min_boards: int = 2
    divergence_confirm_bars: int = 6
    first_day_stop_loss_pct: float = -3.5
    stop_loss_pct: float = -5.0
    weak_close_tolerance_pct: float = 1.0
    breakeven_activate_pct: float = 6.0
    trailing_activate_pct: float = 8.0
    trailing_drawdown_pct: float = 3.5
    volume_spike_pct: float = 30.0
    buy_slippage: float = 0.002
    sell_slippage: float = 0.002
    commission_rate: float = 0.0003
    stamp_tax_rate: float = 0.0005
    lot_size: int = 100


def _row_map(rows: list[dict]) -> dict[date, dict]:
    return {row["date"]: row for row in rows if isinstance(row.get("date"), date)}


def _candidate_pool(day: date, scans: dict[date, list[dict]], config: DragonAccountConfig) -> list[dict]:
    previous = sorted((scan_day for scan_day in scans if scan_day < day), reverse=True)
    selected_days = previous[: max(1, config.candidate_lookback_days)]
    merged: dict[str, dict] = {}
    for scan_day in selected_days:
        for candidate in scans.get(scan_day, []):
            if not candidate.get("is_true_dragon"):
                continue
            symbol = str(candidate.get("symbol") or "")
            if not symbol:
                continue
            item = {**candidate, "candidate_date": scan_day.isoformat()}
            current = merged.get(symbol)
            item_key = (int(item.get("rank") or 999), -float(item.get("composite_score") or 0.0))
            current_key = (
                int(current.get("rank") or 999),
                -float(current.get("composite_score") or 0.0),
            ) if current else None
            if current_key is None or item_key < current_key:
                merged[symbol] = item
    return sorted(
        merged.values(),
        key=lambda item: (int(item.get("rank") or 999), -float(item.get("composite_score") or 0.0)),
    )[: max(1, config.candidate_top_n)]


def _open_gap(row: dict, previous: dict | None) -> float | None:
    prev_close = float((previous or {}).get("raw_close") or (previous or {}).get("close") or 0.0)
    open_price = float(row.get("open") or 0.0)
    if prev_close <= 0 or open_price <= 0:
        return None
    return (open_price / prev_close - 1.0) * 100.0


def _is_one_word(row: dict) -> bool:
    high = float(row.get("high") or 0.0)
    low = float(row.get("low") or 0.0)
    return bool(row.get("signal_limit_up")) and high > 0 and abs(high - low) < 1e-9


def _is_open_locked_limit_up(row: dict) -> bool:
    open_price = float(row.get("open") or 0.0)
    limit_up = float(row.get("limit_up_price") or 0.0)
    return open_price > 0 and limit_up > 0 and open_price >= limit_up * 0.999


def _divergence_buy(
    candidate: dict,
    row: dict,
    previous_rows: list[dict],
    minute_rows: list[dict],
    config: DragonAccountConfig,
) -> dict | None:
    if not config.divergence_enabled or not minute_rows:
        return None
    boards = 0
    volumes: list[float] = []
    for previous in reversed(previous_rows):
        if _is_one_word(previous):
            boards += 1
            volumes.append(float(previous.get("volume") or 0.0))
        else:
            break
    if boards < config.divergence_min_boards:
        return None
    ordered_volumes = list(reversed(volumes))
    if any(ordered_volumes[index] > ordered_volumes[index - 1] for index in range(1, len(ordered_volumes))):
        return None
    previous = previous_rows[-1] if previous_rows else None
    prev_close = float((previous or {}).get("raw_close") or (previous or {}).get("close") or 0.0)
    if prev_close <= 0:
        return None
    limit_up = round(prev_close * 1.10 + 1e-9, 2)
    open_price = float(row.get("open") or 0.0)
    if open_price <= 0 or open_price >= limit_up * 0.998:
        return None
    window = sorted(minute_rows, key=lambda item: item.get("datetime") or datetime.min)[
        : config.divergence_confirm_bars
    ]
    if not window:
        return None
    if any(float(item.get("high") or 0.0) >= limit_up * 0.999 for item in window):
        return {
            "reason_code": "buy_divergence_first_break",
            "reason_text": f"连续{boards}个缩量一字板后首次断板, 盘中回封涨停",
            "priority": 400,
            "price": limit_up,
        }
    window_low = min(float(item.get("low") or 0.0) for item in window)
    first_open = float(window[0].get("open") or 0.0)
    last_close = float(window[-1].get("close") or 0.0)
    if window_low < prev_close or last_close < first_open:
        return None
    return {
        "reason_code": "buy_divergence_first_break",
        "reason_text": f"连续{boards}个缩量一字板后首次断板, 前30分钟承接有效",
        "priority": 400,
        "price": last_close,
    }


def _buy_signal(
    candidate: dict,
    row: dict,
    previous_rows: list[dict],
    minute_rows: list[dict],
    config: DragonAccountConfig,
) -> dict | None:
    if float(candidate.get("composite_score") or 0.0) < config.min_score:
        return None
    divergence = _divergence_buy(candidate, row, previous_rows, minute_rows, config)
    if divergence:
        return divergence
    previous = previous_rows[-1] if previous_rows else None
    amount = float(candidate.get("amount_yuan") or (previous or {}).get("amount") or 0.0)
    turnover = float(
        candidate.get("turnover_rate_pct") or (previous or {}).get("turnover_rate") or 0.0
    )
    if amount < config.min_amount or turnover < config.min_turnover or _is_open_locked_limit_up(row):
        return None
    open_price = float(row.get("open") or 0.0)
    ma5 = float((previous or {}).get("ma5") or 0.0)
    gap = _open_gap(row, previous)
    if ma5 > 0 and open_price <= ma5 * 1.03 and (gap is None or -3.0 < gap <= config.max_open_gap):
        return {
            "reason_code": "buy_open_ma5_pullback",
            "reason_text": "开盘贴近上日 MA5, 具备回踩承接条件",
            "priority": 300,
            "price": open_price,
        }
    previous_high = float((previous or {}).get("high") or 0.0)
    distance_ma5 = (open_price / ma5 - 1.0) * 100.0 if ma5 > 0 else None
    if (
        previous_high > 0
        and open_price > previous_high
        and gap is not None
        and 0 <= gap <= min(5.5, config.max_open_gap)
        and distance_ma5 is not None
        and distance_ma5 <= config.max_close_to_ma5
        and config.strong_turnover_min <= turnover <= config.strong_turnover_max
        and amount >= 500_000_000.0
    ):
        return {
            "reason_code": "buy_open_turn_strong",
            "reason_text": "开盘突破前高, 竞价弱转强",
            "priority": 200,
            "price": open_price,
        }
    return None


def _breakeven_retrace_today(
    position: dict,
    minute_rows: list[dict],
    break_even: float,
    config: DragonAccountConfig,
) -> bool:
    activate = float(position["entry_price"]) * (1.0 + config.breakeven_activate_pct / 100.0)
    activated = False
    for bar in sorted(minute_rows, key=lambda item: item.get("datetime") or datetime.min):
        if activated and float(bar.get("low") or 0.0) <= break_even:
            return True
        if float(bar.get("high") or 0.0) >= activate:
            activated = True
    return False


def _high_open_exit(row: dict, minute_rows: list[dict]) -> dict | None:
    previous = float(row.get("prev_raw_close") or row.get("prev_close") or 0.0)
    open_price = float(row.get("open") or 0.0)
    if previous <= 0 or open_price <= 0 or not minute_rows:
        return None
    gap = (open_price / previous - 1.0) * 100.0
    if gap < 5.0:
        return None
    bars = 1 if gap >= 7.0 else 6
    window = sorted(minute_rows, key=lambda item: item.get("datetime") or datetime.min)[:bars]
    limit_up = float(row.get("limit_up_price") or 0.0)
    if not window or limit_up <= 0:
        return None
    if any(float(bar.get("high") or 0.0) >= limit_up * 0.999 for bar in window):
        return None
    minutes = 5 if bars == 1 else 30
    return {
        "reason_code": f"high_open_{7 if bars == 1 else 5}pct_no_limit_{minutes}m_clear",
        "reason_text": f"开盘高开{gap:.1f}%, {minutes}分钟内未涨停",
        "price": float(window[-1].get("close") or row.get("close") or 0.0),
        "fraction": 1.0,
    }


def _sell_signal(
    position: dict,
    row: dict,
    hold_days: int,
    minute_rows: list[dict],
    config: DragonAccountConfig,
) -> dict | None:
    entry = float(position["entry_price"])
    low_return = (float(row.get("low") or 0.0) / entry - 1.0) * 100.0
    close = float(row.get("close") or 0.0)
    previous_high_return = float(position.get("highest_return") or 0.0)
    applied_stop = config.first_day_stop_loss_pct if hold_days <= 1 else config.stop_loss_pct
    if low_return <= applied_stop:
        return {
            "reason_code": "first_day_stop_loss" if hold_days <= 1 else "hard_stop_loss",
            "reason_text": f"最低价触及 {applied_stop:.1f}% 止损",
            "price": entry * (1.0 + applied_stop / 100.0),
            "fraction": 1.0,
        }
    highest_price = float(position.get("highest_price") or entry)
    ma5 = float(row.get("ma5") or 0.0)
    retrace = (highest_price - close) / highest_price * 100.0 if highest_price > 0 else 0.0
    if previous_high_return >= config.trailing_activate_pct and (
        retrace >= config.trailing_drawdown_pct or (ma5 > 0 and close < ma5)
    ):
        return {
            "reason_code": "trailing_take_profit",
            "reason_text": "峰值浮盈达标后回撤, 触发移动止盈",
            "price": close,
            "fraction": 1.0,
        }
    break_even = entry * (1 + config.buy_slippage + config.commission_rate) / max(
        1 - config.sell_slippage - config.commission_rate - config.stamp_tax_rate,
        1e-9,
    )
    if (
        previous_high_return >= config.breakeven_activate_pct
        and float(row.get("low") or 0.0) <= break_even
    ) or (
        previous_high_return < config.breakeven_activate_pct
        and _breakeven_retrace_today(position, minute_rows, break_even, config)
    ):
        return {
            "reason_code": "profit_back_to_cost_take_profit",
            "reason_text": "浮盈激活后回落至完整成本线",
            "price": break_even,
            "fraction": 1.0,
        }
    if hold_days == 1 and row.get("signal_limit_up"):
        return {
            "reason_code": "next_day_limit_up_half",
            "reason_text": "买入次日收盘涨停, 卖出半仓",
            "price": float(row.get("raw_close") or close),
            "fraction": 0.5,
        }
    high_open = _high_open_exit(row, minute_rows)
    if high_open:
        return high_open
    open_price = float(row.get("open") or 0.0)
    prev_close = float(row.get("prev_raw_close") or row.get("prev_close") or 0.0)
    weak = False
    if open_price > 0 and close < open_price:
        drop = (open_price - close) / open_price * 100.0
        weak = drop > config.weak_close_tolerance_pct or (ma5 > 0 and close < ma5) or (
            prev_close > 0 and close < prev_close
        )
    if weak:
        return {
            "reason_code": "next_day_close_below_open" if hold_days == 1 else "close_below_open_stop",
            "reason_text": "收盘走弱, 失守容忍带或均线",
            "price": close,
            "fraction": 1.0,
        }
    if ma5 > 0 and close < ma5:
        return {
            "reason_code": "break_intraday_ma_stop",
            "reason_text": "收盘跌破 MA5",
            "price": close,
            "fraction": 1.0,
        }
    previous_volume = float(row.get("prev_volume") or 0.0)
    volume = float(row.get("volume") or 0.0)
    volume_change = (volume / previous_volume - 1.0) * 100.0 if previous_volume > 0 else None
    if volume_change is not None and volume_change >= config.volume_spike_pct and not row.get("signal_limit_up"):
        return {
            "reason_code": "volume_spike_take_profit",
            "reason_text": "成交量显著放大且未涨停",
            "price": close,
            "fraction": 1.0,
        }
    return None


def run_account_backtest(
    *,
    trading_days: list[date],
    daily_by_symbol: dict[str, list[dict]],
    scans_by_date: dict[date, list[dict]],
    minute_by_symbol_date: dict[tuple[str, date], list[dict]],
    config: DragonAccountConfig,
) -> dict[str, Any]:
    cash = float(config.initial_cash)
    positions: dict[str, dict] = {}
    trades: list[dict] = []
    equity_curve: list[dict] = []
    peak_equity = cash
    maps = {symbol: _row_map(rows) for symbol, rows in daily_by_symbol.items()}
    ordered_rows = {
        symbol: sorted(rows, key=lambda row: row["date"])
        for symbol, rows in daily_by_symbol.items()
    }

    for day in trading_days:
        sold_today = False
        for symbol, position in list(positions.items()):
            if position["entry_date"] >= day:
                continue
            row = maps.get(symbol, {}).get(day)
            if row is None:
                continue
            hold_days = sum(1 for value in trading_days if position["entry_date"] < value <= day)
            signal = _sell_signal(
                position,
                row,
                hold_days,
                minute_by_symbol_date.get((symbol, day), []),
                config,
            )
            high_return = (float(row.get("high") or 0.0) / position["entry_price"] - 1.0) * 100.0
            position["highest_return"] = max(position["highest_return"], high_return)
            position["highest_price"] = max(position["highest_price"], float(row.get("high") or 0.0))
            if signal is None:
                continue
            quantity = position["quantity"]
            if signal["fraction"] < 1.0:
                half = int(quantity * signal["fraction"] / config.lot_size) * config.lot_size
                quantity = half if half > 0 else quantity
            execution = float(signal["price"]) * (1.0 - config.sell_slippage)
            amount = execution * quantity
            fee = amount * (config.commission_rate + config.stamp_tax_rate)
            cash += amount - fee
            position["quantity"] -= quantity
            position["cost"] *= position["quantity"] / (position["quantity"] + quantity)
            trades.append({
                "trade_date": day.isoformat(), "symbol": symbol, "name": position["name"],
                "side": "sell", "price": round(execution, 4), "quantity": quantity,
                "amount": round(amount, 2), "fee": round(fee, 2),
                "reason_code": signal["reason_code"], "reason_text": signal["reason_text"],
            })
            sold_today = True
            if position["quantity"] <= 0:
                positions.pop(symbol, None)

        if not sold_today and len(positions) < config.max_positions:
            candidates = _candidate_pool(day, scans_by_date, config)
            signals: list[tuple[int, dict, dict, list[dict]]] = []
            for candidate in candidates:
                symbol = candidate["symbol"]
                if symbol in positions:
                    continue
                row = maps.get(symbol, {}).get(day)
                if row is None:
                    continue
                history = [item for item in ordered_rows.get(symbol, []) if item["date"] < day]
                signal = _buy_signal(
                    candidate,
                    row,
                    history,
                    minute_by_symbol_date.get((symbol, day), []),
                    config,
                )
                if signal:
                    signals.append((int(signal["priority"]), candidate, signal, history))
            signals.sort(key=lambda item: (-item[0], int(item[1].get("rank") or 999)))
            for _priority, candidate, signal, _history in signals:
                if len(positions) >= config.max_positions:
                    break
                slots = max(config.max_positions - len(positions), 1)
                budget = cash / slots
                execution = float(signal["price"]) * (1.0 + config.buy_slippage)
                quantity = int(
                    budget / (execution * (1.0 + config.commission_rate)) / config.lot_size
                ) * config.lot_size
                if quantity <= 0:
                    continue
                amount = execution * quantity
                fee = amount * config.commission_rate
                if amount + fee > cash:
                    continue
                cash -= amount + fee
                symbol = candidate["symbol"]
                positions[symbol] = {
                    "symbol": symbol,
                    "name": candidate.get("name") or symbol,
                    "entry_date": day,
                    "entry_price": execution,
                    "quantity": quantity,
                    "cost": amount + fee,
                    "highest_return": 0.0,
                    "highest_price": execution,
                }
                trades.append({
                    "trade_date": day.isoformat(), "symbol": symbol,
                    "name": positions[symbol]["name"], "side": "buy",
                    "price": round(execution, 4), "quantity": quantity,
                    "amount": round(amount, 2), "fee": round(fee, 2),
                    "reason_code": signal["reason_code"], "reason_text": signal["reason_text"],
                    "candidate_date": candidate.get("candidate_date"),
                    "composite_score": candidate.get("composite_score"),
                })

        market_value = 0.0
        for symbol, position in positions.items():
            row = maps.get(symbol, {}).get(day)
            price = float((row or {}).get("close") or position["entry_price"])
            market_value += price * position["quantity"]
        equity = cash + market_value
        peak_equity = max(peak_equity, equity)
        equity_curve.append({
            "date": day.isoformat(),
            "cash": round(cash, 2),
            "market_value": round(market_value, 2),
            "equity": round(equity, 2),
            "return_pct": round((equity / config.initial_cash - 1.0) * 100.0, 4),
            "drawdown_pct": round((equity / peak_equity - 1.0) * 100.0, 4),
            "positions": len(positions),
        })

    final_equity = equity_curve[-1]["equity"] if equity_curve else config.initial_cash
    return {
        "config": config.__dict__,
        "trades": trades,
        "equity_curve": equity_curve,
        "open_positions": [
            {**position, "entry_date": position["entry_date"].isoformat()}
            for position in positions.values()
        ],
        "stats": {
            "initial_cash": config.initial_cash,
            "final_equity": final_equity,
            "total_return_pct": round((final_equity / config.initial_cash - 1.0) * 100.0, 4),
            "max_drawdown_pct": min(
                (point["drawdown_pct"] for point in equity_curve), default=0.0,
            ),
            "trade_count": len(trades),
        },
    }
