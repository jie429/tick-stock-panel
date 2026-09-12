from __future__ import annotations

import math
import re
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from app.custom.dragon_quant.models import DragonCandidate, DragonScanInput, MinuteCurve
from app.market_time import CN_TZ
from app.price_limits import price_limit_pct
from app.services import kline_sync
from app.services.ext_data import ExtConfig, ExtConfigStore

_DIMENSION_SEP = re.compile(r"[;,\uFF0C\u3001|/]+")
_MARKET_INDEX_SYMBOL = "000001.SH"


class DragonDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class DragonScanOptions:
    top_industries: int = 5
    lagging_industries: int = 20
    industry_level: int = 2
    min_industry_members: int = 3
    result_limit: int = 25
    absorption_days: int = 10
    auto_fetch_minute: bool = True
    industry_member_limit: int = 10


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _industry_field(config: ExtConfig) -> str | None:
    for field in config.fields:
        if any(word in f"{field.name} {field.label}".lower() for word in ("行业", "industry", "sector")):
            return field.name
    return None


def _config_files(data_dir: Path, config: ExtConfig) -> list[Path]:
    root = data_dir / "ext_data" / config.id
    if config.mode == "timeseries":
        return sorted((root / "timeseries").rglob("*.parquet"))
    path = root / "part.parquet"
    return [path] if path.exists() else []


def _industry_value(raw: Any, level: int) -> str | None:
    if raw is None:
        return None
    for value in _DIMENSION_SEP.split(str(raw)):
        clean = value.strip()
        if not clean or clean.casefold() in {"nan", "none", "null"}:
            continue
        parts = [part.strip() for part in clean.split("-") if part.strip()]
        if not parts:
            continue
        return parts[min(max(level, 1), len(parts)) - 1]
    return None


def load_industry_map(data_dir: Path, level: int = 2) -> tuple[dict[str, tuple[str, ...]], str | None]:
    store = ExtConfigStore(data_dir)
    mapping: dict[str, set[str]] = {}
    source: str | None = None
    for config in store.load_all():
        field = _industry_field(config)
        if not field:
            continue
        files = _config_files(data_dir, config)
        if not files:
            continue
        try:
            frame = pl.read_parquet(files, hive_partitioning=True)
        except (OSError, pl.exceptions.PolarsError):
            continue
        if frame.is_empty() or field not in frame.columns:
            continue
        if config.mode == "timeseries" and "date" in frame.columns:
            latest = frame["date"].max()
            frame = frame.filter(pl.col("date") == latest)
        symbol_col = next(
            (name for name in ("symbol", "股票代码", "代码", "code") if name in frame.columns),
            None,
        )
        if symbol_col is None:
            continue
        for row in frame.select([symbol_col, field]).iter_rows(named=True):
            symbol = str(row.get(symbol_col) or "").strip().upper()
            if not symbol:
                continue
            if "." not in symbol and len(symbol) == 6 and symbol.isdigit():
                symbol = f"{symbol}.SH" if symbol.startswith("6") else f"{symbol}.SZ"
            industry = _industry_value(row.get(field), level)
            if industry:
                mapping.setdefault(symbol, set()).add(industry)
        if mapping:
            source = f"{config.id}.{field}"
            break
    return {symbol: tuple(sorted(values)) for symbol, values in mapping.items()}, source


def _limit_price(previous_close: float, symbol: str, trade_date: date, name: str) -> float:
    pct = price_limit_pct(symbol, trade_date, is_risk_warning="ST" in name.upper())
    cents = math.floor(previous_close * 100.0 + 0.5)
    numerator = round((1.0 + pct) * 100.0)
    return ((cents * numerator + 50) // 100) / 100.0


def _prepare_daily(repo, as_of: date, lookback_days: int = 60) -> pl.DataFrame:
    start = as_of - timedelta(days=max(lookback_days * 2, 90))
    frame = repo.get_enriched_range(start, as_of)
    if frame is None or frame.is_empty():
        symbols = list(repo.get_name_map())
        frame = repo.get_daily_batch(symbols, start, as_of) if symbols else pl.DataFrame()
        latest, latest_date = repo.get_enriched_latest()
        if (
            latest is not None
            and not latest.is_empty()
            and latest_date is not None
            and start <= latest_date <= as_of
        ):
            if frame is None or frame.is_empty():
                frame = latest
            else:
                frame = pl.concat(
                    [
                        frame.filter(pl.col("date") != latest_date),
                        latest,
                    ],
                    how="diagonal_relaxed",
                )
    if frame is None or frame.is_empty():
        raise DragonDataError("日线 enriched 数据不可用, 请先完成日线同步和指标计算")
    required = {"symbol", "date", "open", "high", "low", "close", "volume", "amount"}
    missing = required - set(frame.columns)
    if missing:
        raise DragonDataError(f"日线 enriched 缺少字段: {', '.join(sorted(missing))}")
    frame = frame.sort(["symbol", "date"])
    raw_close = "raw_close" if "raw_close" in frame.columns else "close"
    names = repo.get_name_map(frame["symbol"].unique().to_list())
    if "name" not in frame.columns:
        if names:
            name_frame = pl.DataFrame({
                "symbol": list(names),
                "name": list(names.values()),
            })
            frame = frame.join(name_frame, on="symbol", how="left").with_columns(
                pl.col("name").fill_null(pl.col("symbol"))
            )
        else:
            frame = frame.with_columns(pl.col("symbol").alias("name"))
    frame = frame.with_columns([
        pl.col(raw_close).shift(1).over("symbol").alias("prev_raw_close"),
        pl.col("volume").shift(1).over("symbol").alias("prev_volume"),
    ])
    if "change_pct" not in frame.columns:
        frame = frame.with_columns(
            pl.when(pl.col("prev_raw_close") > 0)
            .then(pl.col(raw_close) / pl.col("prev_raw_close") - 1.0)
            .otherwise(None)
            .alias("change_pct")
        )
    return frame


def _curve_rows(frame: pl.DataFrame) -> dict[str, MinuteCurve]:
    output: dict[str, MinuteCurve] = {}
    if frame.is_empty():
        return output
    for symbol_frame in frame.partition_by("symbol", maintain_order=True):
        symbol = str(symbol_frame["symbol"][0])
        output[symbol] = {
            row["datetime"]: _finite(row["gain"])
            for row in symbol_frame.select(["datetime", "gain"]).iter_rows(named=True)
        }
    return output


def _industry_curves(
    minute: pl.DataFrame,
    industries: dict[str, tuple[str, ...]],
    selected: set[str],
    *,
    bucket: str | None = None,
) -> dict[str, MinuteCurve]:
    pairs = [
        {"symbol": symbol, "industry": industry}
        for symbol, values in industries.items()
        for industry in values
        if industry in selected
    ]
    if not pairs or minute.is_empty():
        return {}
    joined = minute.join(pl.DataFrame(pairs), on="symbol", how="inner")
    time_column = "datetime"
    if bucket:
        joined = joined.with_columns(pl.col("datetime").dt.truncate(bucket).alias("bucket"))
        time_column = "bucket"
    grouped = joined.group_by(["industry", time_column]).agg(pl.col("gain").mean()).sort(
        ["industry", time_column]
    )
    result: dict[str, MinuteCurve] = {}
    for group in grouped.partition_by("industry", maintain_order=True):
        industry = str(group["industry"][0])
        result[industry] = {
            row[time_column]: _finite(row["gain"])
            for row in group.select([time_column, "gain"]).iter_rows(named=True)
        }
    return result


def _open_count(frame: pl.DataFrame, limit_price: float) -> int | None:
    if frame.is_empty() or limit_price <= 0:
        return None
    sealed = False
    count = 0
    threshold = limit_price * 0.999
    for row in frame.select(["high", "close"]).iter_rows(named=True):
        touched = _finite(row["high"]) >= threshold
        below = _finite(row["close"]) < threshold
        if touched and not below:
            sealed = True
        elif sealed and below:
            count += 1
            sealed = False
    return count


def _five_day_return(rows: pl.DataFrame) -> float:
    if rows.height < 6:
        return 0.0
    close_col = "raw_close" if "raw_close" in rows.columns else "close"
    start = _finite(rows[close_col][-6])
    end = _finite(rows[close_col][-1])
    return (end / start - 1.0) * 100.0 if start > 0 else 0.0


def _is_source_candidate(row: dict, as_of: date) -> tuple[bool, float]:
    symbol = str(row.get("symbol") or "")
    name = str(row.get("name") or "")
    if "ST" in name.upper() or symbol.endswith(".BJ") or symbol.startswith(("300", "301", "688", "689")):
        return False, 0.0
    previous = _finite(row.get("prev_raw_close"))
    raw_close = _finite(row.get("raw_close") or row.get("close"))
    if previous <= 0 or raw_close <= 0:
        return False, 0.0
    limit_price = _limit_price(previous, symbol, as_of, name)
    return raw_close >= limit_price * 0.999, limit_price


def _minute_window(day: date) -> tuple[datetime, datetime]:
    return (
        datetime(day.year, day.month, day.day, 9, 25, tzinfo=CN_TZ),
        datetime(day.year, day.month, day.day, 15, 5, tzinfo=CN_TZ),
    )


def _covered_symbols(frame: pl.DataFrame) -> set[str]:
    if frame.is_empty() or "symbol" not in frame.columns:
        return set()
    return {str(value) for value in frame["symbol"].unique().to_list()}


def _merge_minute(*frames: pl.DataFrame) -> pl.DataFrame:
    available = [frame for frame in frames if frame is not None and not frame.is_empty()]
    if not available:
        return pl.DataFrame()
    return (
        pl.concat(available, how="diagonal_relaxed")
        .unique(subset=["symbol", "datetime"], keep="last")
        .sort(["symbol", "datetime"])
    )


def _load_minute_on_demand(
    repo,
    symbols: list[str],
    day: date,
    *,
    asset_type: str,
    auto_fetch: bool,
) -> tuple[pl.DataFrame, list[str], list[str]]:
    requested = list(dict.fromkeys(symbols))
    if not requested:
        return pl.DataFrame(), [], []
    local = (
        repo.get_minute_by_dates(requested, [day], asset_type=asset_type)
        if asset_type in {"stock", "etf"}
        else pl.DataFrame()
    )
    missing = [symbol for symbol in requested if symbol not in _covered_symbols(local)]
    if not auto_fetch or not missing:
        return local, [], []
    start_time, end_time = _minute_window(day)
    failed: list[str] = []
    try:
        fetched = kline_sync.sync_minute_batch(
            missing,
            start_time=start_time,
            end_time=end_time,
            asset_type=asset_type,
            raise_on_source_error=True,
            failed_out=failed,
        )
    except Exception as exc:
        return local, [], [str(exc)]
    if not fetched.is_empty() and "datetime" in fetched.columns:
        fetched = fetched.filter(pl.col("datetime").dt.date() == day)
    fetched_symbols = sorted(_covered_symbols(fetched))
    return _merge_minute(local, fetched), fetched_symbols, failed


def _ranked_industry_samples(
    industries: set[str],
    members: dict[str, list[str]],
    current_rows: dict[str, dict],
    candidates_by_industry: dict[str, set[str]],
    limit: int,
) -> dict[str, tuple[str, ...]]:
    samples: dict[str, tuple[str, ...]] = {}
    for industry in industries:
        candidates = candidates_by_industry.get(industry, set())
        ranked = sorted(
            members.get(industry, []),
            key=lambda symbol: _finite(current_rows.get(symbol, {}).get("amount")),
            reverse=True,
        )
        ordered = list(candidates) + [symbol for symbol in ranked if symbol not in candidates]
        sample_limit = max(max(1, limit), len(candidates))
        samples[industry] = tuple(ordered[:sample_limit])
    return samples


def _market_index_previous_close(repo, as_of: date) -> float:
    frame = repo.get_index_daily(
        _MARKET_INDEX_SYMBOL,
        as_of - timedelta(days=14),
        as_of,
        columns=["date", "close", "raw_close", "prev_close"],
    )
    if frame is None or frame.is_empty():
        return 0.0
    current = frame.filter(pl.col("date") == as_of)
    if not current.is_empty() and "prev_close" in current.columns:
        previous = _finite(current["prev_close"][0])
        if previous > 0:
            return previous
    before = frame.filter(pl.col("date") < as_of).sort("date")
    if before.is_empty():
        return 0.0
    close_col = "raw_close" if "raw_close" in before.columns else "close"
    return _finite(before[close_col][-1])


def _load_depth_snapshot(repo, depth_service, as_of: date) -> tuple[dict, bool, bool, dict | None]:
    """读取封单快照, 仅对最新 enriched 交易日尝试实时补拉。"""
    if depth_service is None:
        return {}, False, False, None

    def read_snapshot() -> tuple[dict, bool]:
        try:
            return (
                depth_service.get_sealed_map(as_of, is_down=False),
                bool(depth_service.is_sealed_ready(as_of)),
            )
        except Exception:
            return {}, False

    sealed_map, depth_ready = read_snapshot()
    if depth_ready:
        return sealed_map, True, False, None

    latest_date = None
    with suppress(Exception):
        _latest, latest_date = repo.get_enriched_latest()
    if latest_date != as_of:
        return sealed_map, False, False, None

    try:
        raw_result = depth_service.run_once()
        fetch_result = dict(raw_result) if isinstance(raw_result, dict) else {
            "ok": False,
            "count": 0,
            "msg": "五档盘口补拉返回格式异常",
        }
    except Exception as exc:
        fetch_result = {"ok": False, "count": 0, "msg": f"五档盘口补拉失败: {exc}"}
    sealed_map, depth_ready = read_snapshot()
    return sealed_map, depth_ready, True, fetch_result


def build_scan_input(
    repo,
    depth_service,
    data_dir: Path,
    as_of: date,
    options: DragonScanOptions,
) -> DragonScanInput:
    daily = _prepare_daily(repo, as_of, max(60, options.absorption_days + 10))
    current = daily.filter(pl.col("date") == as_of)
    if current.is_empty():
        raise DragonDataError(f"{as_of} 没有日线 enriched 数据")
    industry_map, industry_source = load_industry_map(data_dir, options.industry_level)
    if not industry_map:
        raise DragonDataError("行业映射不可用, 请先在行业分析页获取扩展行业数据")
    current_rows = {str(row["symbol"]): row for row in current.iter_rows(named=True)}
    members: dict[str, list[str]] = {}
    for symbol, values in industry_map.items():
        if symbol not in current_rows:
            continue
        for industry in values:
            members.setdefault(industry, []).append(symbol)
    industry_change = {
        industry: sum(_finite(current_rows[symbol].get("change_pct")) for symbol in symbols) / len(symbols)
        for industry, symbols in members.items()
        if len(symbols) >= options.min_industry_members
    }
    if not industry_change:
        raise DragonDataError("行业映射与当日日线没有足够交集")
    ranked = sorted(industry_change, key=industry_change.get, reverse=True)
    leading = ranked[: options.top_industries]
    lagging = sorted(industry_change, key=industry_change.get)[: options.lagging_industries]
    selected = set(leading) | set(lagging)

    candidate_rows: list[tuple[dict, str, float]] = []
    leading_rank = {industry: index for index, industry in enumerate(leading)}
    for symbol, row in current_rows.items():
        hit, limit_price = _is_source_candidate(row, as_of)
        industries = [value for value in industry_map.get(symbol, ()) if value in leading_rank]
        if hit and industries:
            primary = min(industries, key=leading_rank.get)
            candidate_rows.append((row, primary, limit_price))
    if not candidate_rows:
        return DragonScanInput(
            as_of=as_of,
            candidates=(),
            industry_members={key: tuple(value) for key, value in members.items()},
            industry_change_pct=industry_change,
            stock_change_pct={key: _finite(value.get("change_pct")) for key, value in current_rows.items()},
            industry_curves={},
            market_curve={},
            data_quality={
                "complete": True,
                "missing": [],
                "critical_missing": [],
                "optional_missing": [],
                "adaptations": ["行业指数分钟线使用成分股等权涨幅聚合"],
                "industry_source": industry_source,
                "leading_industries": [
                    {"name": industry, "change_pct": round(industry_change[industry] * 100.0, 2)}
                    for industry in leading
                ],
                "lagging_industries": [
                    {"name": industry, "change_pct": round(industry_change[industry] * 100.0, 2)}
                    for industry in lagging
                ],
                "minute_rows": 0,
            },
        )

    candidate_symbols = {str(row["symbol"]) for row, _primary, _limit in candidate_rows}
    candidate_industries = {primary for _row, primary, _limit in candidate_rows}
    candidates_by_industry: dict[str, set[str]] = {}
    for row, primary, _limit in candidate_rows:
        candidates_by_industry.setdefault(primary, set()).add(str(row["symbol"]))
    industry_samples = _ranked_industry_samples(
        candidate_industries,
        members,
        current_rows,
        candidates_by_industry,
        options.industry_member_limit,
    )
    minute_symbols = sorted({symbol for values in industry_samples.values() for symbol in values})
    minute, fetched_symbols, fetch_errors = _load_minute_on_demand(
        repo,
        minute_symbols,
        as_of,
        asset_type="stock",
        auto_fetch=options.auto_fetch_minute,
    )
    critical_missing: list[str] = []
    optional_missing: list[str] = []
    if minute.is_empty():
        minute_with_gain = pl.DataFrame()
    else:
        previous = current.select(["symbol", "prev_raw_close"])
        minute_with_gain = (
            minute.join(previous, on="symbol", how="inner")
            .filter(pl.col("prev_raw_close") > 0)
            .with_columns((pl.col("close") / pl.col("prev_raw_close") - 1.0).alias("gain"))
            .sort(["symbol", "datetime"])
        )
    stock_curves = _curve_rows(minute_with_gain)
    industry_curves = _industry_curves(minute_with_gain, industry_map, candidate_industries)
    if not candidate_symbols.issubset(stock_curves):
        critical_missing.append("candidate_minute")
    industry_minute_coverage = {
        industry: sum(symbol in stock_curves for symbol in symbols)
        for industry, symbols in industry_samples.items()
    }
    if any(
        industry_minute_coverage.get(industry, 0)
        < min(options.min_industry_members, len(industry_samples.get(industry, ())))
        for industry in candidate_industries
    ):
        critical_missing.append("leading_industry_minute")

    index_minute, fetched_index_symbols, index_fetch_errors = _load_minute_on_demand(
        repo,
        [_MARKET_INDEX_SYMBOL],
        as_of,
        asset_type="index",
        auto_fetch=options.auto_fetch_minute,
    )
    market_curve: MinuteCurve = {}
    index_previous_close = _market_index_previous_close(repo, as_of)
    if not index_minute.is_empty() and index_previous_close > 0:
        market_curve = {
            row["datetime"]: _finite(row["close"]) / index_previous_close - 1.0
            for row in index_minute.select(["datetime", "close"]).sort("datetime").iter_rows(named=True)
        }
    if not market_curve:
        critical_missing.append("market_index_minute")

    sealed_map, depth_ready, depth_fetch_attempted, depth_fetch_result = _load_depth_snapshot(
        repo, depth_service, as_of,
    )
    if not depth_ready:
        optional_missing.append("depth5_sealed_snapshot")
    elif any(
        symbol not in sealed_map
        or not sealed_map[symbol].get("ready")
        or sealed_map[symbol].get("vol") is None
        for symbol in candidate_symbols
    ):
        optional_missing.append("candidate_depth5_snapshot")

    trading_dates = daily["date"].unique().sort().to_list()
    history_dates = [value for value in trading_dates if value <= as_of][-options.absorption_days:]
    history_samples = _ranked_industry_samples(
        selected,
        members,
        current_rows,
        candidates_by_industry,
        options.industry_member_limit,
    )
    relevant_symbols = sorted({symbol for values in history_samples.values() for symbol in values})
    historical_minute = repo.get_minute_by_dates(relevant_symbols, history_dates, asset_type="stock")
    industry_history: dict[date, dict[str, list[float]]] = {}
    if historical_minute.is_empty():
        optional_missing.append("absorption_minute_history")
    else:
        previous_rows = daily.select(["symbol", "date", "prev_raw_close"])
        historical = historical_minute.with_columns(
            pl.col("datetime").dt.date().alias("date")
        ).join(previous_rows, on=["symbol", "date"], how="inner").filter(
            pl.col("prev_raw_close") > 0
        ).with_columns(
            (pl.col("close") / pl.col("prev_raw_close") - 1.0).alias("gain")
        )
        curves_5m = _industry_curves(historical, industry_map, selected, bucket="5m")
        for industry, curve in curves_5m.items():
            per_day: dict[date, list[tuple[datetime, float]]] = {}
            for point, gain in curve.items():
                per_day.setdefault(point.date(), []).append((point, gain))
            for day, values in per_day.items():
                industry_history.setdefault(day, {})[industry] = [
                    gain for _point, gain in sorted(values)
                ]
        covered_history = {
            industry
            for curves in industry_history.values()
            for industry, values in curves.items()
            if values
        }
        if not candidate_industries.issubset(covered_history):
            optional_missing.append("candidate_absorption_history")

    daily_by_symbol = {
        str(group["symbol"][0]): group
        for group in daily.partition_by("symbol", maintain_order=True)
    }
    candidate_minute_frames = {
        str(group["symbol"][0]): group
        for group in minute.partition_by("symbol", maintain_order=True)
    } if not minute.is_empty() else {}
    candidates: list[DragonCandidate] = []
    for row, primary, limit_price in candidate_rows:
        symbol = str(row["symbol"])
        history = daily_by_symbol.get(symbol, pl.DataFrame()).filter(pl.col("date") <= as_of)
        board_count = int(row.get("consecutive_limit_ups") or 0)
        depth = sealed_map.get(symbol, {})
        sealed_volume = (
            _finite(depth.get("vol"))
            if depth_ready and depth.get("ready") and depth.get("vol") is not None
            else None
        )
        candidates.append(DragonCandidate(
            symbol=symbol,
            name=str(row.get("name") or symbol),
            industry=primary,
            industries=industry_map.get(symbol, ()),
            board_count=board_count,
            five_day_return_pct=_five_day_return(history),
            change_pct=_finite(row.get("change_pct")),
            turnover_rate_pct=_finite(row.get("turnover_rate")),
            volume_lots=_finite(row.get("volume")),
            amount_yuan=_finite(row.get("amount")),
            sealed_volume_lots=sealed_volume,
            prev_raw_close=_finite(row.get("prev_raw_close")),
            limit_up_price=limit_price,
            minute_curve=stock_curves.get(symbol, {}),
            open_count=_open_count(candidate_minute_frames.get(symbol, pl.DataFrame()), limit_price),
        ))
    return DragonScanInput(
        as_of=as_of,
        candidates=tuple(candidates),
        industry_members={key: tuple(value) for key, value in members.items()},
        industry_change_pct=industry_change,
        stock_change_pct={key: _finite(value.get("change_pct")) for key, value in current_rows.items()},
        industry_curves=industry_curves,
        market_curve=market_curve,
        industry_history=industry_history,
        data_quality={
            "complete": not critical_missing,
            "missing": list(dict.fromkeys(critical_missing + optional_missing)),
            "critical_missing": list(dict.fromkeys(critical_missing)),
            "optional_missing": list(dict.fromkeys(optional_missing)),
            "adaptations": [
                "行业排名使用行业成分股当日涨跌幅等权平均",
                f"行业分钟线按需使用每行业最多 {options.industry_member_limit} 只高成交额成分股等权聚合",
                "市场分钟基准使用上证指数 000001.SH 相对昨收涨幅",
                "五档封单缺失时按源策略使用 60 分中性值, 不全局否决真龙认证",
                "资金承接分钟历史缺失时按源策略回退 50 分, 不参与硬门槛否决",
            ],
            "industry_source": industry_source,
            "leading_industries": [
                {"name": industry, "change_pct": round(industry_change[industry] * 100.0, 2)}
                for industry in leading
            ],
            "lagging_industries": [
                {"name": industry, "change_pct": round(industry_change[industry] * 100.0, 2)}
                for industry in lagging
            ],
            "minute_rows": minute.height,
            "minute_symbols_requested": len(minute_symbols),
            "minute_symbols_fetched": len(fetched_symbols),
            "industry_minute_coverage": industry_minute_coverage,
            "market_source": _MARKET_INDEX_SYMBOL,
            "market_minute_rows": index_minute.height,
            "market_minute_fetched": bool(fetched_index_symbols),
            "depth_ready": depth_ready,
            "depth_fetch_attempted": depth_fetch_attempted,
            "depth_fetch_result": depth_fetch_result,
            "fetch_errors": fetch_errors + index_fetch_errors,
        },
    )


def load_account_market_data(
    repo,
    symbols: list[str],
    start: date,
    end: date,
) -> tuple[list[date], dict[str, list[dict]], dict[tuple[str, date], list[dict]]]:
    daily = _prepare_daily(repo, end, max((end - start).days + 30, 60)).filter(
        (pl.col("date") >= start - timedelta(days=30)) & (pl.col("date") <= end)
    )
    if symbols:
        daily = daily.filter(pl.col("symbol").is_in(symbols))
    trading_days = [value for value in daily["date"].unique().sort().to_list() if start <= value <= end]
    if not symbols:
        return trading_days, {}, {}
    daily_by_symbol: dict[str, list[dict]] = {}
    for group in daily.partition_by("symbol", maintain_order=True):
        rows = group.to_dicts()
        for row in rows:
            row["prev_close"] = row.get("prev_raw_close")
            previous = _finite(row.get("prev_raw_close"))
            row["limit_up_price"] = _limit_price(
                previous,
                str(row.get("symbol") or group["symbol"][0]),
                row["date"],
                str(row.get("name") or ""),
            ) if previous > 0 else None
        daily_by_symbol[str(group["symbol"][0])] = rows
    minute = repo.get_minute_by_dates(symbols, trading_days, asset_type="stock") if symbols else pl.DataFrame()
    minute_by_symbol_date: dict[tuple[str, date], list[dict]] = {}
    if not minute.is_empty():
        aggregations = [
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
        ]
        for column in ("volume", "amount"):
            if column in minute.columns:
                aggregations.append(pl.col(column).sum().alias(column))
        minute = (
            minute.sort(["symbol", "datetime"])
            .with_columns([
                pl.col("datetime").dt.date().alias("date"),
                pl.col("datetime")
                .dt.offset_by("-1m")
                .dt.truncate("5m")
                .dt.offset_by("5m")
                .alias("bucket"),
            ])
            .group_by(["symbol", "date", "bucket"], maintain_order=True)
            .agg(aggregations)
            .rename({"bucket": "datetime"})
            .sort(["symbol", "date", "datetime"])
        )
        for group in minute.partition_by(["symbol", "date"], maintain_order=True):
            minute_by_symbol_date[(str(group["symbol"][0]), group["date"][0])] = group.to_dicts()
    return trading_days, daily_by_symbol, minute_by_symbol_date
