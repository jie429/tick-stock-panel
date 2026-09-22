"""全市场竞价扫描 — 09:25 集合竞价终态快照 + 竞价量比 (可选插件协议)。

数据源: 路由到「实时行情」的 Provider 可选实现 ``get_market_auction_snapshot()``
(eltdx 走通达信分类行情 0x054b: 实测 5575 只 / 70 页 / 约 4 秒)。未实现该协议的
源给出明确的 source_unavailable, **不换源、不落盘半成品**。

指标口径 (前端「异动监控 → 竞价异动 → 全市场竞价扫描」卡片同口径):
- 开盘涨幅 ``open_pct``      = (今开 - 昨收) ÷ 昨收 (小数制)
- 竞价量比 ``ratio_volume``  = 今日竞价量 ÷ 上一可得快照日竞价量 (同为 09:25 口径)
- 竞价额比 ``ratio_amount``  = 今日竞价额 ÷ 上一可得快照日竞价额 (含跨日价差, 供对照)
- 竞价额占比 ``prev_amount_share`` = 今日竞价额 ÷ 昨日全天成交额 (本地 kline_daily,
  无历史快照时唯一可用的兜底口径; 竞价额与昨日全天额同量纲, 不受跨日价差影响)
- 封单额 ``seal_amount`` = 买一价 * 买一量(手) * 100; 内/外盘 = 当日主动卖/买量(手)

落盘: ``data/auction_scan/date=YYYY-MM-DD.parquet``, 每个交易日一份全市场快照。
- 竞价终态 (09:25) 后当天只写一次 (竞价字段当日起不可变), 显式 refresh 才重写;
  盘中会变的 amount/内外盘/封单额仅作落盘时点快照, 不参与任何比较
- 历史日文件不可变: 命中直接读盘, 不触发 Provider、不加载插件注册表
- 竞价量比基线 = 目标日之前最近一份快照; 无基线时 ratio 为 None (前端列显 "—"),
  历史自启用之日起积累 —— 竞价明细本身没有历史接口, 这是既有结论的落地方案
- 进程内 60 秒 TTL 缓存: 前端 60s 轮询不会把 TCP 源打穿

状态: ``ok`` | ``not_ready`` (交易日 09:25 前且无任何历史快照)
      | ``source_unavailable`` (实时行情源未实现该协议) | ``no_data`` (扫描失败且无历史)
"""

from __future__ import annotations

import contextlib
import logging
import re
import threading
import time as time_mod
from datetime import date as date_cls
from datetime import time as dt_time
from pathlib import Path
from typing import Any

import polars as pl

from app.market_time import cn_now

logger = logging.getLogger(__name__)

# 集合竞价终态 (北京时间 09:25): 此前上游 open_amount 仍为 0, 不具备可比性。
AUCTION_READY_TIME = dt_time(9, 25)

DEFAULT_MIN_OPEN_PCT = 5.0
DEFAULT_MIN_RATIO = 10.0

# 进程内当日扫描缓存 TTL: 只为挡住前端轮询, 不追求实时 (竞价字段当日固定)。
_SCAN_TTL_SECONDS = 60.0

_SCAN_DIR = "auction_scan"
_SNAPSHOT_RE = re.compile(r"^date=(\d{4}-\d{2}-\d{2})\.parquet$")

# 落盘列 = Provider 竞价快照行; 显式 schema 保证跨日文件可比较。
_SNAPSHOT_SCHEMA: dict[str, Any] = {
    "symbol": pl.String,
    "open_price": pl.Float64,
    "prev_close": pl.Float64,
    "open_pct": pl.Float64,
    "change_pct": pl.Float64,
    "last_price": pl.Float64,
    "auction_amount": pl.Float64,
    "auction_volume": pl.Float64,
    "bid1": pl.Float64,
    "ask1": pl.Float64,
    "bid_volume1": pl.Int64,
    "ask_volume1": pl.Int64,
    "seal_amount": pl.Float64,
    "inside_volume": pl.Float64,
    "outside_volume": pl.Float64,
    "amount": pl.Float64,
    "volume": pl.Float64,
    "timestamp": pl.Int64,
}

_ITEM_FIELDS = (
    "symbol", "open_price", "prev_close", "open_pct", "change_pct", "last_price",
    "auction_amount", "auction_volume", "bid1", "ask1", "bid_volume1", "ask_volume1",
    "seal_amount", "inside_volume", "outside_volume", "amount", "volume",
)

_cache_lock = threading.Lock()
_scan_cache: tuple[date_cls, float, list[dict]] | None = None


def reset_cache() -> None:
    """清空进程内扫描缓存 (测试与显式刷新用)。"""
    global _scan_cache
    with _cache_lock:
        _scan_cache = None


# ================================================================
# 快照落盘
# ================================================================

def _snapshot_path(data_dir: Path, d: date_cls) -> Path:
    return data_dir / _SCAN_DIR / f"date={d.isoformat()}.parquet"


def snapshot_days(data_dir: Path) -> list[date_cls]:
    """已落盘快照日期 (升序); 目录缺失返回空。"""
    root = data_dir / _SCAN_DIR
    out: list[date_cls] = []
    try:
        for path in root.iterdir():
            matched = _SNAPSHOT_RE.match(path.name)
            if matched is None or not path.is_file():
                continue
            with contextlib.suppress(ValueError):
                out.append(date_cls.fromisoformat(matched.group(1)))
    except OSError:
        return []
    return sorted(out)


def _read_snapshot(path: Path) -> list[dict]:
    try:
        df = pl.read_parquet(path)
    except (OSError, pl.exceptions.PolarsError) as exc:
        logger.warning("竞价快照读取失败 %s: %s", path, exc)
        return []
    if "symbol" not in df.columns:
        return []
    return df.to_dicts()


def _write_snapshot(path: Path, rows: list[dict]) -> None:
    """原子落盘 (临时文件 + replace), 避免半截文件被次日基线读到。"""
    frame = pl.DataFrame(
        [{key: row.get(key) for key in _SNAPSHOT_SCHEMA} for row in rows],
        schema=_SNAPSHOT_SCHEMA,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    frame.write_parquet(tmp)
    tmp.replace(path)


def _latest_snapshot(
    data_dir: Path, *, before: date_cls | None = None,
) -> tuple[date_cls | None, list[dict]]:
    """目标日之前最近的快照 (date, rows); 无则 (None, [])。"""
    days = [d for d in snapshot_days(data_dir) if before is None or d < before]
    if not days:
        return None, []
    latest = days[-1]
    return latest, _read_snapshot(_snapshot_path(data_dir, latest))


def _load_snapshot(
    data_dir: Path, d: date_cls,
) -> tuple[date_cls | None, list[dict]]:
    """指定日快照; 文件不存在返回 (None, [])。"""
    if d not in snapshot_days(data_dir):
        return None, []
    return d, _read_snapshot(_snapshot_path(data_dir, d))


# ================================================================
# 昨日全天成交额 (本地日K分区, 竞价额占比兜底口径)
# ================================================================

def _kline_days(data_dir: Path) -> list[date_cls]:
    root = data_dir / "kline_daily"
    out: list[date_cls] = []
    try:
        for path in root.iterdir():
            name = path.name
            if not name.startswith("date=") or not path.is_dir():
                continue
            with contextlib.suppress(ValueError):
                out.append(date_cls.fromisoformat(name[len("date="):]))
    except OSError:
        return []
    return sorted(out)


def _prev_day_amounts(data_dir: Path, d: date_cls) -> dict[str, float]:
    """d 之前最近一个日K分区的 {symbol: 全天成交额(元)}; 无分区返回空。"""
    earlier = [x for x in _kline_days(data_dir) if x < d]
    if not earlier:
        return {}
    root = data_dir / "kline_daily" / f"date={earlier[-1].isoformat()}"
    try:
        files = sorted(root.glob("*.parquet"))
        if not files:
            return {}
        df = pl.concat([pl.read_parquet(f, columns=["symbol", "amount"]) for f in files])
    except (OSError, pl.exceptions.PolarsError) as exc:
        logger.debug("昨日成交额读取失败 %s: %s", root, exc)
        return {}
    return {
        symbol: amount
        for symbol, amount in zip(
            df["symbol"].to_list(), df["amount"].to_list(), strict=False,
        )
        if symbol and amount
    }


# ================================================================
# 数据源
# ================================================================

def _resolve_provider() -> Any | None:
    """实时行情路由源是否实现了竞价扫描协议; 不建连、不加载未用到的插件。"""
    from app.data_providers import custom as custom_sources
    from app.services import preferences

    name = preferences.get_realtime_data_provider()
    if not name or name == "tickflow":
        return None
    try:
        if not custom_sources.is_custom_provider(name):
            return None
        provider = custom_sources.get_provider(name)
    except Exception as exc:  # 源解析失败按不可用处理
        logger.warning("竞价扫描数据源 %s 解析失败: %s", name, exc)
        return None
    if not callable(getattr(provider, "get_market_auction_snapshot", None)):
        return None
    return provider


def _scan_today(
    data_dir: Path, provider: Any, today: date_cls, *, refresh: bool,
) -> list[dict] | None:
    """当日快照: 内存 TTL 命中直接返回, 否则拉取 + 首次落盘。失败返回 None。"""
    global _scan_cache
    now_mono = time_mod.monotonic()
    with _cache_lock:
        cached = _scan_cache
    if (
        cached is not None
        and not refresh
        and cached[0] == today
        and now_mono - cached[1] < _SCAN_TTL_SECONDS
    ):
        return cached[2]

    rows = provider.get_market_auction_snapshot()
    if not rows:
        # None = 软失败 (保留上轮); [] = 竞价尚未成交。两者都让调用方走历史兜底。
        return None

    stored = _load_snapshot(data_dir, today)[1]
    if refresh or not stored:
        try:
            _write_snapshot(_snapshot_path(data_dir, today), rows)
        except (OSError, pl.exceptions.PolarsError) as exc:
            logger.warning("竞价快照落盘失败 %s: %s", today, exc)
    with _cache_lock:
        _scan_cache = (today, now_mono, rows)
    return rows


# ================================================================
# 指标与筛选
# ================================================================

def _ratio(value: Any, base: Any) -> float | None:
    """倍率; 分母缺失/为 0 返回 None (不伪造成 0, 前端显 "—")。"""
    if value is None or base is None:
        return None
    try:
        numerator = float(value)
        denominator = float(base)
    except (TypeError, ValueError):
        return None
    if denominator == 0:
        return None
    return numerator / denominator


def _round(value: Any, digits: int) -> Any:
    if value is None or isinstance(value, bool):
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _compose_items(
    rows: list[dict],
    *,
    baseline: dict[str, dict],
    prev_amounts: dict[str, float],
    min_open_pct: float,
    min_ratio: float,
    limit: int,
) -> tuple[list[dict], int, int]:
    """全市场快照 → 命中行 + (高开数, 命中数)。

    有基线时按「高开 ≥ 阈值 且 竞价量比 ≥ 阈值」筛选 (卡片既定口径); 无基线
    (首日) 时竞价量比不可得, 退化为「高开 ≥ 阈值」按竞价额降序, 不假装能筛量比。
    """
    prepared: list[dict] = []
    for row in rows:
        symbol = row.get("symbol")
        open_pct = row.get("open_pct")
        if not symbol or open_pct is None:
            continue
        open_pct = float(open_pct)
        if open_pct * 100.0 < min_open_pct:
            continue
        base = baseline.get(symbol) or {}
        # name 始终在键内 (无维表时为 None), 保持前端类型稳定
        item: dict[str, Any] = {"name": None, **{key: row.get(key) for key in _ITEM_FIELDS}}
        item["open_pct"] = _round(open_pct, 6)
        item["change_pct"] = _round(row.get("change_pct"), 6)
        item["ratio_volume"] = _round(
            _ratio(row.get("auction_volume"), base.get("auction_volume")), 4,
        )
        item["ratio_amount"] = _round(
            _ratio(row.get("auction_amount"), base.get("auction_amount")), 4,
        )
        item["prev_amount_share"] = _round(
            _ratio(row.get("auction_amount"), prev_amounts.get(symbol)), 6,
        )
        prepared.append(item)

    high_open = len(prepared)
    if baseline:
        hits = [
            item for item in prepared
            if item["ratio_volume"] is not None and item["ratio_volume"] >= min_ratio
        ]
        hits.sort(
            key=lambda item: (item["ratio_volume"], item["auction_amount"] or 0.0),
            reverse=True,
        )
    else:
        hits = list(prepared)
        hits.sort(key=lambda item: item["auction_amount"] or 0.0, reverse=True)
    return hits[:limit], high_open, len(hits)


def _attach_names(items: list[dict], repo: Any | None) -> list[dict]:
    """补名称 (维表缺失保持 None, 不拿代码冒充名称)。"""
    if repo is None or not items:
        return items
    getter = getattr(repo, "get_name_map", None)
    if not callable(getter):
        return items
    try:
        names = getter([item["symbol"] for item in items]) or {}
    except Exception as exc:  # 名称是展示增强, 失败不影响扫描结果
        logger.debug("竞价扫描名称补全失败: %s", exc)
        return items
    return [{**item, "name": names.get(item["symbol"])} for item in items]


# ================================================================
# 入口
# ================================================================

def _base_payload(state: str, message: str | None = None) -> dict[str, Any]:
    return {
        "state": state,
        "message": message,
        "trade_date": None,
        "scanned_at": None,
        "total": 0,
        "baseline_date": None,
        "history_days": 0,
        "ratio_ready": False,
        "thresholds": {
            "min_open_pct": DEFAULT_MIN_OPEN_PCT,
            "min_ratio": DEFAULT_MIN_RATIO,
        },
        "counts": {"scanned": 0, "high_open": 0, "hits": 0},
        "items": [],
    }


def get_auction_scan(
    data_dir: Path,
    repo: Any | None = None,
    *,
    min_open_pct: float = DEFAULT_MIN_OPEN_PCT,
    min_ratio: float = DEFAULT_MIN_RATIO,
    limit: int = 200,
    refresh: bool = False,
    now: Any | None = None,
    provider: Any | None = None,
) -> dict[str, Any]:
    """全市场竞价扫描容器。

    now / provider 仅为测试与显式注入保留, 生产调用走北京时间与实时行情路由源。
    """
    from app.services import trading_day

    now = now or cn_now()
    today = now.date()
    session = trading_day.is_trading_day(now)
    # 只有确定休市 (周末/节假日日历) 才不扫; 探针未知时按数据说话 (空结果自然兜底)。
    ready = session is not False and now.time() >= AUCTION_READY_TIME

    rows: list[dict] = []
    trade_date: date_cls | None = None
    source_state: str | None = None

    if ready:
        source = provider if provider is not None else _resolve_provider()
        if source is None:
            source_state = "source_unavailable"
        else:
            fetched = _scan_today(data_dir, source, today, refresh=refresh)
            if fetched:
                rows, trade_date = fetched, today
            else:
                source_state = "no_data"

    if not rows:
        # 兜底: 上一可得快照 (竞价未结束/源不可用/扫描失败时仍给出最近一期)
        trade_date, rows = _latest_snapshot(data_dir)

    if not rows:
        if source_state == "source_unavailable":
            return _base_payload(
                "source_unavailable",
                "实时行情源未实现全市场竞价扫描 (需可选协议 get_market_auction_snapshot)",
            )
        if source_state == "no_data":
            return _base_payload("no_data", "全市场竞价快照拉取失败或当日无竞价成交")
        if session is not False:
            return _base_payload(
                "not_ready", f"集合竞价需在 {AUCTION_READY_TIME.strftime('%H:%M')} 后采集",
            )
        return _base_payload("no_data", "尚无任何竞价快照 (交易日 09:25 后自动采集)")

    baseline_date, baseline_rows = _latest_snapshot(data_dir, before=trade_date)
    baseline = {row["symbol"]: row for row in baseline_rows if row.get("symbol")}
    prev_amounts = _prev_day_amounts(data_dir, trade_date)
    items, high_open, hits = _compose_items(
        rows,
        baseline=baseline,
        prev_amounts=prev_amounts,
        min_open_pct=min_open_pct,
        min_ratio=min_ratio,
        limit=limit,
    )
    return {
        "state": "ok",
        "message": None,
        "trade_date": trade_date.isoformat() if trade_date else None,
        "scanned_at": int(time_mod.time() * 1000),
        "total": len(rows),
        "baseline_date": baseline_date.isoformat() if baseline_date else None,
        "history_days": len(snapshot_days(data_dir)),
        "ratio_ready": bool(baseline),
        "thresholds": {"min_open_pct": min_open_pct, "min_ratio": min_ratio},
        "counts": {"scanned": len(rows), "high_open": high_open, "hits": hits},
        "items": _attach_names(items, repo),
    }
