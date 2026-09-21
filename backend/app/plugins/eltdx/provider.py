"""基于 eltdx 的免费通达信行情 Provider。

eltdx 自身可作 MCP / HTTP 服务运行; 项目后端不依赖外部进程, 直接复用其
``TdxClient`` TCP 客户端。只声明当前契约能真实满足的数据集:

- ``daily``     股票/ETF/指数不复权原始日K, ``volume_lots`` 原生为手;
- ``adj_factor`` 由除权除息事件按交易所公式推导的单事件比值, 与 fuyao 同口径;
- ``minute``    按标的 1 分钟 OHLCV, 实测历史约 100 个交易日;
- ``realtime``  全市场 A 股 + ETF 快照 (TCP 报价单请求上限 80 只, 自行分批);
- ``depth5``    五档盘口, 缺档保留 None。

不声明 ``financial``(上游只有简版财务批量字段)与 ``full_minute``(按标的拉取,
承受不了盘中全市场分钟落盘)。
"""
from __future__ import annotations

import logging
import math
import re
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from app.data_providers.base import AssetType
from app.plugins.eltdx import bridge

logger = logging.getLogger(__name__)

_DATASETS = ("daily", "adj_factor", "minute", "realtime", "depth5")
_CN_TZ = ZoneInfo("Asia/Shanghai")
# 通达信 K 线单页上限 800 根; ``bars.get`` 的 start 是相对最新一根的偏移量。
_KLINE_PAGE_SIZE = 800
# 82 页 x 800 根覆盖 1990 年至今的日K; 分钟K 远小于此, 仅作死循环兜底。
_MAX_KLINE_PAGES = 82
# 报价协议单请求上限 80 只: 实测请求 200 只只回 80 只, 整批上千只会直接断流。
_QUOTE_BATCH_SIZE = 80
# 除权事件接口上游默认批量。
_CAPITAL_CHANGE_BATCH_SIZE = 75
# 推导除权因子需事件日前的原始收盘价, 为交易日/节假日留出自然日余量。
_PREV_CLOSE_BACKDAYS = 31
# 通达信资本变动记录中 category_raw == 1 为除权除息事件。
_EX_RIGHTS_CATEGORY = 1
# 指数 K 线成交量还原为"手"的倍数 (见 _volume_scale)。
_INDEX_VOLUME_SCALE = 100.0
# 代码表 category → 项目 asset_type。
_INSTRUMENT_CATEGORIES = {"stock": "a_share", "etf": "etf", "index": "index"}

_SYMBOL_RE = re.compile(r"^(?P<code>\d{6})\.(?P<exchange>[A-Z]{2})$")
_TDX_SYMBOL_RE = re.compile(r"^(?P<exchange>sh|sz|bj)(?P<code>\d{6})$", re.IGNORECASE)

_DAILY_SCHEMA = {
    "symbol": pl.String,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    # 项目内部 volume 是"手"; eltdx ``volume_lots`` 同为手, 必须原样透传。
    "volume": pl.Float64,
    "amount": pl.Float64,
}
_MINUTE_SCHEMA = {
    "symbol": pl.String,
    "datetime": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
}
_ADJ_FACTOR_SCHEMA = {
    "symbol": pl.String,
    "trade_date": pl.Date,
    "ex_factor": pl.Float64,
}


@dataclass
class _EltDxConfig:
    """让现有 custom loader 识别内置 Provider 的最小 config shim。"""

    name: str = "eltdx"
    display_name: str = "eltdx (通达信免费行情)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def availability() -> tuple[bool, str]:
    """供 plugin.yaml 的 check 使用。"""
    return bridge.availability()


def _to_tdx_symbol(symbol: str) -> str:
    """项目 ``600000.SH`` → 通达信 ``sh600000``, 不靠裸码猜交易所。"""
    text = str(symbol or "").strip().upper()
    matched = _SYMBOL_RE.fullmatch(text)
    if matched is None:
        raise ValueError(f"symbol 必须为带交易所后缀的六码代码(如 600000.SH), 得到: {symbol!r}")
    exchange = matched.group("exchange")
    if exchange not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"不支持的交易所后缀: {exchange}")
    return f"{exchange.lower()}{matched.group('code')}"


def _from_tdx_symbol(tdx_symbol: str) -> tuple[str, str, str]:
    """通达信 ``sh600000`` → 项目 ``600000.SH``、裸码、交易所。"""
    text = str(tdx_symbol or "").strip().lower()
    matched = _TDX_SYMBOL_RE.fullmatch(text)
    if matched is None:
        raise ValueError(f"无法解析通达信代码: {tdx_symbol!r}")
    code = matched.group("code")
    exchange = matched.group("exchange").upper()
    return f"{code}.{exchange}", code, exchange


def _beijing_naive(value: datetime) -> datetime:
    """把 eltdx 的上海时区 datetime 收口为项目分钟 K 的北京 naive 墙钟。"""
    if not isinstance(value, datetime):
        raise ValueError(f"K 线时间不是 datetime: {value!r}")
    if value.tzinfo is None:
        return value
    return value.astimezone(_CN_TZ).replace(tzinfo=None)


def _bar_time(bar: Any) -> datetime | None:
    value = getattr(bar, "time", None)
    if not isinstance(value, datetime):
        return None
    return _beijing_naive(value)


def _finite_float(value: Any) -> float | None:
    """将上游数值收口为有限 float; 缺失/异常值不能伪造成 0。"""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _level_numbers(levels: Any) -> list[int | None]:
    """盘口缺档用 None, 绝不能伪造成 0 (会误判为真封板)。

    eltdx 3.x 的 ``QuoteLevel.volume`` 实测单位为手; 快照只给一档, 完整五档由
    ``helpers.full_quotes`` 合并 0x0547 刷新流补齐, 补不齐的档位保持 None。
    """
    values: list[int | None] = []
    for level in list(levels or [])[:5]:
        value = _finite_float(getattr(level, "volume", None))
        values.append(int(value) if value is not None else None)
    return (values + [None] * 5)[:5]


def _scaled(value: Any, scale: float) -> float | None:
    number = _finite_float(value)
    return None if number is None else number * scale


def _volume_scale(asset_type: AssetType | str) -> float:
    """指数 K 线成交量口径修正系数。

    实测 000001.SH / 399001.SZ / 399006.SZ / 000016.SH 的 ``volume_lots`` 与当日
    快照 ``total_hand`` 恰好相差 100 倍; 同一天的成分股快照成交量之和与指数快照
    一致, 即指数 K 线的成交量按"手/100"给出。股票与 ETF 无此偏差, 原样透传。
    """
    return _INDEX_VOLUME_SCALE if str(asset_type).lower() == "index" else 1.0
class EltdxProvider:
    """免费通达信行情数据源。

    客户端按租约共享: 首次调用时单线程 connect, ``close()`` 后新调用会自行重建,
    避免插件重载与在途请求互相踩连接。
    """

    name = "eltdx"
    builtin = True
    # 通达信代码表能区分股票/ETF/指数, 且日K对三类都有独立记录布局。
    daily_asset_types = frozenset({"stock", "etf", "index"})
    instrument_asset_types = frozenset({"stock", "etf", "index"})
    # 分钟K按标的请求, 不能承接全市场分钟落盘。
    supports_minute_universe_sync = False
    # 2026-09 实测 1m 历史约 100 个交易日; 保守暴露为 90, 避免 UI 承诺深历史。
    minute_history_days = 90
    # TCP 源异常时返回明确空结果, 不因本机仍有 TickFlow 配置而暗中换源。
    fallback_to_tickflow_on_error = False
    # ``kline_sync`` 只向显式声明该能力的 Provider 传入失败标的出参。
    supports_daily_failure_reporting = True
    supports_minute_failure_reporting = True
    # 全市场行情要按代码表切成 80 只一批的 TCP 请求, 不能按普通插件的 1 秒下限轮询。
    realtime_min_interval = 30.0

    def __init__(self) -> None:
        self.config = _EltDxConfig()
        self._client: Any | None = None
        self._client_connected = False
        self._client_connecting = False
        self._client_retiring = False
        self._client_calls_inflight = 0
        self._retired_clients: list[Any] = []
        self._client_condition = threading.Condition(threading.RLock())
        self._realtime_tdx_symbols: tuple[str, ...] | None = None

    # ---- 客户端租约 ----
    def close(self) -> None:
        """停止接收新调用; 在途调用归还租约后再关闭其 TCP client。"""
        to_close: list[Any] = []
        with self._client_condition:
            self._client_retiring = True
            # 若第一次 connect 正在进行, 等待时会释放锁, 避免关闭半初始化 client。
            while self._client_connecting:
                self._client_condition.wait()
            if self._client is not None:
                client = self._client
                self._client = None
                self._client_connected = False
                if self._client_calls_inflight:
                    self._retired_clients.append(client)
                else:
                    to_close.append(client)
            if not self._client_calls_inflight and self._retired_clients:
                to_close.extend(self._retired_clients)
                self._retired_clients = []
        self._close_clients(to_close)

    @staticmethod
    def _close_clients(clients: list[Any]) -> None:
        for client in clients:
            with suppress(Exception):
                client.close()

    @contextmanager
    def _client_session(self):
        client = self._borrow_client()
        try:
            yield client
        finally:
            self._return_client()

    def _borrow_client(self):
        """取得已连接 client 的一次调用租约, 首次连接由单线程完成。"""
        while True:
            with self._client_condition:
                if self._client_retiring:
                    raise bridge.EltdxBridgeError("eltdx Provider 正在重载")
                if self._client is None:
                    self._client = bridge.create_client()
                    self._client_connected = False
                client = self._client
                if self._client_connected:
                    self._client_calls_inflight += 1
                    return client
                if self._client_connecting:
                    self._client_condition.wait()
                    continue
                self._client_connecting = True

            try:
                client.connect()
            except Exception:
                with self._client_condition:
                    if self._client is client:
                        self._client = None
                        self._client_connected = False
                    self._client_connecting = False
                    self._client_condition.notify_all()
                self._close_clients([client])
                raise

            close_after_connect = False
            with self._client_condition:
                self._client_connecting = False
                if self._client_retiring or self._client is not client:
                    if self._client is client:
                        self._client = None
                        self._client_connected = False
                    close_after_connect = True
                else:
                    self._client_connected = True
                    self._client_calls_inflight += 1
                self._client_condition.notify_all()
            if close_after_connect:
                self._close_clients([client])
                raise bridge.EltdxBridgeError("eltdx Provider 正在重载")
            return client

    def _return_client(self) -> None:
        to_close: list[Any] = []
        with self._client_condition:
            self._client_calls_inflight -= 1
            if self._client_calls_inflight == 0 and self._retired_clients:
                to_close = self._retired_clients
                self._retired_clients = []
            self._client_condition.notify_all()
        self._close_clients(to_close)

    # ---- 代码表 ----
    def _get_realtime_tdx_symbols(self, client) -> tuple[str, ...]:
        """加载并缓存报价所需的显式代码表。

        通达信报价协议没有 universe 参数, 只能先取代码表再由本插件分批; 代码表是
        日级维表, 不应在每个轮询周期重复拉取。

        只收 A 股与 ETF: 通达信"指数"代码表混有约 3000 只板块/题材指数, 而
        quote_service 会把不在指数/ETF 维表里的记录按个股写日K。指数实时由
        ``get_realtime_indices`` 按需单独拉取。
        """
        with self._client_condition:
            cached = self._realtime_tdx_symbols
        if cached is not None:
            return cached

        symbols: list[str] = []
        seen: set[str] = set()
        for code_list in (client.codes.all_a_shares(), client.codes.all_etfs()):
            for raw_code in code_list or ():
                text = str(raw_code or "").strip().lower()
                if _TDX_SYMBOL_RE.fullmatch(text) is None:
                    logger.warning("eltdx 实时行情跳过异常代码 %r", raw_code)
                    continue
                if text not in seen:
                    seen.add(text)
                    symbols.append(text)
        if not symbols:
            raise RuntimeError("eltdx 代码表未返回可用于实时行情的标的")

        snapshot = tuple(symbols)
        with self._client_condition:
            if self._realtime_tdx_symbols is None:
                self._realtime_tdx_symbols = snapshot
            return self._realtime_tdx_symbols

    # ---- K 线 ----
    def _fetch_bars(
        self,
        client,
        tdx_symbol: str,
        period: str,
        start_time: datetime | None,
        *,
        daily: bool,
    ) -> list[Any]:
        """按窗口向前回溯分页取原始 K 线。

        单页上限 800 根且 ``start`` 是相对最新一根的偏移量, 因此从最新一页依次
        向前翻, 直到本页最早时间覆盖窗口起点、返回不足一页、或触到页数上限。
        ``start_time`` 为 None 时取上游可提供的全历史 (仍受页数上限约束)。
        """
        target: Any = None
        if start_time is not None:
            wallclock = _beijing_naive(start_time)
            target = wallclock.date() if daily else wallclock

        bars: list[Any] = []
        for page_index in range(_MAX_KLINE_PAGES):
            page = client.bars.get(
                tdx_symbol,
                period=period,
                start=page_index * _KLINE_PAGE_SIZE,
                count=_KLINE_PAGE_SIZE,
            )
            page_bars = list(getattr(page, "bars", None) or ())
            if not page_bars:
                break
            bars.extend(page_bars)
            if len(page_bars) < _KLINE_PAGE_SIZE:
                break
            if target is None:
                continue
            times = [value for value in (_bar_time(bar) for bar in page_bars) if value is not None]
            if not times:
                logger.warning("eltdx %s %s 本页缺少可解析时间, 终止翻页", period, tdx_symbol)
                break
            oldest = min(times)
            covered = oldest.date() <= target if daily else oldest <= target
            if covered:
                break
        else:
            logger.warning(
                "eltdx %s %s 超过 %d 页上限, 停止回溯", period, tdx_symbol, _MAX_KLINE_PAGES,
            )
        return bars

    @staticmethod
    def _daily_rows(
        symbol: str,
        bars: list[Any],
        start_time: datetime | None,
        end_time: datetime | None,
        volume_scale: float = 1.0,
    ) -> list[dict]:
        start_date = _beijing_naive(start_time).date() if start_time is not None else None
        end_date = _beijing_naive(end_time).date() if end_time is not None else None
        rows_by_date: dict = {}
        for bar in bars:
            wallclock = _bar_time(bar)
            if wallclock is None:
                continue
            trade_date = wallclock.date()
            if start_date is not None and trade_date < start_date:
                continue
            if end_date is not None and trade_date > end_date:
                continue
            rows_by_date[trade_date] = {
                "symbol": symbol,
                "date": trade_date,
                "open": _finite_float(getattr(bar, "open", None)),
                "high": _finite_float(getattr(bar, "high", None)),
                "low": _finite_float(getattr(bar, "low", None)),
                "close": _finite_float(getattr(bar, "close", None)),
                # 股票/ETF 的 ``volume_lots`` 已是手, 不可再除以 100; 指数见 _volume_scale。
                "volume": _scaled(getattr(bar, "volume_lots", None), volume_scale),
                "amount": _finite_float(getattr(bar, "amount", None)),
            }
        return [rows_by_date[key] for key in sorted(rows_by_date)]

    @staticmethod
    def _minute_rows(
        symbol: str,
        bars: list[Any],
        start_time: datetime | None,
        end_time: datetime | None,
        volume_scale: float = 1.0,
    ) -> list[dict]:
        start = _beijing_naive(start_time) if start_time is not None else None
        end = _beijing_naive(end_time) if end_time is not None else None
        rows_by_time: dict = {}
        for bar in bars:
            wallclock = _bar_time(bar)
            if wallclock is None:
                continue
            if start is not None and wallclock < start:
                continue
            if end is not None and wallclock > end:
                continue
            rows_by_time[wallclock] = {
                "symbol": symbol,
                "datetime": wallclock,
                "open": _finite_float(getattr(bar, "open", None)),
                "high": _finite_float(getattr(bar, "high", None)),
                "low": _finite_float(getattr(bar, "low", None)),
                "close": _finite_float(getattr(bar, "close", None)),
                "volume": _scaled(getattr(bar, "volume_lots", None), volume_scale),
                "amount": _finite_float(getattr(bar, "amount", None)),
            }
        return [rows_by_time[key] for key in sorted(rows_by_time)]

    @staticmethod
    def _frame(rows: list[dict], schema: dict) -> pl.DataFrame:
        return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)

    @staticmethod
    def _resolve_requested(symbols: list[str]) -> tuple[dict[str, str], list[str]]:
        """项目 symbol → 通达信代码; 非法代码单独记录, 不连坐整批。"""
        requested: dict[str, str] = {}
        rejected: list[str] = []
        for symbol in symbols:
            text = str(symbol or "").strip().upper()
            try:
                tdx_symbol = _to_tdx_symbol(text)
            except ValueError:
                rejected.append(text)
                continue
            requested[tdx_symbol] = text
        return requested, rejected

    # ---- 日K ----
    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType | str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
        failed_out: list[str] | None = None,
    ) -> pl.DataFrame:
        """获取多标的原始日 K; 单标的失败不覆盖其他已成功数据。"""
        if not symbols:
            return self._frame([], _DAILY_SCHEMA)

        rows: list[dict] = []
        failures: list[str] = []
        failed_symbols: list[str] = []
        total = len(symbols)
        try:
            with self._client_session() as client:
                for index, symbol in enumerate(symbols):
                    failed_symbol = str(symbol or "").strip().upper()
                    try:
                        tdx_symbol = _to_tdx_symbol(symbol)
                        canonical_symbol, _, _ = _from_tdx_symbol(tdx_symbol)
                        failed_symbol = canonical_symbol
                        bars = self._fetch_bars(
                            client, tdx_symbol, "day", start_time, daily=True,
                        )
                        rows.extend(
                            self._daily_rows(
                                canonical_symbol, bars, start_time, end_time,
                                _volume_scale(asset_type),
                            ),
                        )
                    except Exception as exc:
                        failures.append(f"{failed_symbol}: {exc}")
                        failed_symbols.append(failed_symbol)
                        logger.warning("eltdx 日K %s 拉取失败: %s", failed_symbol, exc)
                    finally:
                        if on_chunk_done is not None:
                            on_chunk_done(index + 1, total)
        except Exception as exc:
            logger.warning("eltdx 日K client 不可用: %s", exc)
            if on_chunk_done is not None:
                for index in range(total):
                    on_chunk_done(index + 1, total)
            raise RuntimeError(f"eltdx 日K client 不可用: {exc}") from exc

        if failed_symbols:
            logger.warning(
                "eltdx 日K部分失败: %d/%d 标的未获取 (样例: %s)",
                len(failed_symbols), total, failed_symbols[:10],
            )
            if failed_out is not None:
                failed_out.extend(failed_symbols)
        if failures and not rows:
            raise RuntimeError(f"eltdx 日K请求全部失败 ({'; '.join(failures[:3])})")
        return self._frame(rows, _DAILY_SCHEMA).sort(["symbol", "date"])

    # ---- 分钟K ----
    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType | str = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
        failed_out: list[str] | None = None,
    ) -> pl.DataFrame:
        """获取多标的 1 分钟 OHLCV; 全部失败时抛出, 部分失败显式回传。

        用 ``bars.get(period='1m')`` 而不是 ``minutes.today``: 后者只有价量、不带
        OHLC, 且时间标签没有日期, 无法满足项目"北京墙钟 naive"的入库契约。
        """
        if str(freq).lower() not in {"1m", "1min", "1minute"}:
            raise ValueError(f"eltdx 仅支持 1m 分钟K, 得到: {freq!r}")
        if not symbols:
            return self._frame([], _MINUTE_SCHEMA)

        rows: list[dict] = []
        failures: list[str] = []
        failed_symbols: list[str] = []
        total = len(symbols)
        with self._client_session() as client:
            for index, symbol in enumerate(symbols):
                failed_symbol = str(symbol or "").strip().upper()
                try:
                    tdx_symbol = _to_tdx_symbol(symbol)
                    canonical_symbol, _, _ = _from_tdx_symbol(tdx_symbol)
                    failed_symbol = canonical_symbol
                    bars = self._fetch_bars(
                        client, tdx_symbol, "1m", start_time, daily=False,
                    )
                    rows.extend(
                        self._minute_rows(
                            canonical_symbol, bars, start_time, end_time,
                            _volume_scale(asset_type),
                        ),
                    )
                except Exception as exc:
                    failures.append(f"{failed_symbol}: {exc}")
                    failed_symbols.append(failed_symbol)
                    logger.warning("eltdx 分钟K %s 拉取失败: %s", failed_symbol, exc)
                finally:
                    if on_chunk_done is not None:
                        on_chunk_done(index + 1, total)

        if failed_symbols:
            logger.warning(
                "eltdx 分钟K部分失败: %d/%d 标的未获取 (样例: %s)",
                len(failed_symbols), total, failed_symbols[:10],
            )
            if failed_out is not None:
                failed_out.extend(failed_symbols)
        if failures and not rows:
            raise RuntimeError(f"eltdx 分钟K请求全部失败 ({'; '.join(failures[:3])})")
        return self._frame(rows, _MINUTE_SCHEMA).sort(["symbol", "datetime"])

    # ---- 除权因子 ----
    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType | str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """获取股票/ETF 的单事件除权因子, 保持 raw 日K 与复权计算分层。

        数据链: ``corporate.capital_changes`` 除权除息事件(上游按 75 只批量并发) →
        事件日前的原始收盘价(同标的日K窗口内自取) → 交易所公式推导单事件比值。

        不采用 ``corporate.adjustment_factors``: 那里给的是逐日的前/后复权仿射系数,
        与项目"单事件 ex_factor + 管道自行累积"的契约不同构, 直接写入会重复累积。
        """
        if str(asset_type).lower() not in {"stock", "etf"}:
            logger.info("eltdx 不提供 %s 的除权因子", asset_type)
            return pl.DataFrame(schema=_ADJ_FACTOR_SCHEMA)
        if not symbols:
            return pl.DataFrame(schema=_ADJ_FACTOR_SCHEMA)

        requested, rejected = self._resolve_requested(symbols)
        for symbol in rejected:
            logger.warning("eltdx 除权因子跳过非法 symbol: %s", symbol)
        if not requested:
            return pl.DataFrame(schema=_ADJ_FACTOR_SCHEMA)

        start_date = _beijing_naive(start_time).date() if start_time is not None else None
        end_date = _beijing_naive(end_time).date() if end_time is not None else None

        rows: list[dict] = []
        total = len(requested)
        try:
            with self._client_session() as client:
                events = self._collect_ex_events(client, list(requested), start_date, end_date)
                for index, (tdx_symbol, symbol) in enumerate(requested.items()):
                    try:
                        symbol_events = events.get(tdx_symbol)
                        if symbol_events:
                            rows.extend(
                                self._adj_factor_rows(
                                    client, tdx_symbol, symbol, symbol_events,
                                ),
                            )
                    except Exception as exc:
                        logger.warning("eltdx 除权因子 %s 拉取失败: %s", symbol, exc)
                    finally:
                        if on_chunk_done is not None:
                            on_chunk_done(index + 1, total)
        except Exception as exc:
            logger.warning("eltdx 除权因子 client 不可用: %s", exc)
            if on_chunk_done is not None:
                for index in range(total):
                    on_chunk_done(index + 1, total)
            raise RuntimeError(f"eltdx 除权因子 client 不可用: {exc}") from exc

        if not rows:
            return pl.DataFrame(schema=_ADJ_FACTOR_SCHEMA)
        return (
            self._frame(rows, _ADJ_FACTOR_SCHEMA)
            .unique(subset=["symbol", "trade_date"], keep="last")
            .sort(["symbol", "trade_date"])
        )

    def _collect_ex_events(
        self,
        client,
        tdx_symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> dict[str, list[tuple]]:
        """按批取除权除息事件; 批次失败只丢该批, 不连坐其他标的。"""
        events: dict[str, list[tuple]] = {}
        for start in range(0, len(tdx_symbols), _CAPITAL_CHANGE_BATCH_SIZE):
            chunk = tdx_symbols[start : start + _CAPITAL_CHANGE_BATCH_SIZE]
            try:
                batch = client.corporate.capital_changes(chunk)
            except Exception as exc:
                logger.warning("eltdx 除权事件批次(%d 只)拉取失败: %s", len(chunk), exc)
                continue
            for block in getattr(batch, "blocks", None) or ():
                full_code = str(getattr(block, "full_code", "") or "").strip().lower()
                for record in getattr(block, "records", None) or ():
                    if int(getattr(record, "category_raw", 0) or 0) != _EX_RIGHTS_CATEGORY:
                        continue
                    event_date = getattr(record, "date", None)
                    if not isinstance(event_date, date):
                        continue
                    if start_date is not None and event_date < start_date:
                        continue
                    if end_date is not None and event_date > end_date:
                        continue
                    events.setdefault(full_code, []).append((event_date, record))
        return events

    def _adj_factor_rows(
        self,
        client,
        tdx_symbol: str,
        symbol: str,
        symbol_events: list[tuple],
    ) -> list[dict]:
        """单标的: 事件日前收盘 + 事件成分 → 单事件 ex_factor。

        前收盘取自同一标的的原始日K序列 (事件日的上一根), 不依赖服务端
        首根K线才有的 ``last_close_price``; 窗口起点因此要早于首个事件。
        """
        symbol_events.sort(key=lambda item: item[0])
        kline_start = datetime.combine(
            symbol_events[0][0] - timedelta(days=_PREV_CLOSE_BACKDAYS), datetime.min.time(),
        )
        bars = self._fetch_bars(client, tdx_symbol, "day", kline_start, daily=True)
        closes: dict[date, float] = {}
        for bar in bars:
            wallclock = _bar_time(bar)
            close = _finite_float(getattr(bar, "close", None))
            if wallclock is None or close is None:
                continue
            closes[wallclock.date()] = close
        if not closes:
            logger.warning(
                "eltdx 除权因子 %s 原始日K为空, 跳过其 %d 个事件", symbol, len(symbol_events),
            )
            return []

        trading_days = sorted(closes)
        rows: list[dict] = []
        for event_date, record in symbol_events:
            previous_days = [day for day in trading_days if day < event_date]
            if not previous_days:
                logger.warning(
                    "eltdx 除权因子 %s %s 日K窗口未覆盖事件前收盘, 跳过", symbol, event_date,
                )
                continue
            factor = _ex_factor(closes[previous_days[-1]], record)
            if factor is None:
                logger.warning("eltdx 除权因子 %s %s 无法推导, 跳过", symbol, event_date)
                continue
            rows.append({"symbol": symbol, "trade_date": event_date, "ex_factor": factor})
        return rows

    # ---- 实时行情 ----
    @staticmethod
    def _fetch_snapshots(client, codes: list[str]) -> list[Any]:
        """按 80 只切批取报价快照。

        上游不做切批: 单请求超过 80 只会静默截断, 整批上千只会直接断流。
        """
        quotes: list[Any] = []
        for start in range(0, len(codes), _QUOTE_BATCH_SIZE):
            page = client.quotes.get_snapshots(codes[start : start + _QUOTE_BATCH_SIZE])
            quotes.extend(list(page or ()))
        return quotes

    @staticmethod
    def _quote_records(quotes: Any, requested: dict[str, str], fetched_ms: int) -> list[dict]:
        """报价快照 → 内部 realtime record。

        ``total_hand`` 已是手, 不可再除以 100; ``change_pct`` / ``amplitude`` 按项目
        契约用小数制 —— 统一从 ``change_amount`` 推导, 不混用上游的百分数属性。
        """
        rows: list[dict] = []
        for quote in quotes or ():
            try:
                returned = (
                    f"{str(getattr(quote, 'exchange', '')).lower()}"
                    f"{str(getattr(quote, 'code', '')).zfill(6)}"
                )
                symbol = requested.get(returned)
                if symbol is None:
                    logger.warning("eltdx 实时行情忽略未请求的响应: %s", returned)
                    continue
                last_price = _finite_float(getattr(quote, "last_price", None))
                prev_close = _finite_float(getattr(quote, "pre_close_price", None))
                high_price = _finite_float(getattr(quote, "high_price", None))
                low_price = _finite_float(getattr(quote, "low_price", None))
                change_amount = (
                    last_price - prev_close
                    if last_price is not None and prev_close is not None
                    else None
                )
                rows.append({
                    "symbol": symbol,
                    "name": None,  # 快照无名称, 由下游维表关联
                    "last_price": last_price,
                    "prev_close": prev_close,
                    "open": _finite_float(getattr(quote, "open_price", None)),
                    "high": high_price,
                    "low": low_price,
                    "volume": _finite_float(getattr(quote, "total_hand", None)),
                    "amount": _finite_float(getattr(quote, "amount", None)),
                    "change_pct": (
                        change_amount / prev_close
                        if change_amount is not None and prev_close
                        else None
                    ),
                    "change_amount": change_amount,
                    "amplitude": (
                        (high_price - low_price) / prev_close
                        if high_price is not None and low_price is not None and prev_close
                        else None
                    ),
                    # 换手率需历史股本口径 (§3.4), 交给 enriched 管道用历史股本计算。
                    "turnover_rate": None,
                    # 协议只回未解码的原始时间字段, 用本轮拉取时间, 不伪造历史时间戳。
                    "timestamp": fetched_ms,
                    "session": None,
                })
            except Exception as exc:
                logger.warning("eltdx 实时行情响应映射失败: %s", exc)
        return rows

    def get_realtime(self) -> list[dict]:
        """全市场 A 股 + ETF 快照; 软失败返回空列表, 不阻断行情轮询线程。

        这是软失败入口: TCP 或代码表错误不能终止行情轮询, 调用方会保留上一份有效
        快照。指数不在此列 —— 见 ``_get_realtime_tdx_symbols``。
        """
        fetched_ms = int(time.time() * 1000)
        try:
            with self._client_session() as client:
                tdx_symbols = self._get_realtime_tdx_symbols(client)
                quotes = self._fetch_snapshots(client, list(tdx_symbols))
        except Exception as exc:
            logger.warning("eltdx 全市场实时行情拉取失败: %s", exc)
            return []

        requested = {tdx_symbol: _from_tdx_symbol(tdx_symbol)[0] for tdx_symbol in tdx_symbols}
        rows = self._quote_records(quotes, requested, fetched_ms)
        logger.info("eltdx 实时行情拉取完成: %d 条(请求 %d 只)", len(rows), len(tdx_symbols))
        return rows

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        """指数实时快照 → 内部 realtime record (可选插件协议, quote_service 调用)。

        通达信报价接口对指数同样可用; 失败返回 None, 让上层与"成功但无数据"的空
        列表区分, 保留上轮有效指数缓存, 而不是把指数条打空。
        """
        requested, rejected = self._resolve_requested(symbols)
        for symbol in rejected:
            logger.warning("eltdx 指数行情跳过非法 symbol: %s", symbol)
        if not requested:
            return []

        fetched_ms = int(time.time() * 1000)
        try:
            with self._client_session() as client:
                quotes = self._fetch_snapshots(client, list(requested))
        except Exception as exc:
            logger.warning("eltdx 指数行情拉取失败(%d 只): %s", len(requested), exc)
            return None

        rows = self._quote_records(quotes, requested, fetched_ms)
        logger.info("eltdx 指数行情拉取完成: %d 条(请求 %d 只)", len(rows), len(requested))
        return rows

    # ---- 五档盘口 ----
    def get_depth5(self, symbols: list[str]) -> dict[str, dict]:
        """用快照 + 五档刷新流映射 sealed 服务契约; 失败严格返回空, 不换源。

        ``quotes.get_snapshots`` 只给一档, 完整五档要 ``helpers.full_quotes`` 把
        0x0547 刷新流合并进来; 补不齐的档位由 ``_level_numbers`` 保留 None。
        """
        if not symbols:
            return {}

        requested, rejected = self._resolve_requested(symbols)
        for symbol in rejected:
            logger.warning("eltdx 五档跳过非法 symbol: %s", symbol)
        if not requested:
            return {}

        codes = list(requested)
        try:
            with self._client_session() as client:
                quotes: list[Any] = []
                for start in range(0, len(codes), _QUOTE_BATCH_SIZE):
                    quotes.extend(
                        client.helpers.full_quotes(codes[start : start + _QUOTE_BATCH_SIZE]) or [],
                    )
        except Exception as exc:
            logger.warning("eltdx 五档拉取失败(%d 只): %s", len(requested), exc)
            return {}

        fetched_ms = int(time.time() * 1000)
        result: dict[str, dict] = {}
        for quote in quotes:
            returned = (
                f"{str(getattr(quote, 'exchange', '')).lower()}"
                f"{str(getattr(quote, 'code', '')).zfill(6)}"
            )
            symbol = requested.get(returned)
            if symbol is None:
                logger.warning("eltdx 五档忽略未请求的响应: %s", returned)
                continue
            result[symbol] = {
                "ask_volumes": _level_numbers(getattr(quote, "sell_levels", None)),
                "bid_volumes": _level_numbers(getattr(quote, "buy_levels", None)),
                "timestamp": fetched_ms,
            }
        return result

    # ---- 标的维表 ----
    def get_instruments(self, asset_type: AssetType | str = "stock") -> list[dict]:
        """从通达信代码表提供股票/ETF/指数维表。

        只返回上游真实给出的字段(代码/名称/交易所); eltdx 不提供股本与涨跌停
        元数据, 因此不写这些字段, 由 instrument_sync 的 flatten 置空。
        """
        asset = str(asset_type).lower()
        category = _INSTRUMENT_CATEGORIES.get(asset)
        if category is None:
            raise ValueError(f"不支持的标的类型: {asset_type!r}")

        rows_by_symbol: dict[str, dict] = {}
        try:
            with self._client_session() as client:
                for market in ("sh", "sz", "bj"):
                    for item in client.codes.all(market) or ():
                        if str(getattr(item, "category", "")) != category:
                            continue
                        full_code = str(getattr(item, "full_code", "") or "").strip().lower()
                        try:
                            symbol, code, exchange = _from_tdx_symbol(full_code)
                        except ValueError as exc:
                            logger.warning(
                                "eltdx %s 代码表跳过异常值 %r: %s", asset_type, item, exc,
                            )
                            continue
                        rows_by_symbol[symbol] = {
                            "symbol": symbol,
                            # 名称缺失时留空: 用裸代码冒充会让历史 ST 标的按主板判定涨跌停。
                            "name": str(getattr(item, "name", "") or "").strip() or None,
                            "code": code,
                            "exchange": exchange,
                            "region": "CN",
                            "type": asset,
                            "ext": {},
                        }
        except Exception as exc:
            logger.warning("eltdx %s 代码表拉取失败: %s", asset_type, exc)
            raise RuntimeError(f"eltdx {asset_type} 代码表拉取失败: {exc}") from exc

        rows = [rows_by_symbol[symbol] for symbol in sorted(rows_by_symbol)]
        if not rows:
            raise RuntimeError(f"eltdx {asset_type} 代码表未返回可解析标的")
        return rows

    # ---- 设置页试拉 ----
    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        """控制在小窗口, 避免单击测试触发单标的全历史分钟请求。"""
        selected = list(symbols or [])[:3] or ["600000.SH"]
        end_time = datetime.now(_CN_TZ).replace(tzinfo=None)
        start_time = end_time - timedelta(days=30)
        try:
            if dataset == "daily":
                return self._preview(dataset, self.get_daily(selected, start_time, end_time))
            if dataset == "adj_factor":
                return self._preview(dataset, self.get_adj_factors(selected, start_time, end_time))
            if dataset == "minute":
                return self._preview(dataset, self.get_minute(selected[:1], start_time, end_time))
            if dataset == "realtime":
                rows = self.get_realtime()
                return {
                    "provider": self.name,
                    "dataset": dataset,
                    "rows": len(rows),
                    "columns": list(rows[0]) if rows else [],
                    "preview": rows[:5],
                }
            if dataset == "depth5":
                values = self.get_depth5(selected)
                return {
                    "provider": self.name,
                    "dataset": dataset,
                    "rows": len(values),
                    "columns": ["ask_volumes", "bid_volumes", "timestamp"],
                    "preview": [{"symbol": symbol, **row} for symbol, row in list(values.items())[:5]],
                }
        except Exception as exc:
            return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(exc)}
        return {
            "provider": self.name,
            "dataset": dataset,
            "rows": 0,
            "error": f"eltdx 未声明 {dataset} 数据集",
        }

    def _preview(self, dataset: str, df: pl.DataFrame) -> dict:
        preview = df.head(5).to_dicts()
        for row in preview:
            for key, value in list(row.items()):
                if isinstance(value, date) or hasattr(value, "isoformat"):
                    row[key] = value.isoformat()
        return {
            "provider": self.name,
            "dataset": dataset,
            "rows": df.height,
            "columns": df.columns,
            "preview": preview,
        }


def _ex_factor(previous_close: float | None, record: Any) -> float | None:
    """除权除息事件 → 单事件 ``前收盘 / 除权参考价``。

    通达信同一公式 (事件成分均为每 10 股口径, c1=现金分红 c2=配股价 c3=送转股
    c4=配股):

      参考价 = (前收盘 x 10 - 现金分红 + 配股 x 配股价) / (10 + 送转股 + 配股)

    等价于 eltdx ``_event_coefficients`` 的 multiplier/offset, 但项目契约要的是
    单事件比值而不是逐日累积量。
    """
    if previous_close is None or previous_close <= 0:
        return None
    dividend = _finite_float(getattr(record, "c1_value", None)) or 0.0
    allotment_price = _finite_float(getattr(record, "c2_value", None))
    bonus = _finite_float(getattr(record, "c3_value", None)) or 0.0
    allotment = _finite_float(getattr(record, "c4_value", None)) or 0.0
    if allotment > 0 and allotment_price is None:
        logger.warning("eltdx 除权因子配股价缺失, 无法推导 (%s)", getattr(record, "date", None))
        return None

    denominator = 10.0 + bonus + allotment
    if denominator <= 0:
        return None
    reference_price = (
        previous_close * 10.0 - dividend + allotment * (allotment_price or 0.0)
    ) / denominator
    if not math.isfinite(reference_price) or reference_price <= 0:
        return None
    factor = previous_close / reference_price
    if not math.isfinite(factor) or factor <= 0 or math.isclose(factor, 1.0, rel_tol=1e-12, abs_tol=1e-12):
        return None
    return factor
