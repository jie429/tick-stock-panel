"""基于 eltdx 的免费通达信 TCP Provider。

tdx-mcp 本身是 MCP stdio 服务; 项目后端不依赖外部进程, 而是直接调用其底层
eltdx 客户端。只声明可以满足当前项目契约的数据集: 原始日 K、除权因子、1 分钟
K、全市场报价与五档。
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
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from app.data_providers.base import AssetType
from app.plugins.tdx_mcp import bridge

logger = logging.getLogger(__name__)

_DATASETS = ("daily", "adj_factor", "minute", "realtime", "depth5")
_CN_TZ = ZoneInfo("Asia/Shanghai")
_KLINE_PAGE_SIZE = 800
# eltdx 的分页上限是 65536 根; 82 页覆盖最后一页不足 800 的情况。
_MAX_KLINE_PAGES = 82
_SYMBOL_RE = re.compile(r"^(?P<code>\d{6})\.(?P<exchange>[A-Z]{2})$")
_TDX_SYMBOL_RE = re.compile(r"^(?P<exchange>sh|sz|bj)(?P<code>\d{6})$", re.IGNORECASE)

_DAILY_SCHEMA = {
    "symbol": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    # 项目内部 volume 是“手”; eltdx KlineItem.volume 同为手, 必须原样透传。
    "volume": pl.Float64,
    "amount": pl.Float64,
}
_MINUTE_SCHEMA = {
    "symbol": pl.Utf8,
    "datetime": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
}
_ADJ_FACTOR_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "ex_factor": pl.Float64,
}


@dataclass
class _TdxMcpConfig:
    """让现有 custom loader 识别内置 Provider 的最小 config shim。"""

    name: str = "tdx_mcp"
    display_name: str = "tdx-mcp (免费 TCP)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def availability() -> tuple[bool, str]:
    """供 plugin.yaml 的 check 使用。"""
    return bridge.availability()


def _to_tdx_symbol(symbol: str) -> str:
    """项目 ``600000.SH`` → eltdx ``sh600000``, 不靠裸码猜交易所。"""
    text = str(symbol or "").strip().upper()
    matched = _SYMBOL_RE.fullmatch(text)
    if matched is None:
        raise ValueError(f"symbol 必须为带交易所后缀的六码代码(如 600000.SH), 得到: {symbol!r}")
    exchange = matched.group("exchange")
    if exchange not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"不支持的交易所后缀: {exchange}")
    return f"{exchange.lower()}{matched.group('code')}"


def _from_tdx_symbol(tdx_symbol: str) -> tuple[str, str, str]:
    """eltdx ``sh600000`` → 项目 ``600000.SH``、裸码、交易所。"""
    text = str(tdx_symbol or "").strip()
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


def _kline_kind(asset_type: AssetType | str) -> str:
    return "index" if asset_type == "index" else "stock"


def _level_numbers(levels: Any) -> list[int | None]:
    """盘口缺档用 None, 绝不能伪造成 0 (会误判为真封板)。"""
    values: list[int | None] = []
    for level in list(levels or [])[:5]:
        value = getattr(level, "number", None)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            values.append(None)
            continue
        try:
            values.append(int(value) if math.isfinite(float(value)) else None)
        except (OverflowError, ValueError):
            values.append(None)
    return (values + [None] * 5)[:5]


def _finite_float(value: Any) -> float | None:
    """将上游数值收口为有限 float; 缺失/异常值不能伪造成 0。"""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


class TdxMcpProvider:
    """免费通达信 TCP 数据源。

    分钟 K 用 ``get_kline('1m')``, 不使用只有 price/volume 的 ``get_minute``.
    无起点日 K 会调用 ``get_kline_all`` 获取上游可提供的单标的全历史; 分钟 K
    仍受通达信服务器保留窗口限制, 本插件保守声明最近 90 个交易日。
    """

    name = "tdx_mcp"
    builtin = True
    # 默认 custom daily Provider 只会收到股票; TDX 明确覆盖指数和 ETF。
    daily_asset_types = frozenset({"stock", "index", "etf"})
    # 代码表只提供基础维表字段(代码/名称/交易所), 不伪造股本和涨跌停元数据。
    instrument_asset_types = frozenset({"stock", "index", "etf"})
    # TDX 的分钟K按标的请求; 不能作为 TickFlow Expert 的全市场分钟同步替代。
    supports_minute_universe_sync = False
    # 2026-09 实测 1m 历史约 100 个交易日; 保守暴露为 90, 避免 UI 承诺深历史。
    minute_history_days = 90
    # TCP 源异常时返回明确空结果, 不因本机仍有 TickFlow 配置而暗中换源。
    fallback_to_tickflow_on_error = False
    # ``kline_sync`` 仅向显式声明该能力的 Provider 传入失败标的出参, 避免破坏旧插件签名。
    supports_daily_failure_reporting = True
    # 分组分钟K同样需要把单标的 TCP 失败回传给落库/API 层, 不能让部分结果误报成功。
    supports_minute_failure_reporting = True
    # 全市场行情须以代码表切成 80 只一批的 TCP 请求, 不能按普通插件的 1 秒下限轮询。
    realtime_min_interval = 30.0

    def __init__(self) -> None:
        self.config = _TdxMcpConfig()
        self._client = None
        self._client_connected = False
        self._client_connecting = False
        self._client_retiring = False
        self._client_calls_inflight = 0
        self._retired_clients: list[Any] = []
        self._client_condition = threading.Condition(threading.RLock())
        self._instrument_names: dict[str, str] | None = None
        self._realtime_tdx_symbols: tuple[str, ...] | None = None

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
                    raise bridge.TdxMcpBridgeError("tdx-mcp Provider 正在重载")
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
                raise bridge.TdxMcpBridgeError("tdx-mcp Provider 正在重载")
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

    def _get_instrument_names(self, client) -> dict[str, str]:
        """Read real names from eltdx code tables and cache an immutable snapshot.

        Filtered ``*_codes_all`` methods only return codes. Writing a bare code as
        ``name`` would misclassify historical ST price limits, so a failed name
        lookup must skip the instrument update instead of writing fake metadata.
        """
        with self._client_condition:
            cached = self._instrument_names
        if cached is not None:
            return cached

        names: dict[str, str] = {}
        for exchange in ("sh", "sz", "bj"):
            for entry in client.get_codes_all(exchange) or []:
                full_code = str(getattr(entry, "full_code", "") or "").strip().lower()
                if not full_code:
                    entry_exchange = str(getattr(entry, "exchange", "") or "").strip().lower()
                    entry_code = str(getattr(entry, "code", "") or "").strip()
                    full_code = f"{entry_exchange}{entry_code}" if entry_exchange and entry_code else ""
                name = str(getattr(entry, "name", "") or "").strip()
                if full_code and name:
                    names[full_code] = name
        if not names:
            raise RuntimeError("eltdx 代码表未返回可用名称")

        with self._client_condition:
            if self._instrument_names is None:
                self._instrument_names = names
            return self._instrument_names

    def _get_realtime_tdx_symbols(self, client) -> tuple[str, ...]:
        """加载并缓存 TCP 报价所需的全市场显式代码。

        通达信报价协议没有 universe 参数, 只能先取得股票、ETF、指数代码表, 再由
        ``eltdx`` 分成每批最多 80 只的请求。代码表是日级维表, 不应在每个轮询周期
        重复拉取。
        """
        with self._client_condition:
            cached = self._realtime_tdx_symbols
        if cached is not None:
            return cached

        symbols: list[str] = []
        seen: set[str] = set()
        for method_name in ("get_a_share_codes_all", "get_etf_codes_all", "get_index_codes_all"):
            for raw_symbol in getattr(client, method_name)() or []:
                try:
                    canonical, code, exchange = _from_tdx_symbol(str(raw_symbol))
                except ValueError as exc:
                    logger.warning("tdx-mcp 实时行情跳过异常代码 %r: %s", raw_symbol, exc)
                    continue
                # ``canonical`` 仅用于校验标准后缀; 报价请求仍要用 TDX 前缀格式。
                del canonical
                tdx_symbol = f"{exchange.lower()}{code}"
                if tdx_symbol not in seen:
                    symbols.append(tdx_symbol)
                    seen.add(tdx_symbol)
        if not symbols:
            raise RuntimeError("eltdx 代码表未返回可用于实时行情的标的")

        snapshot = tuple(symbols)
        with self._client_condition:
            if self._realtime_tdx_symbols is None:
                self._realtime_tdx_symbols = snapshot
            return self._realtime_tdx_symbols

    def _fetch_kline_items(
        self,
        client,
        period: str,
        tdx_symbol: str,
        *,
        start_time: datetime | None,
        asset_type: AssetType | str,
        daily: bool,
    ) -> list[Any]:
        kind = _kline_kind(asset_type)
        if start_time is None:
            response = client.get_kline_all(period, tdx_symbol, kind=kind)
            return list(getattr(response, "items", None) or [])

        target = _beijing_naive(start_time)
        items: list[Any] = []
        for page_index in range(_MAX_KLINE_PAGES):
            offset = page_index * _KLINE_PAGE_SIZE
            response = client.get_kline(
                period,
                tdx_symbol,
                start=offset,
                count=_KLINE_PAGE_SIZE,
                kind=kind,
            )
            page_items = list(getattr(response, "items", None) or [])
            if not page_items:
                break
            items.extend(page_items)

            page_times = []
            for item in page_items:
                try:
                    page_times.append(_beijing_naive(item.time))
                except ValueError:
                    continue
            if not page_times:
                logger.warning("tdx-mcp %s %s 页面缺少可解析时间, 终止翻页", period, tdx_symbol)
                break
            oldest = min(page_times)
            covered = oldest.date() <= target.date() if daily else oldest <= target
            if covered:
                break
            if int(getattr(response, "count", len(page_items))) < _KLINE_PAGE_SIZE:
                break
        else:
            logger.warning("tdx-mcp %s %s 超过 %d 页上限, 停止回溯", period, tdx_symbol, _MAX_KLINE_PAGES)
        return items

    @staticmethod
    def _daily_rows(
        symbol: str,
        items: list[Any],
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> list[dict]:
        start_date = _beijing_naive(start_time).date() if start_time is not None else None
        end_date = _beijing_naive(end_time).date() if end_time is not None else None
        rows_by_date: dict = {}
        for item in items:
            try:
                trade_date = _beijing_naive(item.time).date()
            except ValueError:
                continue
            if start_date is not None and trade_date < start_date:
                continue
            if end_date is not None and trade_date > end_date:
                continue
            rows_by_date[trade_date] = {
                "symbol": symbol,
                "date": trade_date,
                "open": getattr(item, "open_price", None),
                "high": getattr(item, "high_price", None),
                "low": getattr(item, "low_price", None),
                "close": getattr(item, "close_price", None),
                "volume": getattr(item, "volume", None),
                "amount": getattr(item, "amount", None),
            }
        return [rows_by_date[key] for key in sorted(rows_by_date)]

    @staticmethod
    def _minute_rows(
        symbol: str,
        items: list[Any],
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> list[dict]:
        start = _beijing_naive(start_time) if start_time is not None else None
        end = _beijing_naive(end_time) if end_time is not None else None
        rows_by_time: dict = {}
        for item in items:
            try:
                wallclock = _beijing_naive(item.time)
            except ValueError:
                continue
            if start is not None and wallclock < start:
                continue
            if end is not None and wallclock > end:
                continue
            rows_by_time[wallclock] = {
                "symbol": symbol,
                "datetime": wallclock,
                "open": getattr(item, "open_price", None),
                "high": getattr(item, "high_price", None),
                "low": getattr(item, "low_price", None),
                "close": getattr(item, "close_price", None),
                "volume": getattr(item, "volume", None),
                "amount": getattr(item, "amount", None),
            }
        return [rows_by_time[key] for key in sorted(rows_by_time)]

    @staticmethod
    def _frame(rows: list[dict], schema: dict) -> pl.DataFrame:
        return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)

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
                        items = self._fetch_kline_items(
                            client,
                            "day",
                            tdx_symbol,
                            start_time=start_time,
                            asset_type=asset_type,
                            daily=True,
                        )
                        rows.extend(self._daily_rows(canonical_symbol, items, start_time, end_time))
                    except Exception as exc:
                        failures.append(f"{failed_symbol}: {exc}")
                        failed_symbols.append(failed_symbol)
                        logger.warning("tdx-mcp 日K %s 拉取失败: %s", failed_symbol, exc)
                    finally:
                        if on_chunk_done is not None:
                            on_chunk_done(index + 1, total)
        except Exception as exc:
            logger.warning("tdx-mcp 日K client 不可用: %s", exc)
            if on_chunk_done is not None:
                for index in range(total):
                    on_chunk_done(index + 1, total)
            raise RuntimeError(f"tdx-mcp 日K client 不可用: {exc}") from exc

        if failed_symbols:
            logger.warning(
                "tdx-mcp 日K部分失败: %d/%d 标的未获取 (样例: %s)",
                len(failed_symbols), total, failed_symbols[:10],
            )
            if failed_out is not None:
                failed_out.extend(failed_symbols)
        if failures and not rows:
            raise RuntimeError(f"tdx-mcp 日K请求全部失败 ({'; '.join(failures[:3])})")
        return self._frame(rows, _DAILY_SCHEMA).sort(["symbol", "date"])

    @staticmethod
    def _adj_factor_rows(
        symbol: str,
        items: list[Any],
        xdxr_items: list[Any],
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> list[dict]:
        """通达信除权除息事件 → 项目单事件 ``ex_factor``。

        项目管道会自行做事件累积, 故不能把 ``eltdx.get_factors`` 的每日累计
        ``qfq_factor`` 直接写入。对于每个除权日, 按照通达信同一公式计算
        ``前收盘 / 除权参考价``; 这等价于 qfq 累计因子在该日的跳变比值。
        """
        start_date = _beijing_naive(start_time).date() if start_time is not None else None
        end_date = _beijing_naive(end_time).date() if end_time is not None else None
        bars: list[tuple[Any, Any]] = []
        for item in items:
            try:
                bars.append((_beijing_naive(item.time).date(), item))
            except ValueError:
                continue
        bars.sort(key=lambda value: value[0])
        if not bars:
            return []

        # 同一天的分红、送配可能拆成多条。各成分相加, 配股价取非空最大值; 若同日
        # 返回不一致配股价, 宁可保守跳过, 不能凭猜测制造复权因子。
        events: dict[Any, dict[str, float | None]] = {}
        for event in xdxr_items:
            try:
                event_date = _beijing_naive(event.time).date()
            except ValueError:
                continue
            parts = events.setdefault(event_date, {
                "fenhong": 0.0,
                "songzhuangu": 0.0,
                "peigu": 0.0,
                "peigujia": None,
            })
            for field_name in ("fenhong", "songzhuangu", "peigu"):
                value = _finite_float(getattr(event, field_name, None))
                if value is not None:
                    parts[field_name] = float(parts[field_name] or 0.0) + value
            price = _finite_float(getattr(event, "peigujia", None))
            if price is not None:
                old_price = parts["peigujia"]
                if old_price is not None and not math.isclose(old_price, price, rel_tol=1e-9, abs_tol=1e-12):
                    parts["peigujia"] = math.nan
                elif old_price is None:
                    parts["peigujia"] = price

        rows: list[dict] = []
        for event_date, parts in sorted(events.items()):
            effective = next(((bar_date, bar) for bar_date, bar in bars if bar_date >= event_date), None)
            if effective is None:
                continue
            trade_date, bar = effective
            if (start_date is not None and trade_date < start_date) or (
                end_date is not None and trade_date > end_date
            ):
                continue
            previous_close = _finite_float(getattr(bar, "last_close_price", None))
            dividend = float(parts["fenhong"] or 0.0)
            bonus = float(parts["songzhuangu"] or 0.0)
            allotment = float(parts["peigu"] or 0.0)
            allotment_price = parts["peigujia"]
            if previous_close is None or previous_close <= 0:
                continue
            if allotment > 0 and (allotment_price is None or not math.isfinite(allotment_price)):
                logger.warning("tdx-mcp 除权因子跳过 %s %s: 配股价缺失或冲突", symbol, trade_date)
                continue
            resolved_allotment_price = float(allotment_price or 0.0)
            denominator = 10.0 + bonus + allotment
            if denominator <= 0:
                logger.warning("tdx-mcp 除权因子跳过 %s %s: 非法送配分母", symbol, trade_date)
                continue
            reference_price = (
                (previous_close * 10.0 - dividend) + allotment * resolved_allotment_price
            ) / denominator
            if not math.isfinite(reference_price) or reference_price <= 0:
                logger.warning("tdx-mcp 除权因子跳过 %s %s: 除权参考价无效", symbol, trade_date)
                continue
            factor = previous_close / reference_price
            if not math.isfinite(factor) or factor <= 0 or math.isclose(factor, 1.0, rel_tol=1e-12, abs_tol=1e-12):
                continue
            rows.append({"symbol": symbol, "trade_date": trade_date, "ex_factor": factor})
        return rows

    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType | str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """获取股票/ETF的单事件除权因子, 保持 raw 日K 与复权计算分层。"""
        if str(asset_type).lower() not in {"stock", "etf"}:
            logger.info("tdx-mcp 不提供 %s 的除权因子", asset_type)
            return self._frame([], _ADJ_FACTOR_SCHEMA)
        if not symbols:
            return self._frame([], _ADJ_FACTOR_SCHEMA)

        rows: list[dict] = []
        failures: list[str] = []
        total = len(symbols)
        # 需至少覆盖事件日前收盘; 为交易日/节假日留出自然日余量。
        kline_start = start_time - timedelta(days=31) if start_time is not None else None
        try:
            with self._client_session() as client:
                for index, symbol in enumerate(symbols):
                    failed_symbol = str(symbol or "").strip().upper()
                    try:
                        tdx_symbol = _to_tdx_symbol(symbol)
                        canonical_symbol, _, _ = _from_tdx_symbol(tdx_symbol)
                        failed_symbol = canonical_symbol
                        items = self._fetch_kline_items(
                            client,
                            "day",
                            tdx_symbol,
                            start_time=kline_start,
                            asset_type="stock",
                            daily=True,
                        )
                        xdxr_items = list(client.get_xdxr(tdx_symbol) or [])
                        rows.extend(
                            self._adj_factor_rows(
                                canonical_symbol, items, xdxr_items, start_time, end_time,
                            ),
                        )
                    except Exception as exc:
                        failures.append(f"{failed_symbol}: {exc}")
                        logger.warning("tdx-mcp 除权因子 %s 拉取失败: %s", failed_symbol, exc)
                    finally:
                        if on_chunk_done is not None:
                            on_chunk_done(index + 1, total)
        except Exception as exc:
            logger.warning("tdx-mcp 除权因子 client 不可用: %s", exc)
            if on_chunk_done is not None:
                for index in range(total):
                    on_chunk_done(index + 1, total)
            raise RuntimeError(f"tdx-mcp 除权因子 client 不可用: {exc}") from exc

        if failures:
            logger.warning(
                "tdx-mcp 除权因子部分失败: %d/%d 标的未获取 (样例: %s)",
                len(failures), total, failures[:3],
            )
        if failures and not rows:
            raise RuntimeError(f"tdx-mcp 除权因子请求全部失败 ({'; '.join(failures[:3])})")
        return self._frame(rows, _ADJ_FACTOR_SCHEMA).unique(
            subset=["symbol", "trade_date"], keep="last",
        ).sort(["symbol", "trade_date"])

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
        """获取多标的 1 分钟 OHLC K; 全部失败时抛出, 部分失败显式回传。"""
        if str(freq).lower() not in {"1m", "1min", "1minute"}:
            raise ValueError(f"tdx-mcp 仅支持 1m 分钟K, 得到: {freq!r}")
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
                    items = self._fetch_kline_items(
                        client,
                        "1m",
                        tdx_symbol,
                        start_time=start_time,
                        asset_type=asset_type,
                        daily=False,
                    )
                    rows.extend(self._minute_rows(canonical_symbol, items, start_time, end_time))
                except Exception as exc:
                    failures.append(f"{failed_symbol}: {exc}")
                    failed_symbols.append(failed_symbol)
                    logger.warning("tdx-mcp 分钟K %s 拉取失败: %s", failed_symbol, exc)
                finally:
                    if on_chunk_done is not None:
                        on_chunk_done(index + 1, total)

        if failed_symbols:
            logger.warning(
                "tdx-mcp 分钟K部分失败: %d/%d 标的未获取 (样例: %s)",
                len(failed_symbols), total, failed_symbols[:10],
            )
            if failed_out is not None:
                failed_out.extend(failed_symbols)
        if failures and not rows:
            raise RuntimeError(f"tdx-mcp 分钟K请求全部失败 ({'; '.join(failures[:3])})")
        return self._frame(rows, _MINUTE_SCHEMA).sort(["symbol", "datetime"])

    def get_depth5(self, symbols: list[str]) -> dict[str, dict]:
        """用 quote 的买卖五档映射 sealed 服务契约; 失败严格返回空, 不换源。"""
        if not symbols:
            return {}

        requested: dict[str, str] = {}
        for symbol in symbols:
            try:
                tdx_symbol = _to_tdx_symbol(symbol)
                canonical_symbol, _, _ = _from_tdx_symbol(tdx_symbol)
                requested[tdx_symbol] = canonical_symbol
            except ValueError as exc:
                logger.warning("tdx-mcp 五档跳过非法 symbol %s: %s", symbol, exc)
        if not requested:
            return {}

        try:
            with self._client_session() as client:
                quotes = client.get_quote(list(requested))
        except Exception as exc:
            logger.warning("tdx-mcp 五档拉取失败(%d 只): %s", len(requested), exc)
            return {}

        fallback_timestamp = int(time.time() * 1000)
        result: dict[str, dict] = {}
        for quote in quotes or []:
            try:
                returned_tdx = f"{str(quote.exchange).lower()}{str(quote.code).zfill(6)}"
                symbol = requested.get(returned_tdx)
                if symbol is None:
                    logger.warning("tdx-mcp 五档忽略未请求的响应: %s", returned_tdx)
                    continue
                server_time = getattr(quote, "server_time", None)
                timestamp = (
                    int(server_time.timestamp() * 1000)
                    if isinstance(server_time, datetime) and server_time.tzinfo is not None
                    else fallback_timestamp
                )
                result[symbol] = {
                    "ask_volumes": _level_numbers(getattr(quote, "sell_levels", None)),
                    "bid_volumes": _level_numbers(getattr(quote, "buy_levels", None)),
                    "timestamp": timestamp,
                }
            except Exception as exc:
                logger.warning("tdx-mcp 五档响应映射失败: %s", exc)
        return result

    @staticmethod
    def _quote_timestamp_ms(value: Any, fallback: int) -> int:
        """服务端报价时间优先; 协议仅含时分秒时安全回退到本轮拉取时间。"""
        if not isinstance(value, datetime):
            return fallback
        try:
            if value.tzinfo is None:
                # eltdx 对无日期的 TDX server_time 使用本机上海日期补全。跨日/休市
                # 场景无法由协议证明日期归属, 宁可用当前轮次时间避免伪造历史时间戳。
                return fallback
            return int(value.timestamp() * 1000)
        except (OverflowError, OSError, ValueError):
            return fallback

    def get_realtime(self) -> list[dict]:
        """将显式代码批量报价聚合为项目的全市场实时快照。

        这是软失败入口: TCP 或代码表错误不能终止行情轮询线程, 调用方会保留上一份
        有效快照。``eltdx`` 自己按最多 80 只/请求切批并复用两个 TCP 连接。
        """
        fallback_timestamp = int(time.time() * 1000)
        try:
            with self._client_session() as client:
                symbols = self._get_realtime_tdx_symbols(client)
                quotes = client.get_quote(list(symbols))
        except Exception as exc:
            logger.warning("tdx-mcp 全市场实时行情拉取失败: %s", exc)
            return []

        rows: list[dict] = []
        for quote in quotes or []:
            try:
                returned_tdx = f"{str(getattr(quote, 'exchange', '')).lower()}{str(getattr(quote, 'code', '')).zfill(6)}"
                symbol, _, _ = _from_tdx_symbol(returned_tdx)
                last_price = _finite_float(getattr(quote, "last_price", None))
                prev_close = _finite_float(getattr(quote, "last_close_price", None))
                open_price = _finite_float(getattr(quote, "open_price", None))
                high_price = _finite_float(getattr(quote, "high_price", None))
                low_price = _finite_float(getattr(quote, "low_price", None))
                volume = _finite_float(getattr(quote, "total_hand", None))
                amount = _finite_float(getattr(quote, "amount", None))
                change_amount = (
                    last_price - prev_close
                    if last_price is not None and prev_close is not None
                    else None
                )
                change_pct = (
                    change_amount / prev_close
                    if change_amount is not None and prev_close not in (None, 0.0)
                    else None
                )
                amplitude = (
                    (high_price - low_price) / prev_close
                    if high_price is not None and low_price is not None and prev_close not in (None, 0.0)
                    else None
                )
                rows.append({
                    "symbol": symbol,
                    "name": None,
                    "last_price": last_price,
                    "prev_close": prev_close,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    # Quote.total_hand 已是通达信的“手”; 不可再除以 100。
                    "volume": volume,
                    "amount": amount,
                    # change_pct / amplitude 均按项目契约使用小数制。
                    "change_pct": change_pct,
                    "change_amount": change_amount,
                    "amplitude": amplitude,
                    # Quote.rate 在协议/上游模型中无可靠业务语义, 不能猜作换手率。
                    "turnover_rate": None,
                    "timestamp": self._quote_timestamp_ms(
                        getattr(quote, "server_time", None), fallback_timestamp,
                    ),
                    "session": None,
                })
            except Exception as exc:
                logger.warning("tdx-mcp 实时行情响应映射失败: %s", exc)
        return rows

    def get_instruments(self, asset_type: AssetType | str = "stock") -> list[dict]:
        """从通达信代码表提供全量基础维表, 供首次全市场日K同步建立标的池。

        ``*_codes_all`` determines stock/index/ETF membership, while
        ``get_codes_all`` supplies real names. eltdx does not provide share capital
        or price-limit metadata, so those fields remain empty.
        """
        method_name = {
            "stock": "get_a_share_codes_all",
            "index": "get_index_codes_all",
            "etf": "get_etf_codes_all",
        }.get(str(asset_type).lower())
        if method_name is None:
            raise ValueError(f"不支持的标的类型: {asset_type!r}")

        try:
            with self._client_session() as client:
                tdx_symbols = getattr(client, method_name)()
                names_by_tdx_symbol = self._get_instrument_names(client)
        except Exception as exc:
            logger.warning("tdx-mcp %s 代码表拉取失败: %s", asset_type, exc)
            raise RuntimeError(f"tdx-mcp {asset_type} 代码表拉取失败: {exc}") from exc

        rows_by_symbol: dict[str, dict] = {}
        for tdx_symbol in tdx_symbols or []:
            try:
                symbol, code, exchange = _from_tdx_symbol(tdx_symbol)
            except ValueError as exc:
                logger.warning("tdx-mcp %s 代码表跳过异常值 %r: %s", asset_type, tdx_symbol, exc)
                continue
            rows_by_symbol[symbol] = {
                "symbol": symbol,
                # Keep a missing real name empty: a fake bare code would cause the
                # historical ST rule to treat a risk-warning stock as main-board.
                "name": names_by_tdx_symbol.get(str(tdx_symbol).strip().lower()) or None,
                "code": code,
                "exchange": exchange,
                "region": "CN",
                "type": str(asset_type).lower(),
                "ext": {},
            }
        rows = [rows_by_symbol[symbol] for symbol in sorted(rows_by_symbol)]
        if not rows:
            raise RuntimeError(f"tdx-mcp {asset_type} 代码表未返回可解析标的")
        return rows

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        """设置页试拉, 控制在小窗口, 避免单击测试触发单标的全历史分钟请求。"""
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
            "error": f"tdx-mcp 未声明 {dataset} 数据集",
        }

    def _preview(self, dataset: str, df: pl.DataFrame) -> dict:
        preview = df.head(5).to_dicts()
        for row in preview:
            for key, value in list(row.items()):
                if isinstance(value, datetime) or hasattr(value, "isoformat"):
                    row[key] = value.isoformat()
        return {
            "provider": self.name,
            "dataset": dataset,
            "rows": df.height,
            "columns": df.columns,
            "preview": preview,
        }
