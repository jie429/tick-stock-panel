"""tdx-api HTTP 数据源 Provider。

数据来自外部 Go 服务 tdx-api-main(通达信协议 → HTTP), 本插件只做协议归一。

上游单位口径(实测 + 上游 protocol/ 源码确证):
  - K线价格与成交额: 厘 (``protocol.Price``; model_kline.go 注明"从元转为厘") → ÷1000 得元
  - K线成交量: 手 (``protocol.Kline.Volume``) → 项目日K/分钟K同为手, 原样透传
  - 盘口 ``Amount``: 元 (``protocol.Quote.Amount`` 直接由 getVolume 解出, 与K线成交额
    相差 1000 倍), ``TotalHand`` 为手
  - 盘口五档 ``Number``: 手 (A 股最小交易单位 100 股, 档位值不可能以"股"为单位)
  - 盘口 K 价: 上游 server.go ``adjustQuotePrice`` 对 3 位小数品种(基金类, 代码表
    Decimal==3)额外 ÷10, 与同帧买卖档价差一个数量级 → 见 ``_k_scale``

上游不提供除权事件与财务报表接口, 因此不声明 adj_factor / financial; 指数分钟
(``/api/index?type=minute1``)上游只返回 100 条且与股票分钟口径不一致, 同样不声明。
"""
from __future__ import annotations

import logging
import math
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from app.data_providers.base import AssetType
from app.plugins.tdx_api import client as tdx_client

logger = logging.getLogger(__name__)

_DATASETS = ("daily", "minute", "realtime", "depth5")
_CN_TZ = ZoneInfo("Asia/Shanghai")
# 上游到通达信服务端的连接有限, 4 路并发实测稳定且不挤占其他调用方。
_KLINE_WORKERS = 4
_QUOTE_WORKERS = 4
# 代码表(5567 只股票 + 2205 只 ETF)按小时刷新即可, 避免每轮行情重复拉 200KB。
_UNIVERSE_TTL_S = 3600.0
# 按自然日跨度估算取数条数: A 股约 250 交易日/年; 超过上限直接取全量。
_TRADING_DAYS_PER_YEAR = 250
_MAX_DAILY_LIMIT = 8000
# 单批失败重试一次; 批量接口很轻(50 只 ~0.05s), 重试成本远低于残缺快照。
_BATCH_ATTEMPTS = 2

_SYMBOL_RE = re.compile(r"^(?P<code>\d{6})\.(?P<exchange>SH|SZ|BJ)$")
_EXCHANGE_BY_ID = {0: "SZ", 1: "SH", 2: "BJ"}

_DAILY_SCHEMA = {
    "symbol": pl.Utf8,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
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


@dataclass
class _TdxApiConfig:
    """让现有 custom loader 识别内置 Provider 的最小 config shim。"""

    name: str = "tdx_api"
    display_name: str = "tdx-api (通达信 HTTP)"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def availability() -> tuple[bool, str]:
    """loader 启动自检: tdx-api 服务在监听才注册为可切换数据源。不抛异常。"""
    base = tdx_client.configured_base_url()
    client: tdx_client.TdxApiClient | None = None
    try:
        client = tdx_client.TdxApiClient(base, timeout=tdx_client.PROBE_TIMEOUT)
        status = client.server_status()
        if str(status.get("status", "")).lower() == "running":
            return True, "ok"
        return False, f"tdx-api 服务未就绪({base})"
    except Exception as exc:
        return False, f"未检测到 tdx-api 服务({base}): {exc}"
    finally:
        if client is not None:
            client.close()


def _finite_float(value: Any) -> float | None:
    """上游数值收口为有限 float; 缺失/异常值不能伪造成 0。"""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _li_to_yuan(value: Any) -> float | None:
    """厘 → 元。K线价格与成交额统一走这里。"""
    number = _finite_float(value)
    return None if number is None else number / 1000.0


def _beijing_naive(value: datetime) -> datetime:
    """入参时间收口为北京 naive 墙钟(项目分钟K契约)。"""
    if value.tzinfo is None:
        return value
    return value.astimezone(_CN_TZ).replace(tzinfo=None)


def _parse_api_time(value: Any) -> datetime | None:
    """上游 RFC3339 时间(``+08:00``/``Z``) → 北京 naive 墙钟。"""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _beijing_naive(parsed)


def _split_symbol(symbol: str) -> tuple[str, str]:
    """项目 ``600519.SH`` → ``("600519", "SH")``; 非法代码直接报错。"""
    matched = _SYMBOL_RE.fullmatch(str(symbol or "").strip().upper())
    if matched is None:
        raise ValueError(f"symbol 必须为带交易所后缀的六码代码(如 600519.SH), 得到: {symbol!r}")
    return matched.group("code"), matched.group("exchange")


def _to_api_code(symbol: str) -> str:
    """项目 ``600519.SH`` → 上游带前缀代码 ``sh600519``。

    quote / batch-quote 走上游 ``AddPrefix`` 按代码段猜交易所, 该规则不覆盖 ETF 的
    56/58/16 段, 会直接报"股票代码长度错误"; 显式带前缀由 ``DecodeCode`` 直接解析,
    对股票 / ETF / 指数一致。
    """
    code, exchange = _split_symbol(symbol)
    return f"{exchange.lower()}{code}"


def _to_bare_code(symbol: str) -> str:
    """项目 ``600519.SH`` → 6 位裸码(仅 ``/api/kline*`` 的股票/ETF 路径需要)。"""
    return _split_symbol(symbol)[0]


def _canonical_symbol(code: Any, exchange: Any) -> str | None:
    """上游 ``000001`` + ``sz``/``0`` → 项目 ``000001.SZ``。"""
    text = str(code or "").strip()
    if text.isdigit():
        text = text.zfill(6)
    if len(text) != 6 or not text.isdigit():
        return None
    if isinstance(exchange, bool):
        return None
    if isinstance(exchange, int):
        name = _EXCHANGE_BY_ID.get(exchange)
    else:
        name = str(exchange or "").strip().upper() or None
    if name not in {"SH", "SZ", "BJ"}:
        return None
    return f"{text}.{name}"


def _daily_limit(start_time: datetime | None, end_time: datetime | None) -> int | None:
    """按窗口估算需要的最近 K 线条数(上游仍先取全量再截断, 这里只为缩小响应体)。"""
    if start_time is None:
        return None
    start = _beijing_naive(start_time).date()
    end = _beijing_naive(end_time).date() if end_time is not None else datetime.now(_CN_TZ).date()
    span = (end - start).days
    if span <= 0:
        return 30
    estimated = int(span * _TRADING_DAYS_PER_YEAR / 365) + 30
    return None if estimated >= _MAX_DAILY_LIMIT else estimated


def _level_numbers(levels: Any) -> list[int | None]:
    """盘口档位量(手); 缺档用 None, 绝不能伪造成 0(会误判为真封板)。"""
    values: list[int | None] = []
    for level in list(levels or [])[:5]:
        number = level.get("Number") if isinstance(level, dict) else None
        number = _finite_float(number)
        values.append(None if number is None else int(number))
    return (values + [None] * 5)[:5]


def _level_prices(levels: Any) -> list[float | None]:
    """盘口档位价(元); 缺档 None。"""
    values: list[float | None] = []
    for level in list(levels or [])[:5]:
        price = level.get("Price") if isinstance(level, dict) else None
        values.append(_li_to_yuan(price))
    return (values + [None] * 5)[:5]


def _k_scale(k: dict, reference: float | None, *, is_fund: bool) -> float:
    """盘口 K 价的厘→元缩放系数(1/1000 或 1/100)。

    上游对 3 位小数品种把 K 价额外 ÷10, 而同帧买卖档价未调整。买卖一价必然落在
    最新价附近, 故以同帧档位价作量纲参照: 档位价(元)折回厘后与 K 原始值比值落在
    10 附近, 即说明 K 已被 ÷10, 否则按常规厘处理。无档位(停牌/无报价)时退回品种
    判定, 不凭数值大小猜单位。
    """
    if reference is not None and reference > 0:
        reference_li = reference * 1000.0
        for key in ("Close", "Last", "Open", "High", "Low"):
            value = _finite_float(k.get(key))
            if value is None or value <= 0:
                continue
            ratio = reference_li / value
            return 1.0 / 100.0 if 8.0 <= ratio <= 12.0 else 1.0 / 1000.0
    return 1.0 / 100.0 if is_fund else 1.0 / 1000.0


class TdxApiProvider:
    """通达信 HTTP 数据源(股票/ETF 原始日K、1分钟K、全市场快照、五档盘口)。"""

    daily_asset_types = frozenset({"stock", "etf", "index"})
    instrument_asset_types = frozenset({"stock", "etf", "index"})
    # ``kline_sync`` 仅向显式声明该能力的 Provider 传入失败标的出参。
    supports_daily_failure_reporting = True
    supports_minute_failure_reporting = True
    # 全市场快照需约 155 批 HTTP 请求, 不能按普通插件的秒级下限轮询。
    realtime_min_interval = 30.0

    def __init__(self, base_url: str | None = None) -> None:
        self.config = _TdxApiConfig()
        self._base_url = base_url
        self._client: tdx_client.TdxApiClient | None = None
        self._client_lock = threading.Lock()
        self._universe_lock = threading.Lock()
        self._universe: list[dict] | None = None
        self._fund_symbols: frozenset[str] = frozenset()
        self._universe_expires_at = 0.0

    # ---------------------------------------------------------------- 生命周期

    def close(self) -> None:
        with self._client_lock:
            client, self._client = self._client, None
        if client is not None:
            client.close()

    def _get_client(self) -> tdx_client.TdxApiClient:
        with self._client_lock:
            if self._client is None:
                self._client = tdx_client.TdxApiClient(self._base_url)
            return self._client

    # ---------------------------------------------------------------- 代码池

    def _code_universe(self) -> tuple[list[dict], frozenset[str]]:
        """全市场代码池(股票+ETF)与基金代码集合, 按 TTL 缓存。

        实时行情与五档都按代码池批量请求; 基金集合用于盘口 K 价量纲兜底。
        """
        now = time.monotonic()
        with self._universe_lock:
            if self._universe is not None and now < self._universe_expires_at:
                return self._universe, self._fund_symbols

        client = self._get_client()
        rows: list[dict] = []
        funds: set[str] = set()
        for item in client.list_stock_codes():
            symbol = _canonical_symbol(item.get("code"), item.get("exchange"))
            if symbol is None:
                continue
            rows.append({"symbol": symbol, "name": item.get("name") or None, "fund": False})
        for item in client.list_etf():
            symbol = _canonical_symbol(item.get("code"), item.get("exchange"))
            if symbol is None:
                continue
            funds.add(symbol)
            rows.append({"symbol": symbol, "name": item.get("name") or None, "fund": True})

        deduped: dict[str, dict] = {}
        for row in rows:
            deduped[row["symbol"]] = row
        universe = [deduped[key] for key in sorted(deduped)]
        if not universe:
            raise tdx_client.TdxApiError("tdx-api 代码表为空")

        with self._universe_lock:
            self._universe = universe
            self._fund_symbols = frozenset(funds)
            self._universe_expires_at = time.monotonic() + _UNIVERSE_TTL_S
        logger.info("tdx-api 代码池刷新: %d 只(含 %d 只基金)", len(universe), len(funds))
        return universe, self._fund_symbols

    # ---------------------------------------------------------------- 维表

    def get_instruments(self, asset_type: AssetType | str = "stock") -> list[dict]:
        """股票/ETF 用上游代码表; 指数沿用产品级核心指数清单(上游无指数枚举接口)。"""
        kind = str(asset_type).lower()
        client = self._get_client()
        if kind == "stock":
            source = client.list_stock_codes()
        elif kind == "etf":
            source = client.list_etf()
        elif kind == "index":
            from app.services.index_const import CORE_INDEX_NAMES

            return [
                {
                    "symbol": symbol,
                    "name": name,
                    "code": symbol.split(".")[0],
                    "exchange": symbol.split(".")[1],
                    "region": "CN",
                    "type": "index",
                    "ext": {},
                }
                for symbol, name in sorted(CORE_INDEX_NAMES.items())
            ]
        else:
            raise ValueError(f"tdx-api 不支持 {asset_type!r} 标的维表")

        rows: dict[str, dict] = {}
        for item in source:
            symbol = _canonical_symbol(item.get("code"), item.get("exchange"))
            if symbol is None:
                continue
            code, _, exchange = symbol.partition(".")
            rows[symbol] = {
                "symbol": symbol,
                "name": item.get("name") or None,
                "code": code,
                "exchange": exchange,
                "region": "CN",
                "type": kind,
                "ext": {},
            }
        if not rows:
            raise RuntimeError(f"tdx-api {kind} 代码表未返回可解析标的")
        return [rows[key] for key in sorted(rows)]

    # ---------------------------------------------------------------- 日K

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType | str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
        failed_out: list[str] | None = None,
    ) -> pl.DataFrame:
        """原始(不复权)日K; 单标的失败不覆盖其他已成功数据。"""
        if not symbols:
            return self._frame([], _DAILY_SCHEMA)
        kind = str(asset_type).lower()
        if kind not in {"stock", "etf", "index"}:
            raise ValueError(f"tdx-api 日K不支持资产类型: {asset_type!r}")

        is_index = kind == "index"
        limit = _daily_limit(start_time, end_time)
        start_date = _beijing_naive(start_time).date() if start_time is not None else None
        end_date = _beijing_naive(end_time).date() if end_time is not None else None

        rows: list[dict] = []
        failures: list[str] = []
        failed_symbols: list[str] = []
        total = len(symbols)

        def fetch(symbol: str) -> tuple[str, list[dict]]:
            client = self._get_client()
            if is_index:
                return symbol, client.index_all(_to_api_code(symbol), "day", limit)
            return symbol, client.kline_all_tdx(_to_bare_code(symbol), "day", limit)

        for symbol, payload in self._run_concurrent(
            symbols, fetch, on_chunk_done=on_chunk_done,
            failures=failures, failed_symbols=failed_symbols,
        ):
            rows.extend(self._daily_rows(symbol, payload, start_date, end_date))

        self._log_partial_failure("日K", failures, failed_symbols, total, failed_out)
        if failures and not rows:
            raise RuntimeError(f"tdx-api 日K请求全部失败 ({'; '.join(failures[:3])})")
        return self._frame(rows, _DAILY_SCHEMA).sort(["symbol", "date"])

    @staticmethod
    def _daily_rows(
        symbol: str,
        payload: list[dict],
        start_date: Any,
        end_date: Any,
    ) -> list[dict]:
        rows_by_date: dict[Any, dict] = {}
        for item in payload:
            trade_date = _parse_api_time(item.get("Time"))
            if trade_date is None:
                continue
            day = trade_date.date()
            if start_date is not None and day < start_date:
                continue
            if end_date is not None and day > end_date:
                continue
            rows_by_date[day] = {
                "symbol": symbol,
                "date": day,
                "open": _li_to_yuan(item.get("Open")),
                "high": _li_to_yuan(item.get("High")),
                "low": _li_to_yuan(item.get("Low")),
                "close": _li_to_yuan(item.get("Close")),
                # 上游 K 线成交量已是「手」, 不可再换算。
                "volume": _finite_float(item.get("Volume")),
                "amount": _li_to_yuan(item.get("Amount")),
            }
        return [rows_by_date[key] for key in sorted(rows_by_date)]

    # ---------------------------------------------------------------- 分钟K

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
        """1 分钟原始 K(上游只保留最近约 90 个交易日); 指数分钟上游口径不足, 不提供。"""
        if str(freq).lower() not in {"1m", "1min", "1minute"}:
            raise ValueError(f"tdx-api 仅支持 1m 分钟K, 得到: {freq!r}")
        kind = str(asset_type).lower()
        if kind == "index":
            logger.info("tdx-api 不提供指数分钟K(上游指数分钟仅 100 条且口径不一致)")
            return self._frame([], _MINUTE_SCHEMA)
        if kind not in {"stock", "etf"}:
            raise ValueError(f"tdx-api 分钟K不支持资产类型: {asset_type!r}")
        if not symbols:
            return self._frame([], _MINUTE_SCHEMA)

        start = _beijing_naive(start_time) if start_time is not None else None
        end = _beijing_naive(end_time) if end_time is not None else None
        rows: list[dict] = []
        failures: list[str] = []
        failed_symbols: list[str] = []
        total = len(symbols)

        def fetch(symbol: str) -> tuple[str, list[dict]]:
            return symbol, self._get_client().kline(_to_bare_code(symbol), "minute1")

        for symbol, payload in self._run_concurrent(
            symbols, fetch, on_chunk_done=on_chunk_done,
            failures=failures, failed_symbols=failed_symbols,
        ):
            rows.extend(self._minute_rows(symbol, payload, start, end))

        self._log_partial_failure("分钟K", failures, failed_symbols, total, failed_out)
        if failures and not rows:
            raise RuntimeError(f"tdx-api 分钟K请求全部失败 ({'; '.join(failures[:3])})")
        return self._frame(rows, _MINUTE_SCHEMA).sort(["symbol", "datetime"])

    @staticmethod
    def _minute_rows(
        symbol: str,
        payload: list[dict],
        start: datetime | None,
        end: datetime | None,
    ) -> list[dict]:
        rows_by_time: dict[datetime, dict] = {}
        for item in payload:
            wallclock = _parse_api_time(item.get("Time"))
            if wallclock is None:
                continue
            if start is not None and wallclock < start:
                continue
            if end is not None and wallclock > end:
                continue
            rows_by_time[wallclock] = {
                "symbol": symbol,
                "datetime": wallclock,
                "open": _li_to_yuan(item.get("Open")),
                "high": _li_to_yuan(item.get("High")),
                "low": _li_to_yuan(item.get("Low")),
                "close": _li_to_yuan(item.get("Close")),
                "volume": _finite_float(item.get("Volume")),
                "amount": _li_to_yuan(item.get("Amount")),
            }
        return [rows_by_time[key] for key in sorted(rows_by_time)]

    # ---------------------------------------------------------------- 实时行情

    def get_realtime(self) -> list[dict]:
        """全市场(股票+ETF)快照 → 项目 realtime records。

        软失败入口: 代码表或任一批次不可用即返回空列表, 调用方保留上一份有效
        快照, 不能用残缺的全市场数据覆盖缓存。
        """
        try:
            universe, fund_symbols = self._code_universe()
            quotes = self._fetch_quote_batches([row["symbol"] for row in universe])
        except Exception as exc:
            logger.warning("tdx-api 全市场实时行情拉取失败: %s", exc)
            return []

        names = {row["symbol"]: row["name"] for row in universe}
        fetched_ms = int(time.time() * 1000)
        rows: list[dict] = []
        for quote in quotes:
            record = self._realtime_record(quote, names, fund_symbols, fetched_ms)
            if record is not None:
                rows.append(record)
        if not rows:
            logger.warning("tdx-api 快照未返回可解析记录, 疑似接口结构变化")
        return rows

    def _realtime_record(
        self,
        quote: dict,
        names: dict[str, str | None],
        fund_symbols: frozenset[str],
        fetched_ms: int,
    ) -> dict | None:
        symbol = _canonical_symbol(quote.get("Code"), quote.get("Exchange"))
        if symbol is None:
            return None
        k = quote.get("K") if isinstance(quote.get("K"), dict) else {}
        buy_levels = quote.get("BuyLevel")
        sell_levels = quote.get("SellLevel")
        buy_prices = _level_prices(buy_levels)
        sell_prices = _level_prices(sell_levels)
        reference = next((p for p in [*buy_prices, *sell_prices] if p), None)
        scale = _k_scale(k, reference, is_fund=symbol in fund_symbols)

        def price(key: str) -> float | None:
            value = _finite_float(k.get(key))
            return None if value is None else value * scale

        last_price = price("Close")
        prev_close = price("Last")
        open_price = price("Open")
        high_price = price("High")
        low_price = price("Low")
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
        return {
            "symbol": symbol,
            "name": names.get(symbol),
            "last_price": last_price,
            "prev_close": prev_close,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            # 盘口 TotalHand 已是「手」, 不可再除以 100。
            "volume": _finite_float(quote.get("TotalHand")),
            # 盘口 Amount 单位为元(与K线成交额的厘差 1000 倍)。
            "amount": _finite_float(quote.get("Amount")),
            # change_pct / amplitude 按项目契约使用小数制。
            "change_pct": change_pct,
            "change_amount": change_amount,
            "amplitude": amplitude,
            # 上游无股本数据, 换手率置 None 由 pipeline 按可用输入决定, 不伪造。
            "turnover_rate": None,
            # 上游 ServerTime 为协议内部计数而非 Unix 时间, 用本轮拉取时刻归属。
            "timestamp": fetched_ms,
            "session": None,
        }

    # ---------------------------------------------------------------- 指数实时

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        """指数实时快照 → 内部 realtime record(可选插件协议, quote_service 鸭子调用)。

        指数不在上游 A 股代码表里, 因此不随全市场快照返回; 但 ``/api/batch-quote``
        对带交易所前缀的指数码有效(实测 ``sh000001``/``sz399006``/``bj899050`` 均
        返回点位, 单位同为厘), 故指数单独补拉。失败返回 None, 与“成功但无数据”的
        空列表区分, 让上层保留上轮有效指数缓存。
        """
        wanted: list[str] = []
        for symbol in symbols or []:
            try:
                _to_api_code(symbol)
            except ValueError as exc:
                logger.warning("tdx-api 指数行情跳过非法 symbol %s: %s", symbol, exc)
                continue
            wanted.append(str(symbol).strip().upper())
        if not wanted:
            return []

        try:
            quotes = self._fetch_quote_batches(wanted)
        except Exception as exc:
            logger.warning("tdx-api 指数行情拉取失败(%d 只): %s", len(wanted), exc)
            return None

        # 上游盘口不含名称; 核心指数用本地常量补名, 其余留空由展示层兜底。
        from app.services.index_const import CORE_INDEX_NAMES

        names = {symbol: CORE_INDEX_NAMES.get(symbol) for symbol in wanted}
        fetched_ms = int(time.time() * 1000)
        records: list[dict] = []
        for quote in quotes:
            record = self._realtime_record(quote, names, frozenset(), fetched_ms)
            if record is not None:
                records.append(record)
        logger.info("tdx-api 指数行情拉取完成: %d 条(请求 %d 只)", len(records), len(wanted))
        return records

    # ---------------------------------------------------------------- 五档

    def get_depth5(self, symbols: list[str]) -> dict[str, dict]:
        """五档挂单量(手); 失败严格返回空, 不换源。"""
        if not symbols:
            return {}
        requested: dict[str, str] = {}
        for symbol in symbols:
            try:
                _to_api_code(symbol)
            except ValueError as exc:
                logger.warning("tdx-api 五档跳过非法 symbol %s: %s", symbol, exc)
                continue
            canonical = str(symbol).strip().upper()
            requested[canonical] = canonical
        if not requested:
            return {}

        try:
            quotes = self._fetch_quote_batches(list(requested))
        except Exception as exc:
            logger.warning("tdx-api 五档拉取失败(%d 只): %s", len(requested), exc)
            return {}

        fetched_ms = int(time.time() * 1000)
        result: dict[str, dict] = {}
        for quote in quotes:
            symbol = _canonical_symbol(quote.get("Code"), quote.get("Exchange"))
            if symbol is None or symbol not in requested:
                if symbol is not None:
                    logger.warning("tdx-api 五档忽略未请求的响应: %s", symbol)
                continue
            result[symbol] = {
                "ask_volumes": _level_numbers(quote.get("SellLevel")),
                "bid_volumes": _level_numbers(quote.get("BuyLevel")),
                "timestamp": fetched_ms,
            }
        return result

    # ---------------------------------------------------------------- 内部工具

    def _fetch_quote_batches(self, symbols: list[str]) -> list[dict]:
        """按上游 50 只/批并发取行情; 任一批次最终失败则整轮作废。"""
        if not symbols:
            return []
        batches = [
            symbols[index:index + tdx_client.BATCH_QUOTE_LIMIT]
            for index in range(0, len(symbols), tdx_client.BATCH_QUOTE_LIMIT)
        ]
        client = self._get_client()
        quotes: list[dict] = []
        errors: list[str] = []
        mismatched: list[str] = []

        def fetch(batch: list[str]) -> list[dict]:
            last_exc: Exception | None = None
            for attempt in range(_BATCH_ATTEMPTS):
                try:
                    rows = client.batch_quote([_to_api_code(symbol) for symbol in batch])
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt + 1 < _BATCH_ATTEMPTS:
                        time.sleep(0.2)
            else:
                raise last_exc if last_exc else RuntimeError("tdx-api 批量行情失败")

            # 上游按请求顺序逐条回填; 对不支持的代码会回一条占位记录(实测北交所
            # 旧代码 430047/830799 返回 600839), 位置与代码对不上就丢弃, 宁缺不错。
            picked: list[dict] = []
            for expected, row in zip(batch, rows, strict=False):
                actual = _canonical_symbol(row.get("Code"), row.get("Exchange"))
                if actual != expected:
                    mismatched.append(f"{expected}->{actual}")
                    continue
                picked.append(row)
            if len(rows) != len(batch):
                mismatched.append(f"批次条数 {len(rows)}/{len(batch)}")
            return picked

        with ThreadPoolExecutor(max_workers=_QUOTE_WORKERS) as pool:
            futures = {pool.submit(fetch, batch): batch for batch in batches}
            for future in as_completed(futures):
                try:
                    quotes.extend(future.result())
                except Exception as exc:
                    errors.append(str(exc))
        if mismatched:
            logger.warning(
                "tdx-api 行情丢弃 %d 条不匹配响应(样例: %s)", len(mismatched), mismatched[:5],
            )
        if errors:
            raise tdx_client.TdxApiError(
                f"{len(errors)}/{len(batches)} 批行情失败(样例: {errors[0]})",
            )
        return quotes

    def _run_concurrent(
        self,
        symbols: list[str],
        fetch: Callable[[str], tuple[str, list[dict]]],
        *,
        on_chunk_done: Callable[[int, int], None] | None,
        failures: list[str],
        failed_symbols: list[str],
    ) -> list[tuple[str, list[dict]]]:
        """并发拉取单标的数据; 失败逐标的记录, 进度回调按完成数递增。"""
        total = len(symbols)
        results: list[tuple[str, list[dict]]] = []
        progress_lock = threading.Lock()
        done = 0

        def worker(symbol: str):
            nonlocal done
            try:
                return fetch(symbol)
            finally:
                if on_chunk_done is not None:
                    with progress_lock:
                        done += 1
                        current = done
                    on_chunk_done(current, total)

        with ThreadPoolExecutor(max_workers=_KLINE_WORKERS) as pool:
            futures = {pool.submit(worker, symbol): symbol for symbol in symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    failed = str(symbol or "").strip().upper()
                    failures.append(f"{failed}: {exc}")
                    failed_symbols.append(failed)
                    logger.warning("tdx-api 拉取失败 %s: %s", symbol, exc)
        return results

    @staticmethod
    def _log_partial_failure(
        label: str,
        failures: list[str],
        failed_symbols: list[str],
        total: int,
        failed_out: list[str] | None,
    ) -> None:
        if not failed_symbols:
            return
        logger.warning(
            "tdx-api %s部分失败: %d/%d 标的未获取 (样例: %s)",
            label, len(failed_symbols), total, failed_symbols[:10],
        )
        if failed_out is not None:
            failed_out.extend(failed_symbols)

    @staticmethod
    def _frame(rows: list[dict], schema: dict) -> pl.DataFrame:
        return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)

    # ---------------------------------------------------------------- 试拉

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        """设置页「试拉测试」: 用小窗口, 避免单击测试触发全量请求。"""
        selected = list(symbols or [])[:3] or ["600000.SH"]
        end_time = datetime.now(_CN_TZ).replace(tzinfo=None)
        start_time = end_time - timedelta(days=30)
        try:
            if dataset == "daily":
                return self._preview(dataset, self.get_daily(selected, start_time, end_time))
            if dataset == "minute":
                return self._preview(
                    dataset, self.get_minute(selected[:1], end_time - timedelta(days=3), end_time),
                )
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
            "error": f"tdx-api 未声明 {dataset} 数据集",
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

    @property
    def name(self) -> str:
        return self.config.name
