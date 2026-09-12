"""麦蕊智数内置 Provider。

当前项目契约可用能力:
- daily: 沪深 A 股不复权日 K, 成交量原生为手;
- realtime: 官方多股接口每批最多 20 只, 全市场分批, 百分数在边界转小数;
- depth5: 五档数量原生为手, 缺档保留 None;
- financial: 三大报表、主要指标和历史股本, 公告日独立保留。

当前 licence 实测未开通 Quant Pro 1 分钟接口; 普通接口最细为 5 分钟, 而项目
``minute`` 契约固定消费 1 分钟 K, 因此不声明 minute/full_minute。官方没有独立、
完整的除权事件接口, 近年分红也不覆盖配股, 故不声明 adj_factor。
"""
from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from app.data_providers.normalizer import normalize_daily
from app.plugins.mairui.client import MairuiClient, MairuiError

logger = logging.getLogger(__name__)

API_KEY_ENV = "MAIRUI_LICENSE"
SECRETS_FIELD = "mairui_api_key"
_DATASETS = ("daily", "realtime", "depth5", "financial")
_CN_TZ = ZoneInfo("Asia/Shanghai")
_DAILY_SCHEMA = {
    "symbol": pl.String,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
}
_FINANCIAL_ENDPOINTS = {
    "metrics": "pershareindex",
    "income": "income",
    "balance_sheet": "balance",
    "cash_flow": "cashflow",
    "shares": "capital",
}


def get_license() -> str:
    from app import secrets_store

    return secrets_store.get_env_backed_secret(SECRETS_FIELD, API_KEY_ENV)


def availability() -> tuple[bool, str]:
    if get_license():
        return True, "ok"
    return False, f"缺少 licence (可在下方输入框填写, 或配置环境变量 {API_KEY_ENV})"


def probe_api_key(api_key: str) -> tuple[bool, str]:
    if not str(api_key or "").strip():
        return False, "licence 不能为空"
    client = None
    try:
        client = MairuiClient(licence=api_key, timeout=10.0, request_interval=0)
        client.stock_list()
        return True, "ok"
    except MairuiError as exc:
        return False, f"licence 无效或网络失败: {exc}"
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()


@dataclass
class _MairuiConfig:
    name: str = "mairui"
    display_name: str = "麦蕊智数"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def _number(value) -> float | None:
    if value in (None, "", "-", "--"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_text(value) -> str | None:
    text = str(value or "").strip()
    if not text or text in {"-", "--"}:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _timestamp_ms(value) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d%H:%M:%S"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=_CN_TZ).timestamp() * 1000)
        except ValueError:
            continue
    return None


def _normalize_symbol(value: str) -> str:
    text = str(value or "").strip().upper()
    if "." not in text:
        raise ValueError(f"股票代码必须包含交易所后缀: {value!r}")
    code, exchange = text.rsplit(".", 1)
    if exchange not in {"SH", "SZ"}:
        raise ValueError(f"麦蕊沪深数据不支持交易所: {exchange}")
    if len(code) != 6 or not code.isdigit():
        raise ValueError(f"非法股票代码: {value!r}")
    return f"{code}.{exchange}"


def _bare_code(symbol: str) -> str:
    return _normalize_symbol(symbol).split(".", 1)[0]


def _pad_levels(value) -> list[float | None]:
    values = value if isinstance(value, (list, tuple)) else []
    out = [_number(item) for item in values[:5]]
    return out + [None] * (5 - len(out))


class MairuiProvider:
    name = "mairui"
    builtin = True
    daily_asset_types = frozenset({"stock"})
    instrument_asset_types = frozenset({"stock"})
    fallback_to_tickflow_on_error = False
    supports_daily_failure_reporting = True
    realtime_min_interval = 60.0

    def __init__(self) -> None:
        self.config = _MairuiConfig()
        self._client: MairuiClient | None = None
        self._instruments: list[dict] | None = None
        self._symbol_by_code: dict[str, str] = {}
        self._name_by_code: dict[str, str] = {}

    def close(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None
        self._instruments = None
        self._symbol_by_code.clear()
        self._name_by_code.clear()

    def _get_client(self) -> MairuiClient:
        if self._client is None:
            self._client = MairuiClient(licence=get_license())
        return self._client

    def get_instruments(self, asset_type: str = "stock") -> list[dict]:
        if asset_type != "stock":
            return []
        if self._instruments is not None:
            return [dict(row) for row in self._instruments]
        rows: list[dict] = []
        symbol_by_code: dict[str, str] = {}
        name_by_code: dict[str, str] = {}
        for raw in self._get_client().stock_list():
            raw_code = str(raw.get("dm") or "").strip().upper()
            exchange = str(raw.get("jys") or "").strip().upper()
            if "." in raw_code:
                code, suffix = raw_code.rsplit(".", 1)
                if suffix in {"SH", "SZ"}:
                    exchange = suffix
            else:
                code = raw_code
            if len(code) != 6 or not code.isdigit() or exchange not in {"SH", "SZ"}:
                continue
            symbol = f"{code}.{exchange}"
            name = str(raw.get("mc") or symbol).strip()
            symbol_by_code[code] = symbol
            name_by_code[code] = name
            rows.append({
                "symbol": symbol,
                "name": name,
                "code": code,
                "exchange": exchange,
                "region": "CN",
                "type": "stock",
                "ext": {},
            })
        if not rows:
            raise MairuiError("股票列表未返回可用沪深标的")
        self._instruments = rows
        self._symbol_by_code = symbol_by_code
        self._name_by_code = name_by_code
        return [dict(row) for row in rows]

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
        failed_out: list[str] | None = None,
    ) -> pl.DataFrame:
        if not symbols or asset_type != "stock":
            return pl.DataFrame(schema=_DAILY_SCHEMA)
        rows: list[dict] = []
        failures: list[str] = []
        total = len(symbols)
        for index, raw_symbol in enumerate(symbols):
            symbol = str(raw_symbol or "").strip().upper()
            try:
                symbol = _normalize_symbol(symbol)
                bars = self._get_client().history(
                    symbol, "d", "n", start=start_time, end=end_time,
                )
                for bar in bars:
                    if int(_number(bar.get("sf")) or 0) == 1:
                        continue
                    trade_date = _date_text(bar.get("t"))
                    if trade_date is None:
                        continue
                    rows.append({
                        "symbol": symbol,
                        "date": trade_date,
                        "open": _number(bar.get("o")),
                        "high": _number(bar.get("h")),
                        "low": _number(bar.get("l")),
                        "close": _number(bar.get("c")),
                        # 官方文档与实测 v 均为手, 不做 /100。
                        "volume": _number(bar.get("v")),
                        "amount": _number(bar.get("a")),
                    })
            except (MairuiError, ValueError) as exc:
                failures.append(symbol)
                logger.warning("麦蕊日K %s 拉取失败: %s", symbol, exc)
            finally:
                if on_chunk_done is not None:
                    on_chunk_done(index + 1, total)
        if failures and failed_out is not None:
            failed_out.extend(failures)
        if failures and not rows:
            raise RuntimeError(f"麦蕊日K请求全部失败 ({len(failures)}/{total})")
        if not rows:
            return pl.DataFrame(schema=_DAILY_SCHEMA)
        return normalize_daily(rows, source=self.name).sort(["symbol", "date"])

    def get_realtime(self) -> list[dict]:
        try:
            self.get_instruments("stock")
            codes = list(self._symbol_by_code)
            records: list[dict] = []
            for start in range(0, len(codes), 20):
                chunk = codes[start:start + 20]
                batch = self._get_client().realtime(chunk)
                returned = {str(row.get("dm") or "").strip(): row for row in batch}
                if any(code not in returned for code in chunk):
                    raise MairuiError("多股实时接口返回不完整")
                for code in chunk:
                    row = returned[code]
                    timestamp = _timestamp_ms(row.get("t"))
                    last_price = _number(row.get("p"))
                    prev_close = _number(row.get("yc"))
                    change_amount = _number(row.get("ud"))
                    pct = _number(row.get("pc"))
                    amplitude = _number(row.get("zf"))
                    turnover = _number(row.get("tr"))
                    records.append({
                        "symbol": self._symbol_by_code[code],
                        "name": self._name_by_code.get(code),
                        "last_price": last_price,
                        "prev_close": prev_close,
                        "open": _number(row.get("o")),
                        "high": _number(row.get("h")),
                        "low": _number(row.get("l")),
                        "volume": _number(row.get("v")),
                        "amount": _number(row.get("cje")),
                        "change_pct": pct / 100.0 if pct is not None else (
                            change_amount / prev_close
                            if change_amount is not None and prev_close not in (None, 0)
                            else None
                        ),
                        "change_amount": change_amount,
                        "amplitude": amplitude / 100.0 if amplitude is not None else None,
                        "turnover_rate": turnover / 100.0 if turnover is not None else None,
                        "timestamp": timestamp,
                        "session": None,
                    })
            return records
        except (MairuiError, ValueError) as exc:
            logger.warning("麦蕊全市场实时行情拉取失败, 本轮保留旧缓存: %s", exc)
            return []

    def get_depth5(self, symbols: list[str]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for raw_symbol in symbols:
            try:
                symbol = _normalize_symbol(raw_symbol)
                row = self._get_client().depth5(_bare_code(symbol))
                if not row:
                    continue
                timestamp = _timestamp_ms(row.get("t"))
                ask = _pad_levels(row.get("vs"))
                bid = _pad_levels(row.get("vb"))
                if timestamp is None or not any(value is not None for value in ask + bid):
                    continue
                result[symbol] = {
                    "ask_volumes": ask,
                    "bid_volumes": bid,
                    "timestamp": timestamp,
                }
            except (MairuiError, ValueError) as exc:
                logger.warning("麦蕊五档 %s 拉取失败: %s", raw_symbol, exc)
        return result

    def get_financials(
        self,
        table: str,
        symbols: list[str],
        latest_only: bool = True,
    ) -> pl.DataFrame:
        endpoint = _FINANCIAL_ENDPOINTS.get(table)
        if endpoint is None or not symbols:
            return pl.DataFrame()
        rows: list[dict] = []
        for raw_symbol in symbols:
            try:
                symbol = _normalize_symbol(raw_symbol)
                raw_rows = self._get_client().financial(endpoint, symbol)
                mapped = [self._map_financial(table, symbol, row) for row in raw_rows]
                mapped = [row for row in mapped if row["period_end"] is not None]
                mapped.sort(key=lambda row: row["period_end"], reverse=True)
                rows.extend(mapped[:1] if latest_only else mapped)
            except (MairuiError, ValueError) as exc:
                logger.warning("麦蕊财务 %s %s 拉取失败: %s", table, raw_symbol, exc)
        return pl.DataFrame(rows) if rows else pl.DataFrame()

    @staticmethod
    def _map_financial(table: str, symbol: str, raw: dict) -> dict:
        if table == "shares":
            row = {
                "symbol": symbol,
                "period_end": _date_text(raw.get("bdrq")),
                "announce_date": _date_text(raw.get("plrq")),
                "total_shares": _number(raw.get("zgb")),
                "float_shares": _number(raw.get("ysltag")),
                "restricted_shares": _number(raw.get("xsltgf")),
            }
        else:
            row = {
                "symbol": symbol,
                "period_end": _date_text(raw.get("jzrq")),
                "announce_date": _date_text(raw.get("plrq")),
            }

        maps: dict[str, dict[str, tuple[str, ...]]] = {
            "income": {
                "revenue": ("yysr", "yyzsr"),
                "operating_cost": ("yycb", "yyzcb"),
                "selling_expense": ("xsfy",),
                "admin_expense": ("glfy",),
                "rd_expense": ("yffy",),
                "financial_expense": ("cwfy",),
                "operating_profit": ("yylr",),
                "total_profit": ("lrze",),
                "income_tax": ("sdsfy",),
                "net_income": ("jlr",),
                "net_income_attributable": ("gsmgsyzzdjlr",),
                "basic_eps": ("jbmgsy",),
            },
            "balance_sheet": {
                "total_assets": ("zczj",),
                "total_current_assets": ("ldzchj",),
                "total_non_current_assets": ("fldzchj",),
                "cash_and_equivalents": ("hbzj",),
                "accounts_receivable": ("yszk",),
                "total_liabilities": ("fzhj",),
                "total_equity": ("syzqyhj",),
            },
            "cash_flow": {
                "net_operating_cash_flow": ("jyhdcsdxjlje", "jyhdcsdxjlxj"),
                "net_investing_cash_flow": ("tzhdcsdxjlxj",),
                "net_financing_cash_flow": ("czhdcsdxjlxj",),
                "capex": ("gjgdzcwxzhqtqctzzfdxj",),
                "net_cash_change": ("xjxjdhwjzje", "xjxjdhwdjzje"),
            },
            "metrics": {
                "roe": ("jqjzcsyl", "jzcsyl"),
                "roa": ("tbzzcsyl",),
                "gross_margin": ("xsmlv", "mlv"),
                "net_margin": ("jlv",),
                "debt_to_asset_ratio": ("zcfzl",),
                "revenue_yoy": ("zyyrsrzz",),
                "net_income_yoy": ("gsmgsyzzdjlrzz", "jlrzz"),
                "operating_cash_to_revenue": ("xsxjlyysr",),
                "inventory_turnover": ("chzzl",),
                "bps": ("mgjzc",),
                "basic_eps": ("jbmgsy",),
            },
        }
        for destination, sources in maps.get(table, {}).items():
            row[destination] = next(
                (value for source in sources if (value := _number(raw.get(source))) is not None),
                None,
            )
        for key, value in raw.items():
            if key not in {"jzrq", "bdrq", "plrq"} and key not in row:
                row[key] = _number(value)
        return row

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        symbols = symbols or ["000001.SZ"]
        if dataset == "daily":
            frame = self.get_daily(symbols, None, None)
            return _preview(dataset, frame)
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
            rows = self.get_depth5(symbols)
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": len(rows),
                "columns": ["symbol", "ask_volumes", "bid_volumes", "timestamp"],
                "preview": [{"symbol": symbol, **row} for symbol, row in list(rows.items())[:5]],
            }
        if dataset == "financial":
            frame = self.get_financials("metrics", symbols, latest_only=True)
            return _preview(dataset, frame)
        raise ValueError(f"麦蕊智数未声明数据集: {dataset}")


def _preview(dataset: str, frame: pl.DataFrame) -> dict:
    return {
        "provider": "mairui",
        "dataset": dataset,
        "rows": frame.height,
        "columns": frame.columns,
        "preview": frame.head(5).to_dicts() if not frame.is_empty() else [],
    }
