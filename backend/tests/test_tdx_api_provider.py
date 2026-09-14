"""tdx_api 内置 Provider 的契约测试。

全部注入假 HTTP 客户端, 不依赖真实 tdx-api 服务与网络。
"""
from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from app.data_providers import custom as custom_sources
from app.plugins.tdx_api import client as tdx_client
from app.plugins.tdx_api import provider as tp_app
from app.plugins.tdx_api.provider import TdxApiProvider


class _FakeClient:
    """最小的 tdx-api 客户端替身, 记录调用并可按标的注入失败。"""

    def __init__(
        self,
        *,
        stock_codes=None,
        etf=None,
        kline_all=None,
        kline_data=None,
        index_all_data=None,
        quotes=None,
        fail_codes=None,
        status="running",
    ):
        self.stock_codes = stock_codes if stock_codes is not None else []
        self.etf = etf if etf is not None else []
        self.kline_all = kline_all or {}
        self.kline_data = kline_data or {}
        self.index_all_data = index_all_data or {}
        self.quotes = quotes or {}
        self.fail_codes = set(fail_codes or ())
        self.status = status
        self.kline_all_calls: list[tuple] = []
        self.kline_calls: list[tuple] = []
        self.index_calls: list[tuple] = []
        self.batch_calls: list[list[str]] = []
        self.closed = False

    def close(self):
        self.closed = True

    def server_status(self):
        return {"status": self.status}

    def list_stock_codes(self):
        return list(self.stock_codes)

    def list_etf(self):
        return list(self.etf)

    def kline_all_tdx(self, code, ktype="day", limit=None):
        self.kline_all_calls.append((code, ktype, limit))
        value = self.kline_all.get(code)
        if isinstance(value, Exception):
            raise value
        return list(value or [])

    def kline(self, code, ktype="minute1"):
        self.kline_calls.append((code, ktype))
        value = self.kline_data.get(code)
        if isinstance(value, Exception):
            raise value
        return list(value or [])

    def index_all(self, code, ktype="day", limit=None):
        self.index_calls.append((code, ktype, limit))
        value = self.index_all_data.get(code)
        if isinstance(value, Exception):
            raise value
        return list(value or [])

    def batch_quote(self, codes):
        batch = list(codes)
        self.batch_calls.append(batch)
        # 上游按请求顺序回填; 假客户端同样保持顺序, 便于断言位置校验。
        bare = [_bare_code(code) for code in batch]
        if self.fail_codes.intersection(bare):
            raise tdx_client.TdxApiError("批量行情失败")
        rows = []
        for code in bare:
            value = self.quotes.get(code)
            if value is not None:
                rows.append(value)
        return rows


def _bare_code(code: str) -> str:
    """``sh600000`` → ``600000``(假客户端的 quotes 以裸码为键)。"""
    return code[2:] if len(code) == 8 and code[:2].isalpha() else code


def _provider(fake: _FakeClient) -> TdxApiProvider:
    provider = TdxApiProvider(base_url="http://127.0.0.1:8080")
    provider._client = fake
    return provider


def _daily(code, day, open_, high, low, close, volume, amount):
    return {
        "Time": f"{day}T15:00:00+08:00",
        "Open": open_, "High": high, "Low": low, "Close": close,
        "Volume": volume, "Amount": amount,
    }


def _quote(close, last, *, exchange=0, code="000001", buy=None, sell=None,
           total_hand=100, amount=1000):
    return {
        "Exchange": exchange,
        "Code": code,
        "K": {"Last": last, "Open": close, "High": close, "Low": close, "Close": close},
        "TotalHand": total_hand,
        "Amount": amount,
        "BuyLevel": buy or [],
        "SellLevel": sell or [],
    }


# ---------------------------------------------------------------- 声明与清单

def test_declared_datasets_exclude_unsupported_ones():
    provider = TdxApiProvider()
    assert set(provider.config.datasets) == {"daily", "minute", "realtime", "depth5"}
    # 上游无除权事件/财务报表接口, provider_has_dataset 必须为 False 才能回退其他源。
    assert "adj_factor" not in provider.config.datasets
    assert "financial" not in provider.config.datasets


def test_plugin_manifest_is_loadable_and_isolated():
    manifest = custom_sources.plugin_manifest("tdx_api")
    assert manifest is not None
    assert manifest["entry"] == "app.plugins.tdx_api.provider:TdxApiProvider"
    assert manifest["check"] == "app.plugins.tdx_api.provider:availability"
    assert manifest["fallback_to_tickflow_on_error"] is False


def test_availability_reports_running_and_unreachable(monkeypatch):
    class _Probe:
        def __init__(self, *args, **kwargs):
            pass

        def server_status(self):
            return {"status": "running"}

        def close(self):
            pass

    monkeypatch.setattr(tp_app.tdx_client, "TdxApiClient", _Probe)
    ok, reason = tp_app.availability()
    assert ok is True and reason == "ok"

    class _Bad:
        def __init__(self, *args, **kwargs):
            pass

        def server_status(self):
            raise tdx_client.TdxApiError("connection refused")

        def close(self):
            pass

    monkeypatch.setattr(tp_app.tdx_client, "TdxApiClient", _Bad)
    ok, reason = tp_app.availability()
    assert ok is False and "未检测到 tdx-api 服务" in reason


# ---------------------------------------------------------------- 日K

def test_daily_converts_li_to_yuan_and_keeps_hand_volume():
    fake = _FakeClient(kline_all={"600000": [
        _daily("600000", "2026-09-11", 11680, 11860, 11660, 11850, 867632, 1022543936000),
        _daily("600000", "2026-09-14", 11730, 11900, 11720, 11800, 487878, 576123968000),
    ]})
    df = _provider(fake).get_daily(["600000.SH"], None, None)

    assert df.columns == ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    assert df.height == 2
    row = df.row(1, named=True)
    assert row["date"] == date(2026, 9, 14)
    assert row["close"] == pytest.approx(11.8)
    assert row["open"] == pytest.approx(11.73)
    # K线成交量上游已是「手」, 不得再次换算。
    assert row["volume"] == pytest.approx(487878.0)
    # 成交额上游为厘 → 元。
    assert row["amount"] == pytest.approx(576123968.0)
    assert fake.kline_all_calls == [("600000", "day", None)]


def test_daily_filters_window_and_estimates_limit():
    fake = _FakeClient(kline_all={"600000": [
        _daily("600000", "2020-01-02", 1000, 1000, 1000, 1000, 1, 1000),
        _daily("600000", "2026-09-14", 11730, 11900, 11720, 11800, 487878, 576123968000),
    ]})
    df = _provider(fake).get_daily(
        ["600000.SH"], datetime(2026, 1, 1), datetime(2026, 9, 14),
    )
    assert df.height == 1
    assert df["date"].to_list() == [date(2026, 9, 14)]
    # 窗口估算的 limit 必须传入, 避免每次拉全量历史。
    _, _, limit = fake.kline_all_calls[0]
    assert isinstance(limit, int) and 100 < limit < 1000


def test_daily_index_uses_prefixed_code_and_index_endpoint():
    fake = _FakeClient(index_all_data={"sh000001": [
        _daily("000001", "2026-09-14", 3910920, 3912320, 3852030, 3888110, 579123100, 958186323968000),
    ]})
    df = _provider(fake).get_daily(["000001.SH"], None, None, asset_type="index")
    assert fake.index_calls == [("sh000001", "day", None)]
    assert fake.kline_all_calls == []
    assert df["symbol"].to_list() == ["000001.SH"]
    assert df.row(0, named=True)["close"] == pytest.approx(3888.11)


def test_daily_partial_failure_reports_symbol_and_keeps_success():
    fake = _FakeClient(kline_all={
        "600000": [_daily("600000", "2026-09-14", 11730, 11900, 11720, 11800, 1, 1000)],
        "600001": tdx_client.TdxApiError("boom"),
    })
    failed: list[str] = []
    df = _provider(fake).get_daily(
        ["600000.SH", "600001.SH"], None, None, failed_out=failed,
    )
    assert df.height == 1
    assert failed == ["600001.SH"]


def test_daily_total_failure_raises():
    fake = _FakeClient(kline_all={"600000": tdx_client.TdxApiError("boom")})
    with pytest.raises(RuntimeError, match="全部失败"):
        _provider(fake).get_daily(["600000.SH"], None, None)


def test_daily_rejects_unsupported_asset_type():
    fake = _FakeClient()
    with pytest.raises(ValueError, match="不支持资产类型"):
        _provider(fake).get_daily(["600000.SH"], None, None, asset_type="bond")


def test_daily_progress_callback_reaches_total():
    fake = _FakeClient(kline_all={
        "600000": [_daily("600000", "2026-09-14", 1, 1, 1, 1, 1, 1)],
        "600001": [],
        "600002": [],
    })
    seen: list[tuple[int, int]] = []
    _provider(fake).get_daily(
        ["600000.SH", "600001.SH", "600002.SH"], None, None,
        on_chunk_done=lambda cur, total: seen.append((cur, total)),
    )
    assert len(seen) == 3
    assert sorted(cur for cur, _ in seen) == [1, 2, 3]
    assert {total for _, total in seen} == {3}


# ---------------------------------------------------------------- 分钟K

def test_minute_converts_units_and_beijing_wallclock():
    fake = _FakeClient(kline_data={"000001": [
        {"Time": "2026-09-14T09:31:00+08:00", "Open": 11500, "High": 11560,
         "Low": 11460, "Close": 11560, "Volume": 54430, "Amount": 62644968000},
        {"Time": "2026-09-14T01:32:00Z", "Open": 11550, "High": 11590,
         "Low": 11550, "Close": 11560, "Volume": 65679, "Amount": 75955456000},
    ]})
    df = _provider(fake).get_minute(["000001.SZ"], None, None)
    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    rows = df.sort("datetime").to_dicts()
    # UTC 帧必须换算到北京墙钟(01:32Z == 09:32+08:00)。
    assert rows[0]["datetime"] == datetime(2026, 9, 14, 9, 31)
    assert rows[1]["datetime"] == datetime(2026, 9, 14, 9, 32)
    assert rows[0]["close"] == pytest.approx(11.56)
    assert rows[0]["volume"] == pytest.approx(54430.0)
    assert rows[0]["amount"] == pytest.approx(62644968.0)


def test_minute_filters_window():
    fake = _FakeClient(kline_data={"000001": [
        {"Time": "2026-09-11T09:31:00+08:00", "Open": 1, "High": 1, "Low": 1, "Close": 1,
         "Volume": 1, "Amount": 1000},
        {"Time": "2026-09-14T09:31:00+08:00", "Open": 1, "High": 1, "Low": 1, "Close": 1,
         "Volume": 1, "Amount": 1000},
    ]})
    df = _provider(fake).get_minute(
        ["000001.SZ"], datetime(2026, 9, 14), datetime(2026, 9, 14, 23, 59),
    )
    assert df["datetime"].to_list() == [datetime(2026, 9, 14, 9, 31)]


def test_minute_index_is_not_provided():
    fake = _FakeClient()
    df = _provider(fake).get_minute(["000001.SH"], None, None, asset_type="index")
    assert df.is_empty()
    assert fake.kline_calls == []


def test_minute_rejects_non_one_minute_freq():
    fake = _FakeClient()
    with pytest.raises(ValueError, match="仅支持 1m"):
        _provider(fake).get_minute(["000001.SZ"], None, None, freq="5m")


# ---------------------------------------------------------------- 实时行情

def _stock(code, exchange, name):
    return {"code": code, "exchange": exchange, "name": name}


def _etf(code, exchange, name):
    return {"code": code, "exchange": exchange, "name": name}


def test_realtime_maps_stock_units_and_decimal_change_pct():
    fake = _FakeClient(
        stock_codes=[_stock("000001", "sz", "平安银行")],
        quotes={"000001": _quote(
            close=11800, last=11740, exchange=0, code="000001",
            buy=[{"Price": 11790, "Number": 569}],
            sell=[{"Price": 11800, "Number": 328}],
            total_hand=487878, amount=576123968,
        )},
    )
    rows = _provider(fake).get_realtime()
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "000001.SZ"
    assert row["name"] == "平安银行"
    assert row["last_price"] == pytest.approx(11.8)
    assert row["prev_close"] == pytest.approx(11.74)
    # 盘口 Amount 单位是元, TotalHand 是手。
    assert row["amount"] == pytest.approx(576123968.0)
    assert row["volume"] == pytest.approx(487878.0)
    assert row["change_amount"] == pytest.approx(0.06)
    assert row["change_pct"] == pytest.approx(0.06 / 11.74)
    assert abs(row["change_pct"]) < 1  # 小数制
    assert row["turnover_rate"] is None  # 上游无股本, 不伪造
    assert isinstance(row["timestamp"], int) and row["timestamp"] > 0


def test_realtime_etf_price_scale_uses_same_frame_levels():
    """上游对 ETF 把盘口 K 价额外 ÷10, 必须按同帧档位价还原成 1.76 元。"""
    fake = _FakeClient(
        etf=[_etf("510010", "sh", "治理ETF")],
        quotes={"510010": _quote(
            close=176, last=176, exchange=1, code="510010",
            buy=[{"Price": 1747, "Number": 572}],
            sell=[{"Price": 1778, "Number": 2}],
            total_hand=27, amount=4757,
        )},
    )
    row = _provider(fake).get_realtime()[0]
    assert row["symbol"] == "510010.SH"
    assert row["last_price"] == pytest.approx(1.76)
    assert row["prev_close"] == pytest.approx(1.76)
    assert row["volume"] == pytest.approx(27.0)


def test_realtime_etf_scale_falls_back_to_fund_membership_without_levels():
    fake = _FakeClient(
        etf=[_etf("510010", "sh", None)],
        quotes={"510010": _quote(close=176, last=176, exchange=1, code="510010")},
    )
    row = _provider(fake).get_realtime()[0]
    assert row["last_price"] == pytest.approx(1.76)


def test_realtime_splits_batches_at_upstream_limit():
    codes = [f"{600000 + index}" for index in range(120)]
    fake = _FakeClient(
        stock_codes=[_stock(code, "sh", None) for code in codes],
        quotes={code: _quote(close=10000, last=10000, exchange=1, code=code) for code in codes},
    )
    rows = _provider(fake).get_realtime()
    assert len(rows) == 120
    assert [len(batch) for batch in fake.batch_calls] == [50, 50, 20]


def test_realtime_returns_empty_on_batch_failure():
    """任一批次最终失败即整轮作废, 不能用残缺快照覆盖上一份有效数据。"""
    codes = [f"{600000 + index}" for index in range(60)]
    fake = _FakeClient(
        stock_codes=[_stock(code, "sh", None) for code in codes],
        quotes={code: _quote(close=10000, last=10000, exchange=1, code=code) for code in codes},
        fail_codes={"600055"},
    )
    assert _provider(fake).get_realtime() == []


def test_realtime_returns_empty_when_universe_unavailable():
    fake = _FakeClient(stock_codes=[])
    assert _provider(fake).get_realtime() == []


def test_realtime_skips_unparsable_rows():
    fake = _FakeClient(
        stock_codes=[_stock("000001", "sz", "平安银行")],
        quotes={"000001": {"Code": "00001", "Exchange": 9, "K": {}}},
    )
    assert _provider(fake).get_realtime() == []


# ---------------------------------------------------------------- 指数行情

def test_realtime_indices_uses_prefixed_codes_and_core_names():
    """指数不在 A 股快照里, 需带前缀单独补拉; 点位为厘, 核心指数补名。"""
    fake = _FakeClient(
        quotes={
            "000001": _quote(
                close=3894280, last=3888110, exchange=1, code="000001",
                total_hand=312361615, amount=526345043968,
            ),
            "899050": _quote(close=1029540, last=1020000, exchange=2, code="899050"),
        },
    )
    records = _provider(fake).get_realtime_indices(["000001.SH", "899050.BJ"])
    assert fake.batch_calls == [["sh000001", "bj899050"]]
    assert [row["symbol"] for row in records] == ["000001.SH", "899050.BJ"]
    assert records[0]["name"] == "上证指数"
    # 指数无买卖档 → 按厘还原, 不能误判成基金的额外 ÷10
    assert records[0]["last_price"] == pytest.approx(3894.28)
    assert records[0]["prev_close"] == pytest.approx(3888.11)
    assert records[0]["change_pct"] == pytest.approx((3894.28 - 3888.11) / 3888.11)
    assert records[0]["amount"] == pytest.approx(526345043968.0)
    assert records[1]["last_price"] == pytest.approx(1029.54)


def test_realtime_indices_returns_none_on_failure():
    """失败返回 None(不是空列表), 让上层保留上轮有效指数缓存。"""
    fake = _FakeClient(
        quotes={"000001": _quote(close=10000, last=10000, exchange=1, code="000001")},
        fail_codes={"000001"},
    )
    assert _provider(fake).get_realtime_indices(["000001.SH"]) is None


def test_realtime_indices_drops_position_mismatch():
    """上游对不支持的代码会回填占位记录; 位置对不上必须丢弃。"""
    fake = _FakeClient(
        quotes={"899050": _quote(close=10000, last=10000, exchange=1, code="000001")},
    )
    assert _provider(fake).get_realtime_indices(["899050.BJ"]) == []


def test_realtime_indices_empty_when_no_valid_symbol():
    fake = _FakeClient()
    assert _provider(fake).get_realtime_indices(["000001", "SH600000"]) == []
    assert fake.batch_calls == []


# ---------------------------------------------------------------- 五档

def test_depth5_keeps_missing_levels_as_none():
    fake = _FakeClient(
        quotes={"000001": _quote(
            close=11800, last=11740, code="000001", exchange=0,
            buy=[{"Price": 11790, "Number": 569}, {"Price": 11780, "Number": 1058}],
            sell=[{"Price": 11800, "Number": 328}],
        )},
    )
    data = _provider(fake).get_depth5(["000001.SZ"])
    row = data["000001.SZ"]
    assert row["bid_volumes"] == [569, 1058, None, None, None]
    assert row["ask_volumes"] == [328, None, None, None, None]
    assert isinstance(row["timestamp"], int)


def test_depth5_rejects_unknown_symbol_and_failure_is_empty():
    fake = _FakeClient(fail_codes={"000001"})
    provider = _provider(fake)
    assert provider.get_depth5(["000001.SZ"]) == {}
    provider2 = _provider(_FakeClient())
    assert provider2.get_depth5(["bad-code"]) == {}


# ---------------------------------------------------------------- 维表

def test_instruments_stock_and_etf_map_symbol_and_name():
    fake = _FakeClient(
        stock_codes=[_stock("000001", "sz", "平安银行")],
        etf=[_etf("510010", "sh", "治理ETF")],
    )
    provider = _provider(fake)
    stocks = provider.get_instruments("stock")
    assert stocks[0]["symbol"] == "000001.SZ"
    assert stocks[0]["name"] == "平安银行"
    assert stocks[0]["type"] == "stock"
    etfs = provider.get_instruments("etf")
    assert etfs[0]["symbol"] == "510010.SH"
    assert etfs[0]["type"] == "etf"


def test_instruments_index_uses_core_index_contract():
    from app.services.index_const import CORE_INDEX_NAMES

    rows = _provider(_FakeClient()).get_instruments("index")
    assert {row["symbol"] for row in rows} == set(CORE_INDEX_NAMES)
    assert all(row["type"] == "index" for row in rows)


def test_instruments_rejects_unknown_type():
    with pytest.raises(ValueError, match="不支持"):
        _provider(_FakeClient()).get_instruments("bond")


def test_instruments_empty_source_raises():
    with pytest.raises(RuntimeError, match="代码表"):
        _provider(_FakeClient()).get_instruments("stock")


# ---------------------------------------------------------------- 试拉

def test_test_dataset_unknown_name_returns_error():
    result = _provider(_FakeClient()).test_dataset("financial")
    assert result["rows"] == 0
    assert "未声明" in result["error"]


def test_test_dataset_daily_preview_serializes_dates():
    fake = _FakeClient(kline_all={"600000": [
        _daily("600000", "2026-09-14", 11730, 11900, 11720, 11800, 1, 1000),
    ]})
    result = _provider(fake).test_dataset("daily", ["600000.SH"])
    assert result["rows"] == 1
    assert result["preview"][0]["date"] == "2026-09-14"


# ---------------------------------------------------------------- 工具函数

@pytest.mark.parametrize(
    ("symbol", "expected"),
    [("600519.SH", "sh600519"), ("000001.SZ", "sz000001"), ("000001.SH", "sh000001")],
)
def test_to_api_code_prefixes_exchange(symbol, expected):
    assert tp_app._to_api_code(symbol) == expected


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [("600519.SH", "600519"), ("510010.SH", "510010"), ("000001.SH", "000001")],
)
def test_to_bare_code_strips_exchange(symbol, expected):
    assert tp_app._to_bare_code(symbol) == expected


@pytest.mark.parametrize("symbol", ["600519", "600519.SZ.BJ", "", "abcdef.SH"])
def test_code_helpers_reject_malformed_symbol(symbol):
    with pytest.raises(ValueError):
        tp_app._to_api_code(symbol)
    with pytest.raises(ValueError):
        tp_app._to_bare_code(symbol)


def test_canonical_symbol_normalizes_exchange_forms():
    assert tp_app._canonical_symbol("1", 0) == "000001.SZ"
    assert tp_app._canonical_symbol("600000", 1) == "600000.SH"
    assert tp_app._canonical_symbol("430047", 2) == "430047.BJ"
    assert tp_app._canonical_symbol("600000", "sh") == "600000.SH"
    assert tp_app._canonical_symbol("600000", 9) is None
    assert tp_app._canonical_symbol(None, 1) is None


def test_daily_limit_none_when_start_missing_or_span_huge():
    assert tp_app._daily_limit(None, None) is None
    assert tp_app._daily_limit(datetime(1990, 1, 1), datetime(2026, 9, 14)) is None
    limit = tp_app._daily_limit(datetime(2026, 1, 1), datetime(2026, 9, 14))
    assert isinstance(limit, int) and 100 < limit < 1000


def test_frame_keeps_schema_when_empty():
    df = tp_app.TdxApiProvider._frame([], tp_app._DAILY_SCHEMA)
    assert df.is_empty()
    assert df.columns == list(tp_app._DAILY_SCHEMA)


def test_close_releases_client():
    fake = _FakeClient()
    provider = _provider(fake)
    provider.close()
    assert fake.closed is True


def test_realtime_record_is_polars_friendly():
    """records 会被 pl.DataFrame(records) 直接消费, 列名与类型必须稳定。"""
    fake = _FakeClient(
        stock_codes=[_stock("000001", "sz", "平安银行")],
        quotes={"000001": _quote(close=11800, last=11740, code="000001", exchange=0)},
    )
    rows = _provider(fake).get_realtime()
    df = pl.DataFrame(rows)
    assert {"symbol", "last_price", "prev_close", "volume", "amount", "timestamp"} <= set(df.columns)
