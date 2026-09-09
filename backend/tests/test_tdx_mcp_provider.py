"""tdx_mcp 内置 Provider 的契约测试。

全部使用假的 eltdx 客户端, 避免把公开通达信 TCP 服务当成单测依赖。
"""
from __future__ import annotations

from datetime import date, datetime
from threading import Event, Thread
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from app.plugins.tdx_mcp import bridge
from app.plugins.tdx_mcp import provider as tp
from app.plugins.tdx_mcp.provider import TdxMcpProvider

CN_TZ = ZoneInfo("Asia/Shanghai")


class _Page:
    def __init__(self, items, count: int | None = None):
        self.items = list(items)
        self.count = len(self.items) if count is None else count


class _FakeClient:
    def __init__(
        self,
        *,
        pages=None,
        all_pages=None,
        xdxrs=None,
        quotes=None,
        code_lists=None,
        code_entries=None,
        connect_error: Exception | None = None,
        connect_started: Event | None = None,
        connect_release: Event | None = None,
        request_started: Event | None = None,
        request_release: Event | None = None,
    ):
        self.pages = pages or {}
        self.all_pages = all_pages or {}
        self.xdxrs = xdxrs or {}
        self.quotes = quotes or []
        self.code_lists = code_lists or {}
        self.code_entries = code_entries or {}
        self.connect_error = connect_error
        self.connect_started = connect_started
        self.connect_release = connect_release
        self.request_started = request_started
        self.request_release = request_release
        self.connect_calls = 0
        self.kline_calls: list[tuple] = []
        self.all_calls: list[tuple] = []
        self.xdxr_calls: list[str] = []
        self.quote_calls: list[list[str]] = []
        self.closed = False

    def connect(self):
        self.connect_calls += 1
        if self.connect_started is not None:
            self.connect_started.set()
        if self.connect_release is not None:
            assert self.connect_release.wait(timeout=2)
        if self.connect_error is not None:
            raise self.connect_error

    def _wait_for_request_release(self):
        if self.request_started is not None:
            self.request_started.set()
        if self.request_release is not None:
            assert self.request_release.wait(timeout=2)

    def get_kline(self, period, code, *, start, count, kind):
        self._wait_for_request_release()
        self.kline_calls.append((period, code, start, count, kind))
        value = self.pages.get((period, code, start), _Page([]))
        if isinstance(value, Exception):
            raise value
        return value

    def get_kline_all(self, period, code, *, kind):
        self._wait_for_request_release()
        self.all_calls.append((period, code, kind))
        value = self.all_pages.get((period, code, kind), _Page([]))
        if isinstance(value, Exception):
            raise value
        return value

    def get_quote(self, codes):
        self._wait_for_request_release()
        self.quote_calls.append(list(codes))
        if isinstance(self.quotes, Exception):
            raise self.quotes
        return self.quotes

    def get_xdxr(self, code):
        self._wait_for_request_release()
        self.xdxr_calls.append(code)
        value = self.xdxrs.get(code, [])
        if isinstance(value, Exception):
            raise value
        return value

    def get_a_share_codes_all(self):
        return self.code_lists.get("stock", [])

    def get_index_codes_all(self):
        return self.code_lists.get("index", [])

    def get_etf_codes_all(self):
        return self.code_lists.get("etf", [])

    def get_codes_all(self, exchange):
        return self.code_entries.get(exchange, [])

    def close(self):
        self.closed = True


def _bar(
    when: datetime,
    *,
    volume: int = 179_522,
    amount: float = 1_792_200.0,
    last_close: float | None = None,
):
    return SimpleNamespace(
        time=when,
        open_price=10.0,
        high_price=10.5,
        low_price=9.8,
        close_price=10.2,
        last_close_price=10.2 if last_close is None else last_close,
        volume=volume,
        amount=amount,
    )


def _xdxr(
    when: datetime,
    *,
    fenhong: float,
    peigujia: float = 0.0,
    songzhuangu: float = 0.0,
    peigu: float = 0.0,
):
    return SimpleNamespace(
        time=when,
        fenhong=fenhong,
        peigujia=peigujia,
        songzhuangu=songzhuangu,
        peigu=peigu,
    )


def _with_client(monkeypatch, client: _FakeClient) -> TdxMcpProvider:
    provider = TdxMcpProvider()
    monkeypatch.setattr(bridge, "create_client", lambda: client)
    return provider


def test_symbol_conversion_requires_explicit_exchange():
    assert tp._to_tdx_symbol("600000.SH") == "sh600000"
    assert tp._to_tdx_symbol("000001.SZ") == "sz000001"
    assert tp._to_tdx_symbol("920001.BJ") == "bj920001"

    with pytest.raises(ValueError, match="交易所后缀"):
        tp._to_tdx_symbol("600000")
    with pytest.raises(ValueError, match="不支持"):
        tp._to_tdx_symbol("600000.US")


def test_daily_pages_backwards_filters_and_keeps_volume_in_lots(monkeypatch):
    client = _FakeClient(pages={
        ("day", "sh600000", 0): _Page([
            _bar(datetime(2026, 9, 3, 15, tzinfo=CN_TZ)),
            _bar(datetime(2026, 9, 4, 15, tzinfo=CN_TZ)),
        ], count=800),
        ("day", "sh600000", 800): _Page([
            _bar(datetime(2026, 9, 1, 15, tzinfo=CN_TZ)),
            _bar(datetime(2026, 9, 2, 15, tzinfo=CN_TZ)),
        ], count=2),
    })
    provider = _with_client(monkeypatch, client)

    df = provider.get_daily(
        ["600000.SH"], datetime(2026, 9, 2), datetime(2026, 9, 3),
    )

    assert df.columns == ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    assert df.schema["date"] == pl.Date
    assert df["date"].to_list() == [datetime(2026, 9, 2).date(), datetime(2026, 9, 3).date()]
    assert df["volume"].to_list() == [179_522.0, 179_522.0]
    assert [(call[2], call[4]) for call in client.kline_calls] == [(0, "stock"), (800, "stock")]


def test_daily_unbounded_uses_full_kline_history(monkeypatch):
    client = _FakeClient(all_pages={
        ("day", "sh600000", "stock"): _Page([
            _bar(datetime(1999, 11, 10, 15, tzinfo=CN_TZ)),
            _bar(datetime(2026, 9, 4, 15, tzinfo=CN_TZ)),
        ]),
    })
    provider = _with_client(monkeypatch, client)

    df = provider.get_daily(["600000.SH"], None, None)

    assert df["date"].to_list() == [datetime(1999, 11, 10).date(), datetime(2026, 9, 4).date()]
    assert client.all_calls == [("day", "sh600000", "stock")]


def test_daily_uses_index_kline_kind(monkeypatch):
    client = _FakeClient(pages={
        ("day", "sh000001", 0): _Page([_bar(datetime(2026, 9, 4, 15, tzinfo=CN_TZ))]),
    })
    provider = _with_client(monkeypatch, client)

    df = provider.get_daily(["000001.SH"], datetime(2026, 9, 1), None, asset_type="index")

    assert df.height == 1
    assert client.kline_calls[0][-1] == "index"


def test_minute_unbounded_uses_full_kline_and_returns_naive_beijing_time(monkeypatch):
    client = _FakeClient(all_pages={
        ("1m", "sh600000", "stock"): _Page([
            _bar(datetime(2026, 9, 4, 9, 31, tzinfo=CN_TZ), volume=120),
            _bar(datetime(2026, 9, 4, 9, 32, tzinfo=CN_TZ), volume=200),
        ]),
    })
    provider = _with_client(monkeypatch, client)

    df = provider.get_minute(["600000.SH"], None, None)

    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    assert df.schema["datetime"] == pl.Datetime("us")
    assert df["datetime"].to_list() == [
        datetime(2026, 9, 4, 9, 31),
        datetime(2026, 9, 4, 9, 32),
    ]
    assert client.all_calls == [("1m", "sh600000", "stock")]


def test_minute_partial_symbol_failure_keeps_other_symbols_and_reports_failure(monkeypatch):
    client = _FakeClient(pages={
        ("1m", "sh600000", 0): _Page([_bar(datetime(2026, 9, 4, 9, 31, tzinfo=CN_TZ))]),
        ("1m", "sz000001", 0): RuntimeError("TCP timeout"),
    })
    provider = _with_client(monkeypatch, client)
    failed_symbols: list[str] = []

    df = provider.get_minute(
        ["600000.SH", " 000001.sz "],
        datetime(2026, 9, 4, 9, 25),
        datetime(2026, 9, 4, 15, 5),
        failed_out=failed_symbols,
    )

    assert df["symbol"].to_list() == ["600000.SH"]
    assert failed_symbols == ["000001.SZ"]


def test_adj_factor_maps_only_event_ratio_not_cumulative_qfq_factor(monkeypatch):
    client = _FakeClient(
        pages={
            ("day", "sh600000", 0): _Page([
                _bar(datetime(2026, 7, 16, 15, tzinfo=CN_TZ), last_close=9.31),
            ]),
        },
        xdxrs={
            "sh600000": [_xdxr(datetime(2026, 7, 16, tzinfo=CN_TZ), fenhong=4.2)],
        },
    )
    provider = _with_client(monkeypatch, client)

    df = provider.get_adj_factors(
        ["600000.SH"], datetime(2026, 7, 16), datetime(2026, 7, 16),
    )

    assert client.xdxr_calls == ["sh600000"]
    assert df.to_dicts() == [{
        "symbol": "600000.SH",
        "trade_date": date(2026, 7, 16),
        "ex_factor": pytest.approx(9.31 / 8.89),
    }]


def test_adj_factor_partial_symbol_failure_keeps_other_symbols(monkeypatch):
    client = _FakeClient(
        pages={
            ("day", "sh600000", 0): _Page([
                _bar(datetime(2026, 7, 16, 15, tzinfo=CN_TZ), last_close=9.31),
            ]),
            ("day", "sz000001", 0): RuntimeError("TCP timeout"),
        },
        xdxrs={
            "sh600000": [_xdxr(datetime(2026, 7, 16, tzinfo=CN_TZ), fenhong=4.2)],
        },
    )
    provider = _with_client(monkeypatch, client)

    df = provider.get_adj_factors(
        ["600000.SH", "000001.SZ"], datetime(2026, 7, 16), datetime(2026, 7, 16),
    )

    assert df["symbol"].to_list() == ["600000.SH"]


def test_realtime_aggregates_cached_stock_etf_and_index_quotes(monkeypatch):
    quote_time = datetime(2026, 9, 4, 10, 0, tzinfo=CN_TZ)
    quotes = [
        SimpleNamespace(
            exchange="sh", code="600000", server_time=quote_time,
            last_price=10.2, last_close_price=10.0,
            open_price=10.1, high_price=10.3, low_price=9.9,
            total_hand=12_345, amount=1_234_500.0,
        ),
        SimpleNamespace(
            exchange="sh", code="510300", server_time=quote_time,
            last_price=4.1, last_close_price=4.0,
            open_price=4.0, high_price=4.2, low_price=3.9,
            total_hand=456, amount=456_000.0,
        ),
        SimpleNamespace(
            exchange="sh", code="000001", server_time=quote_time,
            last_price=3_100.0, last_close_price=3_000.0,
            open_price=3_010.0, high_price=3_120.0, low_price=2_990.0,
            total_hand=789, amount=789_000.0,
        ),
    ]
    client = _FakeClient(
        quotes=quotes,
        code_lists={
            "stock": ["sh600000"],
            "etf": ["sh510300"],
            "index": ["sh000001"],
        },
    )
    provider = _with_client(monkeypatch, client)

    rows = provider.get_realtime()

    assert client.quote_calls == [["sh600000", "sh510300", "sh000001"]]
    assert [row["symbol"] for row in rows] == ["600000.SH", "510300.SH", "000001.SH"]
    stock = rows[0]
    assert stock["volume"] == 12_345.0
    assert stock["amount"] == 1_234_500.0
    assert stock["change_amount"] == pytest.approx(0.2)
    assert stock["change_pct"] == pytest.approx(0.02)
    assert stock["amplitude"] == pytest.approx(0.04)
    assert stock["turnover_rate"] is None
    assert stock["timestamp"] == int(quote_time.timestamp() * 1000)

    # 第二轮重用已解析的全市场代码表, 只重新请求快照。
    provider.get_realtime()
    assert client.quote_calls == [
        ["sh600000", "sh510300", "sh000001"],
        ["sh600000", "sh510300", "sh000001"],
    ]


def test_realtime_soft_fails_on_tcp_error(monkeypatch):
    client = _FakeClient(
        quotes=RuntimeError("TCP down"),
        code_lists={"stock": ["sh600000"]},
    )
    provider = _with_client(monkeypatch, client)

    assert provider.get_realtime() == []


def test_daily_partial_symbol_failure_keeps_other_symbols(monkeypatch):
    client = _FakeClient(pages={
        ("day", "sh600000", 0): _Page([_bar(datetime(2026, 9, 4, 15, tzinfo=CN_TZ))]),
        ("day", "sz000001", 0): RuntimeError("TCP timeout"),
    })
    provider = _with_client(monkeypatch, client)
    failed_symbols: list[str] = []

    df = provider.get_daily(
        ["600000.SH", " 000001.sz "],
        datetime(2026, 9, 1),
        None,
        failed_out=failed_symbols,
    )

    assert df["symbol"].to_list() == ["600000.SH"]
    assert failed_symbols == ["000001.SZ"]


def test_daily_connection_failure_raises_instead_of_looking_like_empty_history(monkeypatch):
    provider = _with_client(monkeypatch, _FakeClient(connect_error=RuntimeError("TCP down")))

    with pytest.raises(RuntimeError, match="日K client 不可用"):
        provider.get_daily(["600000.SH"], datetime(2026, 9, 1), None)


def test_depth5_maps_buy_sell_levels_and_never_turns_missing_levels_into_zero(monkeypatch):
    quote_time = datetime(2026, 9, 4, 10, 0, tzinfo=CN_TZ)
    quote = SimpleNamespace(
        exchange="sh",
        code="600000",
        server_time=quote_time,
        buy_levels=[SimpleNamespace(number=0), SimpleNamespace(number=200)],
        sell_levels=[SimpleNamespace(number=300)],
    )
    client = _FakeClient(quotes=[quote])
    provider = _with_client(monkeypatch, client)

    result = provider.get_depth5(["600000.SH"])

    row = result["600000.SH"]
    assert client.quote_calls == [["sh600000"]]
    assert row["bid_volumes"] == [0, 200, None, None, None]
    assert row["ask_volumes"] == [300, None, None, None, None]
    assert row["timestamp"] == int(quote_time.timestamp() * 1000)


def test_depth5_soft_fails_without_ticking_over_to_another_provider(monkeypatch):
    provider = _with_client(monkeypatch, _FakeClient(quotes=RuntimeError("TCP down")))

    assert provider.get_depth5(["600000.SH"]) == {}


def test_concurrent_first_access_creates_and_connects_one_client(monkeypatch):
    connect_started = Event()
    connect_release = Event()
    client = _FakeClient(
        quotes=[],
        connect_started=connect_started,
        connect_release=connect_release,
    )
    provider = TdxMcpProvider()
    create_calls = []

    def _create_client():
        create_calls.append(True)
        return client

    monkeypatch.setattr(bridge, "create_client", _create_client)
    results: list[dict] = []
    first = Thread(target=lambda: results.append(provider.get_depth5(["600000.SH"])))
    second = Thread(target=lambda: results.append(provider.get_depth5(["600000.SH"])))

    first.start()
    assert connect_started.wait(timeout=2)
    second.start()
    connect_release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert results == [{}, {}]
    assert len(create_calls) == 1
    assert client.connect_calls == 1


def test_close_waits_for_inflight_daily_request(monkeypatch):
    request_started = Event()
    request_release = Event()
    client = _FakeClient(
        pages={
            ("day", "sh600000", 0): _Page([
                _bar(datetime(2026, 9, 4, 15, tzinfo=CN_TZ)),
            ]),
        },
        request_started=request_started,
        request_release=request_release,
    )
    provider = _with_client(monkeypatch, client)
    result: list[pl.DataFrame] = []
    worker = Thread(
        target=lambda: result.append(
            provider.get_daily(["600000.SH"], datetime(2026, 9, 1), None),
        ),
    )

    worker.start()
    assert request_started.wait(timeout=2)
    provider.close()
    assert client.closed is False

    request_release.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert result[0].height == 1
    assert client.closed is True


def test_close_waits_for_inflight_depth_request(monkeypatch):
    request_started = Event()
    request_release = Event()
    quote = SimpleNamespace(
        exchange="sh",
        code="600000",
        server_time=datetime(2026, 9, 4, 10, tzinfo=CN_TZ),
        buy_levels=[],
        sell_levels=[],
    )
    client = _FakeClient(
        quotes=[quote],
        request_started=request_started,
        request_release=request_release,
    )
    provider = _with_client(monkeypatch, client)
    result: list[dict] = []
    worker = Thread(target=lambda: result.append(provider.get_depth5(["600000.SH"])))

    worker.start()
    assert request_started.wait(timeout=2)
    provider.close()
    assert client.closed is False

    request_release.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert "600000.SH" in result[0]
    assert client.closed is True


def test_connect_failure_closes_unusable_client(monkeypatch):
    client = _FakeClient(connect_error=RuntimeError("TCP unavailable"))
    provider = _with_client(monkeypatch, client)

    with pytest.raises(RuntimeError, match="日K client 不可用"):
        provider.get_daily(["600000.SH"], datetime(2026, 9, 1), None)
    assert client.closed is True


def test_provider_exposes_named_instrument_universe_without_fabricating_metadata(monkeypatch):
    client = _FakeClient(code_lists={
        "stock": ["sz000001", "sh600000", "invalid"],
        "index": ["sh000001"],
        "etf": ["sh510300"],
    }, code_entries={
        "sh": [
            SimpleNamespace(exchange="sh", code="600000", name="*ST 浦发"),
            SimpleNamespace(exchange="sh", code="000001", name="上证指数"),
            SimpleNamespace(exchange="sh", code="510300", name="沪深300ETF"),
        ],
        "sz": [SimpleNamespace(exchange="sz", code="000001", name="平安银行")],
        "bj": [],
    })
    provider = _with_client(monkeypatch, client)

    stocks = provider.get_instruments("stock")
    index = provider.get_instruments("index")
    etfs = provider.get_instruments("etf")

    assert stocks == [
        {
            "symbol": "000001.SZ", "name": "平安银行", "code": "000001",
            "exchange": "SZ", "region": "CN", "type": "stock", "ext": {},
        },
        {
            "symbol": "600000.SH", "name": "*ST 浦发", "code": "600000",
            "exchange": "SH", "region": "CN", "type": "stock", "ext": {},
        },
    ]
    assert index[0]["symbol"] == "000001.SH"
    assert index[0]["name"] == "上证指数"
    assert etfs[0]["symbol"] == "510300.SH"
    assert etfs[0]["name"] == "沪深300ETF"
    assert client.code_entries
    assert "instruments" not in provider.config.datasets
    assert provider.daily_asset_types == {"stock", "index", "etf"}

    from app.price_limits import is_risk_warning_name, price_limit_pct

    assert is_risk_warning_name(stocks[1]["name"])
    assert price_limit_pct(
        stocks[1]["symbol"], date(2026, 7, 3), is_risk_warning=True,
    ) == 0.05


def test_provider_normalizes_symbols_in_all_tdx_outputs(monkeypatch):
    quote = SimpleNamespace(
        exchange="sh",
        code="600000",
        server_time=datetime(2026, 9, 4, 10, 0, tzinfo=CN_TZ),
        buy_levels=[],
        sell_levels=[],
    )
    client = _FakeClient(
        pages={
            ("day", "sh600000", 0): _Page([_bar(datetime(2026, 9, 4, 15, tzinfo=CN_TZ))]),
            ("1m", "sh600000", 0): _Page([_bar(datetime(2026, 9, 4, 9, 31, tzinfo=CN_TZ))]),
        },
        quotes=[quote],
    )
    provider = _with_client(monkeypatch, client)

    daily = provider.get_daily([" 600000.sh "], datetime(2026, 9, 1), None)
    minute = provider.get_minute([" 600000.sh "], datetime(2026, 9, 1), None)
    depth = provider.get_depth5([" 600000.sh "])

    assert daily["symbol"].to_list() == ["600000.SH"]
    assert minute["symbol"].to_list() == ["600000.SH"]
    assert list(depth) == ["600000.SH"]


def test_availability_only_checks_dependency_import(monkeypatch):
    def _missing_client_class():
        raise ImportError("No module named 'eltdx'")

    monkeypatch.setattr(bridge, "_client_class", _missing_client_class)
    ok, reason = bridge.availability()
    assert ok is False
    assert "eltdx" in reason


def test_availability_rejects_incompatible_eltdx_major_version(monkeypatch):
    monkeypatch.setattr(bridge, "_client_class", lambda: object)
    monkeypatch.setattr(bridge, "version", lambda _name: "1.0.0")

    ok, reason = bridge.availability()

    assert ok is False
    assert "不兼容" in reason


def test_availability_rejects_eltdx_prerelease(monkeypatch):
    monkeypatch.setattr(bridge, "_client_class", lambda: _FakeClient)
    monkeypatch.setattr(bridge, "version", lambda _name: "1.0rc1")

    ok, reason = bridge.availability()

    assert ok is False
    assert "不兼容" in reason


def test_availability_rejects_missing_required_eltdx_method(monkeypatch):
    monkeypatch.setattr(bridge, "_client_class", lambda: object)
    monkeypatch.setattr(bridge, "version", lambda _name: "0.5.1")

    ok, reason = bridge.availability()

    assert ok is False
    assert "缺少所需接口" in reason


def test_plugin_dependency_failure_is_isolated_from_other_plugin_discovery(monkeypatch, tmp_path):
    from app.data_providers import custom as custom_sources
    from app.data_providers.custom import loader

    with monkeypatch.context() as scoped:
        scoped.setattr(
            bridge,
            "_client_class",
            lambda: (_ for _ in ()).throw(ImportError("No module named 'eltdx'")),
        )
        loader.load_all(tmp_path)
        plugins = {row["name"]: row for row in custom_sources.list_plugins()}

        assert plugins["tdx_mcp"]["available"] is False
        assert "eltdx" in plugins["tdx_mcp"]["status"]
        assert "tdx_mcp" not in custom_sources.names()
        assert "stocksdk" in plugins
        assert custom_sources.is_builtin("tdx_mcp")

    loader.load_all(tmp_path)


def test_unavailable_tdx_selection_is_preserved_for_fail_closed_routing(monkeypatch):
    from app.data_providers import custom as custom_sources
    from app.services import preferences

    monkeypatch.setattr(custom_sources, "names", lambda: set())
    monkeypatch.setattr(
        custom_sources,
        "plugin_requires_source_isolation",
        lambda name: name == "tdx_mcp",
    )
    monkeypatch.setattr(
        custom_sources,
        "plugin_manifest",
        lambda name: {
            "datasets": ["daily", "adj_factor", "minute", "realtime", "depth5"],
        } if name == "tdx_mcp" else None,
    )
    monkeypatch.setattr(
        preferences,
        "load",
        lambda: {
            "daily_data_provider": "tdx_mcp",
            "minute_data_provider": "tdx_mcp",
            "depth5_data_provider": "tdx_mcp",
            "adj_factor_provider": "tdx_mcp",
            "realtime_data_provider": "tdx_mcp",
            "financial_data_provider": "tdx_mcp",
        },
    )

    assert preferences.get_daily_data_provider() == "tdx_mcp"
    assert preferences.get_minute_data_provider() == "tdx_mcp"
    assert preferences.get_depth5_data_provider() == "tdx_mcp"
    assert preferences.get_adj_factor_provider() == "tdx_mcp"
    assert preferences.get_realtime_data_provider() == "tdx_mcp"
    assert preferences.get_financial_provider() == "tickflow"
