"""EltdxProvider 契约与单位标准化测试。

不依赖真实网络: 用假 eltdx client 返回样例 K 线页/报价快照/除权事件, 验证字段映射、
单位口径 (CONTRIBUTING §3.1: ``volume_lots``/``total_hand`` 原样为手、百分数→小数、
指数成交量放大 100 倍)、K 线分页方向与页数上限、报价与五档按 80 只切批、软失败与
``failed_out``、除权因子推导、能力声明、availability 两态与 loader 注册。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from app.plugins.eltdx import bridge as eb
from app.plugins.eltdx import provider as ep
from app.plugins.eltdx.provider import EltdxProvider

_CN = ZoneInfo("Asia/Shanghai")


# =====================================================================
# 假 eltdx client
# =====================================================================


class _Bar:
    """KlineBar: ``time`` 带时区, 价格元, ``volume_lots`` 手, ``amount`` 元。"""

    def __init__(self, time, close=10.0, *, open_=None, high=None, low=None,
                 volume_lots=1000.0, amount=None):
        self.time = time
        self.open = (None if close is None else close - 0.1) if open_ is None else open_
        self.high = (None if close is None else close + 0.1) if high is None else high
        self.low = (None if close is None else close - 0.2) if low is None else low
        self.close = close
        self.volume_lots = volume_lots
        if amount is not None:
            self.amount = amount
        elif close is None or volume_lots is None:
            self.amount = None
        else:
            self.amount = close * 100 * volume_lots


def _day(year, month, day, **kw):
    return _Bar(datetime(year, month, day, 15, 0, tzinfo=_CN), **kw)


def _minute(hour, minute, **kw):
    return _Bar(datetime(2026, 8, 3, hour, minute, tzinfo=_CN), **kw)


class _Page:
    def __init__(self, bars):
        self.bars = tuple(bars)


class _Bars:
    """按标的各自记录页序, 支持按调用序号返回预置页。"""

    def __init__(self, pages_by_symbol=None, error=None):
        self.pages_by_symbol = dict(pages_by_symbol or {})
        self.error = error
        self.calls: list[dict] = []

    def get(self, code, period="day", start=0, count=800):
        self.calls.append({"code": code, "period": period, "start": start, "count": count})
        if self.error is not None:
            raise self.error
        pages = self.pages_by_symbol.get(code, [])
        index = sum(1 for call in self.calls if call["code"] == code) - 1
        if index >= len(pages):
            return _Page([])
        return _Page(pages[index])


class _Snapshot:
    """QuoteSnapshot: 价格元, ``total_hand`` 手, ``change_pct`` 上游为百分数。"""

    def __init__(self, code, exchange="sh", *, last=10.0, pre=9.5, open_=9.6,
                 high=10.2, low=9.4, hand=1234.0, amount=1.2e6):
        self.exchange = exchange
        self.code = code
        self.last_price = last
        self.pre_close_price = pre
        self.open_price = open_
        self.high_price = high
        self.low_price = low
        self.total_hand = hand
        self.amount = amount


class _CategoryRecord:
    """CategoryQuoteRecord (0x054b): 价格与成交额元, 量为手, 内/外盘为手。

    ``change_pct`` 属性刻意返回上游的百分数口径, 用于断言插件自行推导小数制。
    """

    def __init__(self, code, exchange="sh", *, open_=10.0, pre=9.5, last=10.2,
                 open_amount=1.5e7, hand=1234, amount=2.0e7, inside=600, outside=634,
                 bid1=10.19, bid_vol1=150, ask1=10.21, ask_vol1=90):
        self.exchange = exchange
        self.code = code
        self.open_price = open_
        self.pre_close_price = pre
        self.last_price = last
        self.open_amount = open_amount
        self.total_hand = hand
        self.amount = amount
        self.inside_dish = inside
        self.outer_disc = outside
        self.bid1 = bid1
        self.bid_vol1 = bid_vol1
        self.ask1 = ask1
        self.ask_vol1 = ask_vol1

    @property
    def change_pct(self):
        """上游口径是百分数 (10.2 = +10.2%), 插件不得透传。"""
        return 999.0


class _Level:
    def __init__(self, volume):
        self.volume = volume


class _LevelQuote:
    """``helpers.full_quotes`` 返回的五档报价。"""

    def __init__(self, code, exchange="sh", *, bids=(), asks=()):
        self.exchange = exchange
        self.code = code
        self.buy_levels = tuple(_Level(value) for value in bids)
        self.sell_levels = tuple(_Level(value) for value in asks)


class _Quotes:
    def __init__(self, snapshots=(), error=None, category_records=(), category_error=None):
        self.snapshots = list(snapshots)
        self.error = error
        self.calls: list[list[str]] = []
        self.category_records = list(category_records)
        self.category_error = category_error
        self.category_calls: list[dict] = []

    def get_snapshots(self, codes):
        self.calls.append(list(codes))
        if self.error is not None:
            raise self.error
        wanted = {str(code).lower() for code in codes}
        return [row for row in self.snapshots if _full_code(row) in wanted]

    def list_by_category(self, category, *, sort_by=None, start=0, count=80, ascending=False):
        """0x054b 分类行情分页: 记录按 start/count 切片, 末页短页。"""
        self.category_calls.append({
            "category": category, "sort_by": sort_by, "start": start,
            "count": count, "ascending": ascending,
        })
        if self.category_error is not None:
            raise self.category_error
        return _CategoryPage(self.category_records[start : start + count])


class _CategoryPage:
    def __init__(self, records):
        self.records = tuple(records)


class _Helpers:
    def __init__(self, quotes=(), error=None):
        self.quotes = list(quotes)
        self.error = error
        self.calls: list[list[str]] = []

    def full_quotes(self, codes):
        self.calls.append(list(codes))
        if self.error is not None:
            raise self.error
        wanted = {str(code).lower() for code in codes}
        return [row for row in self.quotes if _full_code(row) in wanted]


def _full_code(row):
    return f"{str(row.exchange).lower()}{str(row.code).zfill(6)}"


class _Event:
    """除权除息事件记录; 成分均为每 10 股口径。"""

    def __init__(self, day, *, category=1, c1=0.0, c2=None, c3=0.0, c4=0.0):
        self.date = day
        self.category_raw = category
        self.c1_value = c1
        self.c2_value = c2
        self.c3_value = c3
        self.c4_value = c4


class _Block:
    def __init__(self, full_code, records):
        self.full_code = full_code
        self.records = list(records)


class _Batch:
    def __init__(self, blocks):
        self.blocks = list(blocks)


class _Corporate:
    def __init__(self, events_by_symbol=None, error=None):
        self.events = {str(key).lower(): list(value) for key, value in (events_by_symbol or {}).items()}
        self.error = error
        self.calls: list[list[str]] = []

    def capital_changes(self, codes, batch_size=75):
        self.calls.append(list(codes))
        if self.error is not None:
            raise self.error
        blocks = []
        for code in codes:
            entries = self.events.get(str(code).lower())
            if not entries:
                continue
            # 测试数据按 (事件日, 记录) 书写, 上游 ``records`` 只有记录对象本身
            records = [entry[1] if isinstance(entry, tuple) else entry for entry in entries]
            blocks.append(_Block(code, records))
        return _Batch(blocks)


class _Security:
    """SecurityCode: 代码表条目。"""

    def __init__(self, full_code, name, category):
        self.full_code = full_code
        self.name = name
        self.category = category


class _Codes:
    def __init__(self, a_shares=(), etfs=(), indices=(), all_by_market=None):
        self._a = list(a_shares)
        self._e = list(etfs)
        self._i = list(indices)
        self._all = {key: list(value) for key, value in (all_by_market or {}).items()}
        self.universe_calls = 0
        self.all_calls: list[str] = []

    def all_a_shares(self):
        self.universe_calls += 1
        return list(self._a)

    def all_etfs(self):
        return list(self._e)

    def a_shares(self, market):
        return [row for row in self._all.get(market, []) if row.category == "a_share"]

    def etfs(self, market):
        return [row for row in self._all.get(market, []) if row.category == "etf"]

    def indices(self, market):
        return [row for row in self._all.get(market, []) if row.category == "index"]

    def all(self, market):
        self.all_calls.append(market)
        return list(self._all.get(market, ()))


class _FakeClient:
    def __init__(self, *, bars=None, bars_error=None, snapshots=(), snapshot_error=None,
                 level_quotes=(), level_error=None, events=None, corporate_error=None,
                 codes=None, universe_error=None, connect_error=None,
                 category_records=(), category_error=None):
        self.bars = _Bars(bars, bars_error)
        self.quotes = _Quotes(snapshots, snapshot_error, category_records, category_error)
        self.helpers = _Helpers(level_quotes, level_error)
        self.corporate = _Corporate(events, corporate_error)
        self.codes = codes if codes is not None else _Codes()
        self.universe_error = universe_error
        self.connect_error = connect_error
        self.connect_calls = 0
        self.closed = False
        if universe_error is not None:
            def _boom():
                raise universe_error

            self.codes.all_a_shares = _boom

    def connect(self):
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error

    def close(self):
        self.closed = True


def _install(monkeypatch, fake):
    """把 bridge.create_client 指向假 client, 返回可用的 Provider。"""
    monkeypatch.setattr(eb, "create_client", lambda: fake)
    return EltdxProvider()


# =====================================================================
# 代码格式与单位口径
# =====================================================================


def test_to_tdx_symbol_requires_exchange_suffix():
    assert ep._to_tdx_symbol("600519.SH") == "sh600519"
    assert ep._to_tdx_symbol("000001.sz") == "sz000001"
    assert ep._to_tdx_symbol(" 430047.BJ ") == "bj430047"
    # 裸码不猜交易所, 直接拒绝
    for bad in ("600519", "sh600519", "60051.SH", "600519.XX", ""):
        with pytest.raises(ValueError):
            ep._to_tdx_symbol(bad)


def test_from_tdx_symbol_roundtrip():
    assert ep._from_tdx_symbol("sz000001") == ("000001.SZ", "000001", "SZ")
    assert ep._from_tdx_symbol("SH600519") == ("600519.SH", "600519", "SH")
    with pytest.raises(ValueError):
        ep._from_tdx_symbol("600519.SH")


def test_volume_scale_only_index_needs_conversion():
    """指数 K 线成交量上游为"手/100", 股票/ETF 原样。"""
    assert ep._volume_scale("index") == 100.0
    assert ep._volume_scale("INDEX") == 100.0
    assert ep._volume_scale("stock") == 1.0
    assert ep._volume_scale("etf") == 1.0
    assert ep._INDEX_VOLUME_SCALE == 100.0


def test_finite_float_rejects_nan_and_booleans():
    assert ep._finite_float("1.5") == 1.5
    assert ep._finite_float(None) is None
    assert ep._finite_float(float("nan")) is None
    assert ep._finite_float(float("inf")) is None
    assert ep._finite_float(True) is None
    assert ep._finite_float("abc") is None


def test_level_numbers_pads_missing_levels_with_none():
    """缺档必须是 None, 不能伪造成 0 (会被误判为真封板)。"""
    levels = [_Level(100), _Level(None), _Level(300)]
    assert ep._level_numbers(levels) == [100, None, 300, None, None]
    assert ep._level_numbers(None) == [None] * 5
    # 超过五档只取前五
    assert ep._level_numbers([_Level(i) for i in range(8)]) == [0, 1, 2, 3, 4]


def test_beijing_naive_normalizes_shanghai_wallclock():
    aware = datetime(2026, 8, 3, 9, 31, tzinfo=_CN)
    assert ep._beijing_naive(aware) == datetime(2026, 8, 3, 9, 31)
    naive = datetime(2026, 8, 3, 9, 31)
    assert ep._beijing_naive(naive) == naive
    with pytest.raises(ValueError):
        ep._beijing_naive("2026-08-03")


# =====================================================================
# daily
# =====================================================================


def test_daily_maps_units_and_code_format(monkeypatch):
    """核心口径: 价格元、成交额元、``volume_lots`` 手原样透传。"""
    fake = _FakeClient(bars={
        "sh600519": [[_day(2026, 8, 3, close=1500.0, open_=1498.0, high=1502.0,
                           low=1495.0, volume_lots=1234.0, amount=1.85e8)]],
    })
    provider = _install(monkeypatch, fake)
    frame = provider.get_daily(["600519.SH"], None, None)
    assert frame.to_dicts() == [{
        "symbol": "600519.SH",
        "date": date(2026, 8, 3),
        "open": 1498.0,
        "high": 1502.0,
        "low": 1495.0,
        "close": 1500.0,
        "volume": 1234.0,
        "amount": 1.85e8,
    }]
    assert fake.bars.calls[0]["code"] == "sh600519"
    assert fake.bars.calls[0]["period"] == "day"


def test_daily_index_volume_scaled_by_100(monkeypatch):
    """指数日K成交量按"手/100"给出, 必须放大 100 倍还原; 股票不受影响。"""
    fake = _FakeClient(bars={
        "sh000001": [[_day(2026, 8, 3, close=3500.0, volume_lots=1234.0)]],
        "sh600519": [[_day(2026, 8, 3, close=1500.0, volume_lots=1234.0)]],
    })
    provider = _install(monkeypatch, fake)
    index_frame = provider.get_daily(["000001.SH"], None, None, "index")
    assert index_frame["volume"].to_list() == [123400.0]
    stock_frame = provider.get_daily(["600519.SH"], None, None, "stock")
    assert stock_frame["volume"].to_list() == [1234.0]


def test_daily_window_filter_and_sorting(monkeypatch):
    fake = _FakeClient(bars={
        "sh600519": [[
            _day(2026, 7, 31, close=1400.0),
            _day(2026, 8, 3, close=1500.0),
            _day(2026, 8, 4, close=1510.0),
        ]],
    })
    provider = _install(monkeypatch, fake)
    frame = provider.get_daily(
        ["600519.SH"], datetime(2026, 8, 3, 0, 0), datetime(2026, 8, 3, 23, 59),
    )
    assert frame["date"].to_list() == [date(2026, 8, 3)]


def test_daily_missing_values_stay_none(monkeypatch):
    """上游缺字段按 None 落库, 不伪造成 0。"""
    fake = _FakeClient(bars={
        "sh600519": [[_day(2026, 8, 3, close=None, volume_lots=None, amount=None)]],
    })
    provider = _install(monkeypatch, fake)
    row = provider.get_daily(["600519.SH"], None, None).to_dicts()[0]
    assert row["close"] is None and row["volume"] is None and row["amount"] is None


def test_daily_schema_is_stable_when_upstream_returns_nothing(monkeypatch):
    provider = _install(monkeypatch, _FakeClient(bars={"sh600519": [[]]}))
    frame = provider.get_daily(["600519.SH"], None, None)
    assert frame.height == 0
    assert frame.schema["symbol"] == ep.pl.String
    assert frame.schema["date"] == ep.pl.Date
    assert frame.schema["volume"] == ep.pl.Float64


def test_daily_empty_symbols_returns_empty_frame(monkeypatch):
    provider = _install(monkeypatch, _FakeClient())
    frame = provider.get_daily([], None, None)
    assert frame.height == 0 and frame.columns == list(ep._DAILY_SCHEMA)


def test_daily_pagination_walks_backwards_until_window_covered(monkeypatch):
    """``start`` 是相对最新一根的偏移量: 第 2 页应取更旧数据并覆盖窗口起点后停止。"""
    monkeypatch.setattr(ep, "_KLINE_PAGE_SIZE", 2)
    fake = _FakeClient(bars={
        "sh600519": [
            [_day(2026, 8, 3), _day(2026, 8, 4)],
            [_day(2026, 7, 30), _day(2026, 7, 31)],
            [_day(2026, 7, 28), _day(2026, 7, 29)],
        ],
    })
    provider = _install(monkeypatch, fake)
    frame = provider.get_daily(["600519.SH"], datetime(2026, 8, 1, 0, 0), None)
    assert [call["start"] for call in fake.bars.calls] == [0, 2]
    assert frame["date"].to_list() == [date(2026, 8, 3), date(2026, 8, 4)]


def test_daily_pagination_stops_on_short_page(monkeypatch):
    monkeypatch.setattr(ep, "_KLINE_PAGE_SIZE", 5)
    fake = _FakeClient(bars={"sh600519": [[_day(2026, 8, 3), _day(2026, 8, 4)]]})
    provider = _install(monkeypatch, fake)
    provider.get_daily(["600519.SH"], None, None)
    assert len(fake.bars.calls) == 1


def test_daily_pagination_is_capped(monkeypatch):
    """页数上限防止上游 count 异常导致死循环。"""
    monkeypatch.setattr(ep, "_KLINE_PAGE_SIZE", 2)
    monkeypatch.setattr(ep, "_MAX_KLINE_PAGES", 3)
    pages = [[_day(2026, 1, day), _day(2026, 1, day + 1)] for day in (1, 3, 5, 7)]
    fake = _FakeClient(bars={"sh600519": pages})
    provider = _install(monkeypatch, fake)
    frame = provider.get_daily(["600519.SH"], None, None)
    assert len(fake.bars.calls) == 3
    assert frame.height == 6


def test_daily_page_without_parseable_time_terminates_pagination(monkeypatch):
    """整体 schema 变化时告警并终止, 而不是静默返回空数据后继续翻页。"""
    monkeypatch.setattr(ep, "_KLINE_PAGE_SIZE", 2)
    fake = _FakeClient(bars={
        "sh600519": [[_Bar(None), _Bar(None)], [_day(2026, 8, 3), _day(2026, 8, 4)]],
    })
    provider = _install(monkeypatch, fake)
    frame = provider.get_daily(["600519.SH"], datetime(2026, 8, 1, 0, 0), None)
    assert len(fake.bars.calls) == 1
    assert frame.height == 0


def test_daily_page_without_bars_attribute_is_treated_as_empty(monkeypatch):
    fake = _FakeClient()
    fake.bars.get = lambda code, period="day", start=0, count=800: object()
    provider = _install(monkeypatch, fake)
    assert provider.get_daily(["600519.SH"], None, None).height == 0


def test_daily_soft_fail_reports_failed_symbols(monkeypatch):
    fake = _FakeClient(
        bars={"sh600519": [[_day(2026, 8, 3, close=1500.0)]]},
        bars_error=None,
    )
    provider = _install(monkeypatch, fake)

    def _partial(code, period="day", start=0, count=800):
        if code == "sz000001":
            raise RuntimeError("上游断流")
        return _Page([_day(2026, 8, 3, close=11.0)])

    fake.bars.get = _partial
    failed: list[str] = []
    frame = provider.get_daily(["600519.SH", "000001.SZ"], None, None, failed_out=failed)
    assert frame["symbol"].to_list() == ["600519.SH"]
    assert failed == ["000001.SZ"]


def test_daily_raises_when_every_symbol_fails(monkeypatch):
    fake = _FakeClient(bars_error=RuntimeError("TCP 断开"))
    provider = _install(monkeypatch, fake)
    with pytest.raises(RuntimeError, match="全部失败"):
        provider.get_daily(["600519.SH"], None, None)


def test_daily_invalid_symbol_reported_without_killing_batch(monkeypatch):
    fake = _FakeClient(bars={"sh600519": [[_day(2026, 8, 3)]]})
    provider = _install(monkeypatch, fake)
    failed: list[str] = []
    frame = provider.get_daily(["600519", "600519.SH"], None, None, failed_out=failed)
    assert frame["symbol"].to_list() == ["600519.SH"]
    assert failed == ["600519"]


def test_daily_client_unavailable_is_hard_failure(monkeypatch):
    fake = _FakeClient(connect_error=OSError("无法连接行情服务器"))
    provider = _install(monkeypatch, fake)
    with pytest.raises(RuntimeError, match="client 不可用"):
        provider.get_daily(["600519.SH"], None, None)
    assert provider._client is None


def test_daily_progress_callback_runs_for_every_symbol(monkeypatch):
    fake = _FakeClient(bars={"sh600519": [[_day(2026, 8, 3)]]})
    provider = _install(monkeypatch, fake)
    seen: list[tuple[int, int]] = []
    provider.get_daily(["600519.SH", "000001.SZ"], None, None,
                       on_chunk_done=lambda done, total: seen.append((done, total)))
    assert seen == [(1, 2), (2, 2)]


def test_client_connected_once_across_calls(monkeypatch):
    fake = _FakeClient(bars={"sh600519": [[_day(2026, 8, 3)]]})
    provider = _install(monkeypatch, fake)
    provider.get_daily(["600519.SH"], None, None)
    provider.get_daily(["600519.SH"], None, None)
    assert fake.connect_calls == 1


def test_close_releases_client_and_blocks_further_use(monkeypatch):
    fake = _FakeClient(bars={"sh600519": [[_day(2026, 8, 3)]]})
    provider = _install(monkeypatch, fake)
    provider.get_daily(["600519.SH"], None, None)
    provider.close()
    provider.close()  # 幂等
    assert fake.closed is True
    with pytest.raises(RuntimeError, match="正在重载"):
        provider.get_daily(["600519.SH"], None, None)


# =====================================================================
# minute
# =====================================================================


def test_minute_uses_1m_bars_and_beijing_wallclock(monkeypatch):
    fake = _FakeClient(bars={
        "sh600519": [[
            _minute(9, 31, close=1500.0, volume_lots=12.0),
            _minute(9, 32, close=1501.0, volume_lots=34.0),
        ]],
    })
    provider = _install(monkeypatch, fake)
    frame = provider.get_minute(["600519.SH"], None, None)
    assert fake.bars.calls[0]["period"] == "1m"
    assert frame["datetime"].to_list() == [
        datetime(2026, 8, 3, 9, 31), datetime(2026, 8, 3, 9, 32),
    ]
    assert frame["volume"].to_list() == [12.0, 34.0]


def test_minute_rejects_non_1m_frequency(monkeypatch):
    provider = _install(monkeypatch, _FakeClient())
    with pytest.raises(ValueError, match="1m"):
        provider.get_minute(["600519.SH"], None, None, freq="5m")


def test_minute_window_filter(monkeypatch):
    fake = _FakeClient(bars={
        "sh600519": [[_minute(9, 31), _minute(9, 32), _minute(9, 33)]],
    })
    provider = _install(monkeypatch, fake)
    frame = provider.get_minute(
        ["600519.SH"], datetime(2026, 8, 3, 9, 32), datetime(2026, 8, 3, 9, 32),
    )
    assert frame["datetime"].to_list() == [datetime(2026, 8, 3, 9, 32)]


def test_minute_partial_failure_reports_failed_out(monkeypatch):
    fake = _FakeClient(bars={"sh600519": [[_minute(9, 31, close=1500.0)]]})

    def _partial(code, period="1m", start=0, count=800):
        if code == "sz000001":
            raise RuntimeError("上游断流")
        return _Page([_minute(9, 31, close=11.0)])

    fake.bars.get = _partial
    provider = _install(monkeypatch, fake)
    failed: list[str] = []
    frame = provider.get_minute(["600519.SH", "000001.SZ"], None, None, failed_out=failed)
    assert frame["symbol"].to_list() == ["600519.SH"]
    assert failed == ["000001.SZ"]


def test_minute_raises_when_every_symbol_fails(monkeypatch):
    fake = _FakeClient(bars_error=RuntimeError("TCP 断开"))
    provider = _install(monkeypatch, fake)
    with pytest.raises(RuntimeError, match="全部失败"):
        provider.get_minute(["600519.SH"], None, None)


def test_minute_empty_symbols_returns_empty_frame(monkeypatch):
    provider = _install(monkeypatch, _FakeClient())
    frame = provider.get_minute([], None, None)
    assert frame.height == 0 and frame.columns == list(ep._MINUTE_SCHEMA)


# =====================================================================
# adj_factor: 交易所公式推导
# =====================================================================


def test_ex_factor_dividend_only():
    record = _Event(date(2026, 8, 1), c1=5.0)
    assert ep._ex_factor(20.0, record) == pytest.approx(20.0 / 19.5)


def test_ex_factor_bonus_and_allotment():
    record = _Event(date(2026, 8, 1), c3=10.0)
    assert ep._ex_factor(20.0, record) == pytest.approx(2.0)
    record = _Event(date(2026, 8, 1), c2=8.0, c4=3.0)
    assert ep._ex_factor(20.0, record) == pytest.approx(20.0 / ((20.0 * 10 + 3 * 8.0) / 13.0))


def test_ex_factor_combined_components():
    record = _Event(date(2026, 8, 1), c1=3.0, c2=6.0, c3=2.0, c4=1.0)
    expected = (20.0 * 10 - 3.0 + 1 * 6.0) / 13.0
    assert ep._ex_factor(20.0, record) == pytest.approx(20.0 / expected)


def test_ex_factor_returns_none_for_missing_inputs():
    assert ep._ex_factor(None, _Event(date(2026, 8, 1), c1=5.0)) is None
    assert ep._ex_factor(0.0, _Event(date(2026, 8, 1), c1=5.0)) is None
    # 配股却缺配股价: 不能按 0 元配股价编造因子
    assert ep._ex_factor(20.0, _Event(date(2026, 8, 1), c4=3.0)) is None
    # 无事件成分 → 比值为 1, 不写库
    assert ep._ex_factor(20.0, _Event(date(2026, 8, 1))) is None
    # 除权参考价 <= 0 视为脏数据
    assert ep._ex_factor(20.0, _Event(date(2026, 8, 1), c1=1000.0)) is None


def test_adj_factor_pipeline_derives_single_event_ratio(monkeypatch):
    events = {date(2026, 8, 1): _Event(date(2026, 8, 1), c1=5.0)}
    fake = _FakeClient(
        bars={"sh600519": [[_day(2026, 7, 30, close=19.8), _day(2026, 7, 31, close=20.0)]]},
        events={"sh600519": [(date(2026, 8, 1), events[date(2026, 8, 1)])]},
    )
    provider = _install(monkeypatch, fake)
    done: list[tuple[int, int]] = []
    frame = provider.get_adj_factors(
        ["600519.SH"], None, None,
        on_chunk_done=lambda count, total: done.append((count, total)),
    )
    assert frame.to_dicts() == [{
        "symbol": "600519.SH",
        "trade_date": date(2026, 8, 1),
        "ex_factor": pytest.approx(20.0 / 19.5),
    }]
    assert done == [(1, 1)]
    # 前收盘必须取自事件日前的原始日K, 窗口要早于首个事件
    assert fake.bars.calls[0]["code"] == "sh600519"


def test_adj_factor_ignores_non_ex_rights_records(monkeypatch):
    fake = _FakeClient(
        bars={"sh600519": [[_day(2026, 7, 31, close=20.0)]]},
        events={"sh600519": [
            (date(2026, 8, 1), _Event(date(2026, 8, 1), category=2, c1=5.0)),
        ]},
    )
    provider = _install(monkeypatch, fake)
    assert provider.get_adj_factors(["600519.SH"], None, None).height == 0
    assert fake.bars.calls == []


def test_adj_factor_window_filter_skips_out_of_range_events(monkeypatch):
    fake = _FakeClient(
        bars={"sh600519": [[_day(2026, 7, 31, close=20.0)]]},
        events={"sh600519": [(date(2026, 8, 1), _Event(date(2026, 8, 1), c1=5.0))]},
    )
    provider = _install(monkeypatch, fake)
    frame = provider.get_adj_factors(
        ["600519.SH"], datetime(2026, 9, 1, 0, 0), datetime(2026, 9, 30, 0, 0),
    )
    assert frame.height == 0
    assert fake.bars.calls == []


def test_adj_factor_skips_event_without_previous_close(monkeypatch):
    """日K窗口未覆盖事件前收盘时跳过, 不用事件日收盘顶替。"""
    fake = _FakeClient(
        bars={"sh600519": [[_day(2026, 8, 3, close=20.0)]]},
        events={"sh600519": [(date(2026, 8, 1), _Event(date(2026, 8, 1), c1=5.0))]},
    )
    provider = _install(monkeypatch, fake)
    assert provider.get_adj_factors(["600519.SH"], None, None).height == 0


def test_adj_factor_dedups_same_trade_date(monkeypatch):
    record = _Event(date(2026, 8, 1), c1=5.0)
    later = _Event(date(2026, 8, 1), c1=2.0)
    fake = _FakeClient(
        bars={"sh600519": [[_day(2026, 7, 31, close=20.0)]]},
        events={"sh600519": [(date(2026, 8, 1), record), (date(2026, 8, 1), later)]},
    )
    provider = _install(monkeypatch, fake)
    rows = provider.get_adj_factors(["600519.SH"], None, None).to_dicts()
    assert len(rows) == 1
    assert rows[0]["ex_factor"] == pytest.approx(20.0 / 19.8)


def test_adj_factor_batch_failure_isolates_other_symbols(monkeypatch):
    fake = _FakeClient(
        bars={"sh600519": [[_day(2026, 7, 31, close=20.0)]]},
        events={"sh600519": [(date(2026, 8, 1), _Event(date(2026, 8, 1), c1=5.0))]},
        corporate_error=RuntimeError("公告接口超时"),
    )
    provider = _install(monkeypatch, fake)
    assert provider.get_adj_factors(["600519.SH"], None, None).height == 0


def test_adj_factor_non_stock_asset_type_returns_empty(monkeypatch):
    fake = _FakeClient()
    provider = _install(monkeypatch, fake)
    frame = provider.get_adj_factors(["000001.SH"], None, None, "index")
    assert frame.height == 0 and frame.columns == list(ep._ADJ_FACTOR_SCHEMA)
    assert fake.corporate.calls == []


def test_adj_factor_empty_symbols_returns_empty(monkeypatch):
    provider = _install(monkeypatch, _FakeClient())
    assert provider.get_adj_factors([], None, None).height == 0


def test_adj_factor_skips_invalid_symbols(monkeypatch):
    fake = _FakeClient(events={"sh600519": [(date(2026, 8, 1), _Event(date(2026, 8, 1), c1=5.0))]})
    provider = _install(monkeypatch, fake)
    frame = provider.get_adj_factors(["600519", "BAD"], None, None)
    assert frame.height == 0
    assert fake.corporate.calls == []


# =====================================================================
# realtime: 全市场快照
# =====================================================================


def test_realtime_units_are_decimal_and_volume_is_lots(monkeypatch):
    """``total_hand`` 已是手; ``change_pct`` 用 change_amount/prev_close 的小数制。"""
    snapshot = _Snapshot("600519")
    snapshot.change_pct = 5.26  # 上游百分数字段: 不得直接采用
    fake = _FakeClient(
        snapshots=[snapshot],
        codes=_Codes(a_shares=["sh600519"]),
    )
    provider = _install(monkeypatch, fake)
    records = provider.get_realtime()
    assert len(records) == 1
    row = records[0]
    assert row["symbol"] == "600519.SH"
    assert row["last_price"] == 10.0
    assert row["prev_close"] == 9.5
    assert row["change_amount"] == pytest.approx(0.5)
    assert row["change_pct"] == pytest.approx(0.5 / 9.5)
    assert row["amplitude"] == pytest.approx((10.2 - 9.4) / 9.5)
    assert row["volume"] == 1234.0
    assert row["amount"] == 1.2e6
    # 快照不提供的字段必须为 None, 不启发式伪造
    assert row["name"] is None
    assert row["turnover_rate"] is None
    assert row["session"] is None
    assert row["timestamp"] > 0


def test_realtime_missing_prev_close_keeps_derived_fields_none(monkeypatch):
    fake = _FakeClient(
        snapshots=[_Snapshot("600519", pre=None)],
        codes=_Codes(a_shares=["sh600519"]),
    )
    provider = _install(monkeypatch, fake)
    row = provider.get_realtime()[0]
    assert row["prev_close"] is None
    assert row["change_amount"] is None and row["change_pct"] is None
    assert row["amplitude"] is None


def test_realtime_batches_snapshots_per_upstream_limit(monkeypatch):
    """上游单请求上限 80 只: 超出会静默截断, 必须自行切批。"""
    assert ep._QUOTE_BATCH_SIZE == 80
    monkeypatch.setattr(ep, "_QUOTE_BATCH_SIZE", 2)
    codes = ["sh600519", "sz000001", "sh510300"]
    fake = _FakeClient(
        snapshots=[_Snapshot("600519"), _Snapshot("000001", "sz"), _Snapshot("510300")],
        codes=_Codes(a_shares=codes),
    )
    provider = _install(monkeypatch, fake)
    records = provider.get_realtime()
    assert [len(call) for call in fake.quotes.calls] == [2, 1]
    assert [row["symbol"] for row in records] == ["600519.SH", "000001.SZ", "510300.SH"]


def test_realtime_universe_is_cached_between_polls(monkeypatch):
    fake = _FakeClient(snapshots=[_Snapshot("600519")], codes=_Codes(a_shares=["sh600519"]))
    provider = _install(monkeypatch, fake)
    provider.get_realtime()
    provider.get_realtime()
    assert fake.codes.universe_calls == 1
    assert len(fake.quotes.calls) == 2


def test_realtime_skips_malformed_codes_in_universe(monkeypatch):
    fake = _FakeClient(
        snapshots=[_Snapshot("600519")],
        codes=_Codes(a_shares=["sh600519", "600519", "bad"]),
    )
    provider = _install(monkeypatch, fake)
    assert provider.get_realtime()[0]["symbol"] == "600519.SH"
    assert fake.quotes.calls == [["sh600519"]]


def test_realtime_ignores_unrequested_responses(monkeypatch):
    fake = _FakeClient(
        snapshots=[_Snapshot("600519"), _Snapshot("000001", "sz")],
        codes=_Codes(a_shares=["sh600519"]),
    )
    provider = _install(monkeypatch, fake)
    assert [row["symbol"] for row in provider.get_realtime()] == ["600519.SH"]


def test_realtime_merges_stock_and_etf_universe(monkeypatch):
    fake = _FakeClient(
        snapshots=[_Snapshot("600519"), _Snapshot("510300")],
        codes=_Codes(a_shares=["sh600519"], etfs=["sh510300"]),
    )
    provider = _install(monkeypatch, fake)
    assert [row["symbol"] for row in provider.get_realtime()] == ["600519.SH", "510300.SH"]


def test_realtime_soft_fail_returns_empty_list(monkeypatch):
    """TCP 或代码表错误不能终止行情轮询, 返回空列表让调用方保留上轮快照。"""
    fake = _FakeClient(
        snapshots=[_Snapshot("600519")],
        codes=_Codes(a_shares=["sh600519"]),
        snapshot_error=OSError("连接被重置"),
    )
    provider = _install(monkeypatch, fake)
    assert provider.get_realtime() == []


def test_realtime_empty_universe_returns_empty_list(monkeypatch):
    provider = _install(monkeypatch, _FakeClient(codes=_Codes()))
    assert provider.get_realtime() == []


def test_realtime_universe_error_returns_empty_list(monkeypatch):
    fake = _FakeClient(universe_error=RuntimeError("代码表超时"))
    provider = _install(monkeypatch, fake)
    assert provider.get_realtime() == []


# ---- 指数快照 (可选插件协议) ----


def test_realtime_indices_maps_and_keeps_decimal_units(monkeypatch):
    fake = _FakeClient(snapshots=[_Snapshot("000001", last=3500.0, pre=3450.0)])
    provider = _install(monkeypatch, fake)
    records = provider.get_realtime_indices(["000001.SH"])
    assert [row["symbol"] for row in records] == ["000001.SH"]
    assert records[0]["change_amount"] == pytest.approx(50.0)
    assert records[0]["change_pct"] == pytest.approx(50.0 / 3450.0)


def test_realtime_indices_error_returns_none(monkeypatch):
    """失败返回 None, 与"成功但无数据"的空列表区分, 上层保留上轮指数缓存。"""
    fake = _FakeClient(snapshot_error=OSError("断流"))
    provider = _install(monkeypatch, fake)
    assert provider.get_realtime_indices(["000001.SH"]) is None


def test_realtime_indices_empty_or_invalid_returns_empty(monkeypatch):
    fake = _FakeClient(snapshots=[_Snapshot("000001")])
    provider = _install(monkeypatch, fake)
    assert provider.get_realtime_indices([]) == []
    assert provider.get_realtime_indices(["000001"]) == []
    assert fake.quotes.calls == []


# =====================================================================
# depth5
# =====================================================================


def test_depth5_maps_levels_and_missing_slots_to_none(monkeypatch):
    fake = _FakeClient(level_quotes=[
        _LevelQuote("600519", bids=[1, 2, 3, None, 5], asks=[10, 20]),
    ])
    provider = _install(monkeypatch, fake)
    result = provider.get_depth5(["600519.SH"])
    assert result["600519.SH"]["bid_volumes"] == [1, 2, 3, None, 5]
    assert result["600519.SH"]["ask_volumes"] == [10, 20, None, None, None]
    assert result["600519.SH"]["timestamp"] > 0
    assert fake.helpers.calls == [["sh600519"]]


def test_depth5_batches_by_upstream_limit(monkeypatch):
    monkeypatch.setattr(ep, "_QUOTE_BATCH_SIZE", 2)
    fake = _FakeClient(level_quotes=[
        _LevelQuote("600519", bids=[1]),
        _LevelQuote("000001", "sz", bids=[2]),
        _LevelQuote("510300", bids=[3]),
    ])
    provider = _install(monkeypatch, fake)
    result = provider.get_depth5(["600519.SH", "000001.SZ", "510300.SH"])
    assert [len(call) for call in fake.helpers.calls] == [2, 1]
    assert set(result) == {"600519.SH", "000001.SZ", "510300.SH"}


def test_depth5_error_returns_empty_dict(monkeypatch):
    """五档失败严格返回空, 由上层保留上轮缓存, 不换源也不编造档位。"""
    fake = _FakeClient(level_error=OSError("断流"))
    provider = _install(monkeypatch, fake)
    assert provider.get_depth5(["600519.SH"]) == {}


def test_depth5_empty_or_invalid_symbols_return_empty(monkeypatch):
    fake = _FakeClient()
    provider = _install(monkeypatch, fake)
    assert provider.get_depth5([]) == {}
    assert provider.get_depth5(["600519"]) == {}
    assert fake.helpers.calls == []


# =====================================================================
# 全市场竞价扫描 (0x054b 分类行情)
# =====================================================================


def test_market_auction_snapshot_maps_units_and_derives_volume(monkeypatch):
    """竞价量由"额 ÷ 今开 ÷ 100"还原为手; 涨跌幅自行推导小数制。"""
    record = _CategoryRecord(
        "600519", "sh", open_=230.0, pre=210.0, last=222.0, open_amount=529658300.0,
    )
    provider = _install(monkeypatch, _FakeClient(category_records=[record]))

    rows = provider.get_market_auction_snapshot()

    assert rows is not None and len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "600519.SH"
    assert row["open_price"] == 230.0
    assert row["prev_close"] == 210.0
    assert row["open_pct"] == pytest.approx((230.0 - 210.0) / 210.0)
    assert row["auction_amount"] == 529658300.0
    assert row["auction_volume"] == pytest.approx(529658300.0 / 230.0 / 100.0)
    # 封单额 (元) = 买一价 * 买一量(手) * 100
    assert row["seal_amount"] == pytest.approx(10.19 * 150 * 100.0)
    assert row["bid_volume1"] == 150
    assert row["ask_volume1"] == 90
    assert row["inside_volume"] == 600.0
    assert row["outside_volume"] == 634.0
    assert row["amount"] == 2.0e7
    assert row["volume"] == 1234.0
    assert isinstance(row["timestamp"], int) and row["timestamp"] > 0


def test_market_auction_snapshot_derives_change_pct_not_upstream_percent(monkeypatch):
    """上游 change_pct 是百分数 (999.0), 插件必须自己推导小数制。"""
    provider = _install(monkeypatch, _FakeClient(
        category_records=[_CategoryRecord("000001", "sz", open_=11.0, pre=10.0, last=11.5)],
    ))

    row = provider.get_market_auction_snapshot()[0]

    assert row["change_pct"] == pytest.approx(0.15)
    assert row["open_pct"] == pytest.approx(0.10)


def test_market_auction_snapshot_maps_bj_and_lowercase_exchange(monkeypatch):
    provider = _install(monkeypatch, _FakeClient(
        category_records=[_CategoryRecord("920107", "BJ")],
    ))
    assert provider.get_market_auction_snapshot()[0]["symbol"] == "920107.BJ"


def test_market_auction_snapshot_skips_suspended_and_unparsable_rows(monkeypatch):
    """停牌/无竞价成交 (今开或竞价额为 0) 与异常代码不得进入竞价量比。"""
    provider = _install(monkeypatch, _FakeClient(category_records=[
        _CategoryRecord("600000", "sh", open_=0.0, open_amount=0.0),
        _CategoryRecord("600001", "sh", open_=10.0, open_amount=0.0),
        _CategoryRecord("600002", "sh", open_=0.0, open_amount=1.0e7),
        _CategoryRecord("600003", "xx"),
        _CategoryRecord("6004", "sh"),
        _CategoryRecord("600004", "sh"),
    ]))

    rows = provider.get_market_auction_snapshot()

    assert [row["symbol"] for row in rows] == ["600004.SH"]


def test_market_auction_snapshot_derives_volume_when_open_price_only(monkeypatch):
    """竞价额与今开齐全即够还原量; 昨收缺失时涨跌幅留 None, 不伪造成 0。"""
    provider = _install(monkeypatch, _FakeClient(category_records=[
        _CategoryRecord("600005", "sh", pre=None, open_=20.0, open_amount=4.0e7),
    ]))

    row = provider.get_market_auction_snapshot()[0]

    assert row["open_pct"] is None
    assert row["change_pct"] is None
    assert row["auction_volume"] == pytest.approx(4.0e7 / 20.0 / 100.0)


def test_market_auction_snapshot_uses_market_category_and_open_amount_sort(monkeypatch):
    fake = _FakeClient(category_records=[_CategoryRecord("600000", "sh")])
    provider = _install(monkeypatch, fake)

    provider.get_market_auction_snapshot()

    assert ep._MARKET_CATEGORY == "沪深A股"
    assert ep._AUCTION_SORT_FIELD == "开盘金额"
    assert fake.quotes.category_calls == [{
        "category": "沪深A股", "sort_by": "开盘金额", "start": 0, "count": 80,
        "ascending": False,
    }]


def test_market_auction_snapshot_pages_until_short_page(monkeypatch):
    """满页继续翻页, 短页即停: 80 + 80 + 12 = 172 只, 只发 3 次请求。"""
    records = [
        _CategoryRecord(f"{600000 + index}", "sh")
        for index in range(172)
    ]
    fake = _FakeClient(category_records=records)
    provider = _install(monkeypatch, fake)

    rows = provider.get_market_auction_snapshot()

    assert len(rows) == 172
    assert [call["start"] for call in fake.quotes.category_calls] == [0, 80, 160]
    assert all(call["count"] == ep._MARKET_PAGE_SIZE for call in fake.quotes.category_calls)


def test_market_auction_snapshot_stops_on_empty_page(monkeypatch):
    fake = _FakeClient(category_records=[])
    provider = _install(monkeypatch, fake)

    assert provider.get_market_auction_snapshot() == []
    assert len(fake.quotes.category_calls) == 1


def test_market_auction_snapshot_is_capped_by_page_limit(monkeypatch):
    """上游始终回满页时的死循环兜底: 达到页数上限即停, 不无限翻页。"""
    records = [_CategoryRecord(f"{600000 + index}", "sh") for index in range(20000)]
    fake = _FakeClient(category_records=records)
    provider = _install(monkeypatch, fake)

    rows = provider.get_market_auction_snapshot()

    assert len(fake.quotes.category_calls) == ep._MAX_MARKET_PAGES
    assert rows is not None and len(rows) == ep._MAX_MARKET_PAGES * ep._MARKET_PAGE_SIZE


def test_market_auction_snapshot_dedups_overlapping_pages(monkeypatch):
    """翻页期间行情变动导致同一标的重复出现时只保留一条。"""
    record = _CategoryRecord("600519", "sh")
    fake = _FakeClient(category_records=[record] * 200)
    provider = _install(monkeypatch, fake)

    rows = provider.get_market_auction_snapshot()

    assert [row["symbol"] for row in rows] == ["600519.SH"]


def test_market_auction_snapshot_error_returns_none(monkeypatch):
    """软失败: 分类行情异常返回 None (调用方保留上轮结果), 不抛异常。"""
    provider = _install(monkeypatch, _FakeClient(category_error=RuntimeError("断流")))

    assert provider.get_market_auction_snapshot() is None


def test_market_auction_snapshot_client_unavailable_returns_none(monkeypatch):
    provider = _install(monkeypatch, _FakeClient(connect_error=RuntimeError("无连接")))

    assert provider.get_market_auction_snapshot() is None


def test_market_auction_snapshot_row_helper_requires_amount_and_open(monkeypatch):
    assert ep._auction_snapshot_row(_CategoryRecord("600000", "sh", open_amount=0.0), 1) is None
    assert ep._auction_snapshot_row(_CategoryRecord("600000", "sh", open_=0.0), 1) is None
    assert ep._auction_snapshot_row(_CategoryRecord("600000", "zz"), 1) is None
    assert ep._auction_snapshot_row(_CategoryRecord("600000", "sh"), 7)["timestamp"] == 7


# =====================================================================
# 标的维表
# =====================================================================


def _market_codes():
    return {
        "sh": [
            _Security("sh600519", "贵州茅台", "a_share"),
            _Security("sh510300", "沪深300ETF", "etf"),
            _Security("sh000001", "上证指数", "index"),
        ],
        "sz": [_Security("sz000001", "平安银行", "a_share")],
        "bj": [],
    }


def test_instruments_maps_asset_types(monkeypatch):
    fake = _FakeClient(codes=_Codes(all_by_market=_market_codes()))
    provider = _install(monkeypatch, fake)
    rows = provider.get_instruments("stock")
    assert [row["symbol"] for row in rows] == ["000001.SZ", "600519.SH"]
    assert rows[1] == {
        "symbol": "600519.SH",
        "name": "贵州茅台",
        "code": "600519",
        "exchange": "SH",
        "region": "CN",
        "type": "stock",
        "ext": {},
    }
    assert [row["symbol"] for row in provider.get_instruments("etf")] == ["510300.SH"]
    assert [row["symbol"] for row in provider.get_instruments("index")] == ["000001.SH"]
    assert fake.codes.all_calls == ["sh", "sz", "bj"] * 3


def test_instruments_missing_name_stays_none(monkeypatch):
    """名称缺失时不能用裸代码顶替 (会让历史 ST 标的按主板判涨跌停)。"""
    fake = _FakeClient(codes=_Codes(all_by_market={"sh": [_Security("sh600519", "", "a_share")]}))
    provider = _install(monkeypatch, fake)
    assert provider.get_instruments("stock")[0]["name"] is None


def test_instruments_skips_malformed_codes(monkeypatch):
    fake = _FakeClient(codes=_Codes(all_by_market={
        "sh": [_Security("600519", "裸码", "a_share"), _Security("sh600519", "贵州茅台", "a_share")],
    }))
    provider = _install(monkeypatch, fake)
    assert [row["symbol"] for row in provider.get_instruments("stock")] == ["600519.SH"]


def test_instruments_rejects_unknown_asset_type(monkeypatch):
    provider = _install(monkeypatch, _FakeClient())
    with pytest.raises(ValueError):
        provider.get_instruments("option")


def test_instruments_error_and_empty_are_hard_failures(monkeypatch):
    fake = _FakeClient()
    provider = _install(monkeypatch, fake)
    with pytest.raises(RuntimeError, match="代码表未返回"):
        provider.get_instruments("stock")

    boom = _FakeClient(codes=_Codes(all_by_market=_market_codes()))

    def _fail(market):
        raise OSError("读取板块文件失败")

    boom.codes.all = _fail
    failing = _install(monkeypatch, boom)
    with pytest.raises(RuntimeError, match="拉取失败"):
        failing.get_instruments("stock")


# =====================================================================
# 能力声明与清单
# =====================================================================


def test_datasets_declaration():
    """声明 daily/adj_factor/minute/realtime/depth5; financial 未声明。"""
    datasets = EltdxProvider().config.datasets
    assert set(datasets) == {"daily", "adj_factor", "minute", "realtime", "depth5"}
    assert "financial" not in datasets
    assert "full_minute" not in datasets


def test_provider_capability_flags():
    provider = EltdxProvider()
    assert provider.name == "eltdx"
    assert provider.builtin is True
    assert provider.daily_asset_types == frozenset({"stock", "etf", "index"})
    assert provider.instrument_asset_types == frozenset({"stock", "etf", "index"})
    # 按标的拉分钟, 不能承接全市场分钟落盘
    assert provider.supports_minute_universe_sync is False
    assert provider.minute_history_days == 90
    assert provider.supports_daily_failure_reporting is True
    assert provider.supports_minute_failure_reporting is True
    # 来源隔离: TCP 源故障时不得暗中改用 TickFlow
    assert provider.fallback_to_tickflow_on_error is False
    # 全市场要按代码表切 80 只一批, 不能按 1 秒下限轮询
    assert provider.realtime_min_interval == 30.0


def test_provider_has_dataset_false_for_undeclared(monkeypatch):
    from app.data_providers.custom import loader

    monkeypatch.setattr(loader, "_PROVIDERS", {"eltdx": EltdxProvider()})
    assert loader.provider_has_dataset("eltdx", "daily")
    assert loader.provider_has_dataset("eltdx", "depth5")
    assert not loader.provider_has_dataset("eltdx", "financial")
    assert not loader.provider_has_dataset("eltdx", "full_minute")


def test_plugin_manifest_declares_contract():
    from app.data_providers.custom import loader

    manifest = loader.plugin_manifest("eltdx")
    assert manifest is not None
    assert manifest["name"] == "eltdx"
    assert manifest["entry"] == "app.plugins.eltdx.provider:EltdxProvider"
    assert manifest["check"] == "app.plugins.eltdx.bridge:availability"
    assert manifest["runtime"] == "python"
    assert {"daily", "adj_factor", "minute", "realtime", "depth5"} <= set(manifest["datasets"])
    assert manifest["fallback_to_tickflow_on_error"] is False
    assert manifest["homepage"] == "https://github.com/electkismet/eltdx"
    assert manifest.get("hidden") is not True
    # 免费源: 不能声明 API Key 契约
    assert not manifest.get("api_key_env")


def test_plugin_requires_source_isolation():
    from app.data_providers.custom import loader

    assert loader.plugin_requires_source_isolation("eltdx") is True


def test_loader_registers_plugin(monkeypatch):
    from app.data_providers import custom as cs
    from app.data_providers.custom import loader

    monkeypatch.setattr(loader, "_PROVIDERS", {})
    monkeypatch.setattr(loader, "_PLUGIN_STATUS", {})
    monkeypatch.setattr(loader, "_call_check", lambda ref: (True, "ok"))

    manifest = loader.plugin_manifest("eltdx")
    loader._register_one_plugin(manifest)

    assert loader._PLUGIN_STATUS["eltdx"]["runtime"] == "python"
    assert loader._PLUGIN_STATUS["eltdx"]["available"] is True
    assert isinstance(loader._PROVIDERS["eltdx"], EltdxProvider)
    assert cs.provider_has_dataset("eltdx", "realtime")
    assert cs.is_builtin("eltdx")
    # 内置源不出现在用户自定义源列表
    assert "eltdx" not in [source["name"] for source in cs.list_sources()]


# =====================================================================
# availability / bridge 边界
# =====================================================================


_READY_API = {
    "bars": ("get",),
    "quotes": ("get_snapshots",),
    "corporate": ("capital_changes",),
    "codes": ("all_a_shares", "all_etfs", "a_shares", "etfs", "indices"),
    "helpers": ("full_quotes",),
}


class _Namespace:
    pass


def _ready_client_class(drop=()):
    """构造具备全部必需入口的假 client 类 (可指定缺失项以覆盖探测分支)。"""

    class _Client:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            for namespace, methods in _READY_API.items():
                if namespace in drop:
                    setattr(self, namespace, None)
                    continue
                holder = _Namespace()
                for method in methods:
                    if f"{namespace}.{method}" not in drop:
                        setattr(holder, method, lambda *args, **kwargs: None)
                setattr(self, namespace, holder)

        def close(self):
            pass

    return _Client


def test_availability_reports_missing_dependency(monkeypatch):
    def _boom():
        raise ImportError("No module named 'eltdx'")

    monkeypatch.setattr(eb, "_client_class", _boom)
    ok, reason = eb.availability()
    assert ok is False and "eltdx" in reason


def test_availability_ok_for_compatible_version(monkeypatch):
    monkeypatch.setattr(eb, "_client_class", lambda: _ready_client_class())
    monkeypatch.setattr(eb, "version", lambda name: "3.2.2")
    assert eb.availability() == (True, "ok (eltdx 3.2.2)")


def test_availability_rejects_legacy_flat_api(monkeypatch):
    """0.5.x 的扁平接口在 3.x 已移除, 版本不在区间内必须判为不可用。"""
    monkeypatch.setattr(eb, "version", lambda name: "0.5.1")
    monkeypatch.setattr(eb, "_client_class", _ready_client_class())
    ok, reason = eb.availability()
    assert ok is False and "0.5.1" in reason and "3.2" in reason


def test_availability_rejects_prerelease(monkeypatch):
    monkeypatch.setattr(eb, "version", lambda name: "3.3.0rc1")
    monkeypatch.setattr(eb, "_client_class", _ready_client_class())
    ok, reason = eb.availability()
    assert ok is False and "rc1" in reason


def test_availability_reports_missing_api_method(monkeypatch):
    monkeypatch.setattr(eb, "version", lambda name: "3.2.2")
    monkeypatch.setattr(eb, "_client_class", lambda: _ready_client_class(drop={"quotes.get_snapshots"}))
    ok, reason = eb.availability()
    assert ok is False and "quotes.get_snapshots" in reason


def test_availability_reports_missing_namespace(monkeypatch):
    monkeypatch.setattr(eb, "version", lambda name: "3.2.2")
    monkeypatch.setattr(eb, "_client_class", lambda: _ready_client_class(drop={"helpers"}))
    ok, reason = eb.availability()
    assert ok is False and "helpers" in reason


def test_availability_reports_unparseable_version(monkeypatch):
    monkeypatch.setattr(eb, "version", lambda name: "unknown")
    monkeypatch.setattr(eb, "_client_class", _ready_client_class())
    ok, reason = eb.availability()
    assert ok is False and "unknown" in reason


def test_availability_reports_package_metadata_missing(monkeypatch):
    from importlib.metadata import PackageNotFoundError

    def _missing(name):
        raise PackageNotFoundError(name)

    monkeypatch.setattr(eb, "version", _missing)
    monkeypatch.setattr(eb, "_client_class", _ready_client_class())
    ok, reason = eb.availability()
    assert ok is False and "版本" in reason


def test_availability_never_raises_on_broken_client_construction(monkeypatch):
    """``check`` 是插件扫描入口: 依赖损坏只能判不可用, 不能抛异常打断启动。"""

    class _Exploding:
        def __init__(self, **kwargs):
            raise RuntimeError("初始化失败")

    monkeypatch.setattr(eb, "version", lambda name: "3.2.2")
    monkeypatch.setattr(eb, "_client_class", lambda: _Exploding)
    ok, reason = eb.availability()
    assert ok is False and "构造客户端失败" in reason


def test_missing_api_probe_builds_instance_without_probing_hosts(monkeypatch):
    """3.x 子 API 挂在实例上, 探测必须基于实例; 且不能触发测速/连接。"""
    captured = {}
    cls = _ready_client_class()
    original_init = cls.__init__

    def _init(self, **kwargs):
        captured.update(kwargs)
        original_init(self, **kwargs)

    cls.__init__ = _init
    assert eb._missing_api(cls) == []
    assert captured == {"timeout": 5.0, "probe_hosts": False}


def test_create_client_uses_builtin_host_list(monkeypatch):
    """不接受来自设置页/环境变量的任意 host, 避免变成任意 TCP 出口。"""
    captured = {}

    class _Client:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(eb, "_client_class", lambda: _Client)
    client = eb.create_client()
    assert isinstance(client, _Client)
    assert captured == {"timeout": 8.0, "pool_size": 2, "probe_hosts": False}


def test_create_client_without_dependency_raises_readable_error(monkeypatch):
    def _boom():
        raise ImportError("No module named 'eltdx'")

    monkeypatch.setattr(eb, "_client_class", _boom)
    with pytest.raises(eb.EltdxBridgeError, match="缺少 eltdx 依赖"):
        eb.create_client()


def test_provider_availability_delegates_to_bridge(monkeypatch):
    monkeypatch.setattr(eb, "availability", lambda: (False, "模拟不可用"))
    assert ep.availability() == (False, "模拟不可用")


# =====================================================================
# 设置页试拉
# =====================================================================


def _recent_day(days_ago, **kw):
    when = datetime.now(_CN) - timedelta(days=days_ago)
    return _Bar(when.replace(hour=15, minute=0, second=0, microsecond=0), **kw)


def _recent_minute(**kw):
    when = datetime.now(_CN) - timedelta(minutes=1)
    return _Bar(when.replace(second=0, microsecond=0), **kw)


def test_test_dataset_daily_preview_serializes_dates(monkeypatch):
    older = _recent_day(2, close=9.9)
    newer = _recent_day(1, close=10.0)
    fake = _FakeClient(bars={"sh600000": [[newer, older]]})
    provider = _install(monkeypatch, fake)
    out = provider.test_dataset("daily", ["600000.SH"])
    assert out["provider"] == "eltdx" and out["dataset"] == "daily"
    assert out["rows"] == 2
    # date 序列化为 ISO 字符串, 且按交易日升序
    assert out["preview"][0]["date"] == older.time.date().isoformat()
    assert out["preview"][1]["date"] == newer.time.date().isoformat()
    assert fake.bars.calls[0]["code"] == "sh600000"


def test_test_dataset_caps_symbols_to_three(monkeypatch):
    """单击测试只能打小窗口, 不能触发 5 只标的全历史请求。"""
    fake = _FakeClient(bars={f"sh60000{i}": [[_recent_day(1)]] for i in range(5)})
    provider = _install(monkeypatch, fake)
    provider.test_dataset("daily", [f"60000{i}.SH" for i in range(5)])
    assert len(fake.bars.calls) == 3


def test_test_dataset_minute_uses_first_symbol_only(monkeypatch):
    fake = _FakeClient(bars={"sh600000": [[_recent_minute()]]})
    provider = _install(monkeypatch, fake)
    out = provider.test_dataset("minute", ["600000.SH", "600519.SH"])
    assert out["rows"] == 1
    assert [call["code"] for call in fake.bars.calls] == ["sh600000"]


def test_test_dataset_adj_factor_preview(monkeypatch):
    today = datetime.now(_CN).date()
    event_day = today - timedelta(days=1)
    fake = _FakeClient(
        bars={"sh600000": [[_recent_day(2, close=20.0)]]},
        events={"sh600000": [(event_day, _Event(event_day, c1=5.0))]},
    )
    provider = _install(monkeypatch, fake)
    out = provider.test_dataset("adj_factor", ["600000.SH"])
    assert out["rows"] == 1
    assert out["preview"][0]["trade_date"] == event_day.isoformat()


def test_test_dataset_realtime_preview(monkeypatch):
    fake = _FakeClient(snapshots=[_Snapshot("600519")], codes=_Codes(a_shares=["sh600519"]))
    provider = _install(monkeypatch, fake)
    out = provider.test_dataset("realtime")
    assert out["rows"] == 1
    assert "last_price" in out["columns"]
    assert out["preview"][0]["symbol"] == "600519.SH"


def test_test_dataset_depth5_preview(monkeypatch):
    fake = _FakeClient(level_quotes=[_LevelQuote("600000", bids=[1, 2])])
    provider = _install(monkeypatch, fake)
    out = provider.test_dataset("depth5", ["600000.SH"])
    assert out["rows"] == 1
    assert out["columns"] == ["ask_volumes", "bid_volumes", "timestamp"]
    assert out["preview"][0]["symbol"] == "600000.SH"


def test_test_dataset_unsupported_dataset_reports_error(monkeypatch):
    provider = _install(monkeypatch, _FakeClient())
    out = provider.test_dataset("financial")
    assert out["rows"] == 0 and "未声明" in out["error"]


def test_test_dataset_reports_failure_instead_of_raising(monkeypatch):
    fake = _FakeClient(bars_error=RuntimeError("TCP 断开"))
    provider = _install(monkeypatch, fake)
    out = provider.test_dataset("daily", ["600000.SH"])
    assert out["rows"] == 0 and "TCP 断开" in out["error"]


def test_test_dataset_defaults_to_shanghai_sample(monkeypatch):
    fake = _FakeClient(bars={"sh600000": [[_recent_day(1)]]})
    provider = _install(monkeypatch, fake)
    provider.test_dataset("daily")
    assert fake.bars.calls[0]["code"] == "sh600000"


def test_close_without_client_is_safe():
    EltdxProvider().close()
