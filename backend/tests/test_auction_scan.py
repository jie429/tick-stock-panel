"""全市场竞价扫描服务测试 (不依赖真实网络与真实数据源)。

覆盖: 源未实现协议 → source_unavailable、竞价未结束且无历史 → not_ready、当日扫描
落盘、按日落盘 → 竞价量比基线、首日无基线时退化为高开榜、阈值可配、扫描失败回退
上一份快照、进程内 TTL 缓存、当日只写一次盘 (refresh 才重写)、名称补全、昨日全天
成交额口径。日期用 2026-08-26/27/28/31 (写作时为过去交易日), 与仓库既有绝对日期
测试风格一致。
"""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from app.services import auction_scan as svc
from app.services import trading_day


def _beijing(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute)


def _record(symbol, *, open_=10.0, pre=9.0, amount=1.0e7, volume=None, last=None):
    """Provider ``get_market_auction_snapshot`` 的行 (单位: 元 / 手)。"""
    return {
        "symbol": symbol,
        "open_price": open_,
        "prev_close": pre,
        "open_pct": (open_ - pre) / pre if pre else None,
        "change_pct": 0.0,
        "last_price": last if last is not None else open_,
        "auction_amount": amount,
        "auction_volume": volume if volume is not None else amount / open_ / 100.0,
        "bid1": open_,
        "ask1": open_ + 0.01,
        "bid_volume1": 100,
        "ask_volume1": 100,
        "seal_amount": open_ * 100 * 100.0,
        "inside_volume": 10.0,
        "outside_volume": 20.0,
        "amount": 1.0e8,
        "volume": 1000.0,
        "timestamp": 1788505200000,
    }


class _FakeProvider:
    """记录调用次数; rows=None 模拟软失败 (协议调用返回 None)。"""

    def __init__(self, rows=None):
        self.rows = rows
        self.calls = 0

    def get_market_auction_snapshot(self):
        self.calls += 1
        return self.rows


def _write_kline(data_dir: Path, day: str, rows: list[tuple[str, float]]) -> None:
    part = data_dir / "kline_daily" / f"date={day}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [row[0] for row in rows],
        "amount": [row[1] for row in rows],
    }).write_parquet(part / "part-0.parquet")


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    for day in ("2026-08-26", "2026-08-27", "2026-08-28"):
        (tmp_path / "kline_daily" / f"date={day}").mkdir(parents=True, exist_ok=True)
    _write_kline(tmp_path, "2026-08-28", [("600519.SH", 8.0e8), ("000858.SZ", 2.0e8)])
    return tmp_path


@pytest.fixture(autouse=True)
def _clear_cache():
    svc.reset_cache()
    yield
    svc.reset_cache()


def _set_trading_day(monkeypatch, verdict) -> None:
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: verdict)


def _no_provider(monkeypatch) -> None:
    monkeypatch.setattr(svc, "_resolve_provider", lambda: None)


def _stored_snapshot(data_dir: Path, day: str) -> list[dict]:
    return svc._read_snapshot(svc._snapshot_path(data_dir, date.fromisoformat(day)))


# =====================================================================
# 源与状态
# =====================================================================


def test_source_unavailable_when_route_lacks_protocol(data_dir, monkeypatch):
    """实时行情源未实现可选协议 → 明确不可用, 不换源也不编造数据。"""
    _set_trading_day(monkeypatch, True)
    _no_provider(monkeypatch)

    payload = svc.get_auction_scan(data_dir, None, now=_beijing(2026, 8, 31, 10, 0))

    assert payload["state"] == "source_unavailable"
    assert payload["items"] == []
    assert payload["counts"] == {"scanned": 0, "high_open": 0, "hits": 0}
    assert "get_market_auction_snapshot" in payload["message"]


def test_not_ready_before_auction_without_history(data_dir, monkeypatch):
    """交易日 09:25 前上游尚无竞价成交 → not_ready, 且不触发扫描。"""
    _set_trading_day(monkeypatch, True)
    provider = _FakeProvider([_record("600519.SH")])

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 9, 10), provider=provider,
    )

    assert payload["state"] == "not_ready"
    assert provider.calls == 0
    assert "09:25" in payload["message"]


def test_no_data_when_scan_fails_without_history(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)
    provider = _FakeProvider(None)

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0), provider=provider,
    )

    assert payload["state"] == "no_data"
    assert provider.calls == 1


def test_holiday_without_source_serves_stored_snapshot(data_dir, monkeypatch):
    """休市日不扫描: 直接读最近一份落盘快照 (源甚至不必可用)。"""
    _set_trading_day(monkeypatch, True)
    svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0),
        provider=_FakeProvider([_record("600519.SH")]),
    )
    _set_trading_day(monkeypatch, False)
    _no_provider(monkeypatch)

    payload = svc.get_auction_scan(data_dir, None, now=_beijing(2026, 9, 1, 10, 0))

    assert payload["state"] == "ok"
    assert payload["trade_date"] == "2026-08-31"


def test_scan_failure_falls_back_to_stored_snapshot(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)
    svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0),
        provider=_FakeProvider([_record("600519.SH")]),
    )
    svc.reset_cache()
    provider = _FakeProvider(None)

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 9, 1, 10, 0), provider=provider,
    )

    assert provider.calls == 1
    assert payload["state"] == "ok"
    assert payload["trade_date"] == "2026-08-31"


# =====================================================================
# 落盘与缓存
# =====================================================================


def test_scan_persists_snapshot_for_the_day(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)
    rows = [_record("600519.SH"), _record("000858.SZ")]

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0), provider=_FakeProvider(rows),
    )

    assert payload["state"] == "ok"
    assert payload["trade_date"] == "2026-08-31"
    assert payload["total"] == 2
    assert payload["history_days"] == 1
    stored = _stored_snapshot(data_dir, "2026-08-31")
    assert sorted(row["symbol"] for row in stored) == ["000858.SZ", "600519.SH"]
    assert stored[0]["auction_amount"] == 1.0e7
    assert stored[0]["bid_volume1"] == 100


def test_snapshot_is_written_once_per_day_and_refresh_rewrites(data_dir, monkeypatch):
    """竞价字段当日不可变: 同一天重复扫描不重写文件, 显式 refresh 才重写。"""
    _set_trading_day(monkeypatch, True)
    now = _beijing(2026, 8, 31, 10, 0)
    path = svc._snapshot_path(data_dir, date(2026, 8, 31))

    svc.get_auction_scan(data_dir, None, now=now, provider=_FakeProvider([_record("600519.SH")]))
    first = os.stat(path).st_mtime_ns
    svc.reset_cache()
    svc.get_auction_scan(data_dir, None, now=now, provider=_FakeProvider([_record("000858.SZ")]))
    assert os.stat(path).st_mtime_ns == first
    assert [row["symbol"] for row in _stored_snapshot(data_dir, "2026-08-31")] == ["600519.SH"]

    svc.reset_cache()
    svc.get_auction_scan(
        data_dir, None, now=now, provider=_FakeProvider([_record("000858.SZ")]), refresh=True,
    )
    assert [row["symbol"] for row in _stored_snapshot(data_dir, "2026-08-31")] == ["000858.SZ"]


def test_ttl_cache_avoids_rescanning_within_window(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)
    provider = _FakeProvider([_record("600519.SH")])
    now = _beijing(2026, 8, 31, 10, 0)

    svc.get_auction_scan(data_dir, None, now=now, provider=provider)
    svc.get_auction_scan(data_dir, None, now=now, provider=provider)

    assert provider.calls == 1


def test_snapshot_roundtrip_keeps_nullable_columns(data_dir):
    rows = [_record("600519.SH"), {**_record("000858.SZ"), "bid_volume1": None, "ask1": None}]

    svc._write_snapshot(svc._snapshot_path(data_dir, date(2026, 8, 31)), rows)
    stored = _stored_snapshot(data_dir, "2026-08-31")

    assert set(svc._SNAPSHOT_SCHEMA).issubset(set(stored[0]))
    assert stored[1]["bid_volume1"] is None
    assert stored[1]["ask1"] is None


def test_snapshot_days_ignores_foreign_files(data_dir):
    root = data_dir / svc._SCAN_DIR
    root.mkdir(parents=True, exist_ok=True)
    (root / "date=2026-08-31.parquet").write_bytes(b"")
    (root / "date=2026-13-45.parquet").write_bytes(b"")
    (root / "notes.txt").write_text("x", encoding="utf-8")

    assert svc.snapshot_days(data_dir) == [date(2026, 8, 31)]


def test_prev_day_amounts_uses_latest_earlier_partition(data_dir):
    _write_kline(data_dir, "2026-08-27", [("600519.SH", 5.0e8)])

    assert svc._prev_day_amounts(data_dir, date(2026, 8, 31)) == {
        "600519.SH": 8.0e8, "000858.SZ": 2.0e8,
    }
    assert svc._prev_day_amounts(data_dir, date(2026, 8, 26)) == {}
    assert svc._prev_day_amounts(data_dir, date(2026, 8, 28)) == {"600519.SH": 5.0e8}


# =====================================================================
# 竞价量比与筛选
# =====================================================================


def _seed_baseline(data_dir: Path, monkeypatch) -> None:
    """写入 2026-08-28 基线: 600519 竞价量 1000 手 / 1e7 元, 000858 竞价量 2000 手。"""
    _set_trading_day(monkeypatch, True)
    svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 28, 10, 0),
        provider=_FakeProvider([
            _record("600519.SH", open_=10.0, pre=9.5, amount=1.0e7, volume=1000.0),
            _record("000858.SZ", open_=20.0, pre=19.0, amount=4.0e7, volume=2000.0),
        ]),
    )
    svc.reset_cache()


def test_ratio_uses_previous_snapshot_and_filters_by_thresholds(data_dir, monkeypatch):
    _seed_baseline(data_dir, monkeypatch)
    rows = [
        # 高开 6% + 竞价量 20 倍 → 命中
        _record("600519.SH", open_=10.0, pre=9.434, amount=2.0e7, volume=20000.0),
        # 高开 1% (低于门槛) + 量比 50 → 不命中
        _record("000858.SZ", open_=20.2, pre=20.0, amount=1.0e8, volume=100000.0),
        # 高开 8% + 量比 2 (低于门槛) → 不命中
        _record("601318.SH", open_=10.8, pre=10.0, amount=1.0e7, volume=1000.0),
    ]

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0), provider=_FakeProvider(rows),
    )

    assert payload["state"] == "ok"
    assert payload["ratio_ready"] is True
    assert payload["baseline_date"] == "2026-08-28"
    assert payload["counts"] == {"scanned": 3, "high_open": 2, "hits": 1}
    item = payload["items"][0]
    assert item["symbol"] == "600519.SH"
    assert item["ratio_volume"] == pytest.approx(20.0)
    assert item["ratio_amount"] == pytest.approx(2.0)
    # 竞价额占比 = 今日竞价额 ÷ 昨日 (08-28) 全天成交额
    assert item["prev_amount_share"] == pytest.approx(2.0e7 / 8.0e8)
    assert item["open_pct"] == pytest.approx((10.0 - 9.434) / 9.434, abs=1e-6)


def test_first_day_without_baseline_lists_high_open_by_amount(data_dir, monkeypatch):
    """首日没有基线: 不假装能筛量比, 退化为高开榜 (按竞价额降序), ratio 留 None。"""
    _set_trading_day(monkeypatch, True)
    rows = [
        _record("600519.SH", open_=10.0, pre=9.0, amount=1.0e7),
        _record("000858.SZ", open_=20.0, pre=19.0, amount=9.0e7),
        _record("601318.SH", open_=10.05, pre=10.0, amount=5.0e7),
    ]

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0), provider=_FakeProvider(rows),
    )

    assert payload["ratio_ready"] is False
    assert payload["baseline_date"] is None
    assert payload["counts"] == {"scanned": 3, "high_open": 2, "hits": 2}
    assert [item["symbol"] for item in payload["items"]] == ["000858.SZ", "600519.SH"]
    assert all(item["ratio_volume"] is None for item in payload["items"])


def test_thresholds_and_limit_are_configurable(data_dir, monkeypatch):
    _seed_baseline(data_dir, monkeypatch)
    rows = [
        _record("600519.SH", open_=10.0, pre=9.9, amount=2.0e7, volume=20000.0),
        _record("000858.SZ", open_=20.0, pre=19.9, amount=4.0e7, volume=40000.0),
    ]

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0), provider=_FakeProvider(rows),
        min_open_pct=0.0, min_ratio=0.0, limit=1,
    )

    assert payload["thresholds"] == {"min_open_pct": 0.0, "min_ratio": 0.0}
    assert payload["counts"] == {"scanned": 2, "high_open": 2, "hits": 2}
    assert len(payload["items"]) == 1
    # 量比降序: 000858 (40 倍) 在前
    assert payload["items"][0]["symbol"] == "000858.SZ"


def test_ratio_ignores_zero_and_missing_baseline_values(data_dir, monkeypatch):
    """基线竞价量缺失或为 0 时该股量比留 None, 不伪造成 0 或无穷大。"""
    _set_trading_day(monkeypatch, True)
    svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 28, 10, 0),
        provider=_FakeProvider([
            {**_record("600519.SH", open_=10.0, pre=9.0), "auction_volume": 0.0},
            {**_record("000858.SZ", open_=20.0, pre=19.0), "auction_volume": None},
        ]),
    )
    svc.reset_cache()

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0),
        provider=_FakeProvider([_record("600519.SH"), _record("000858.SZ")]),
        min_open_pct=0.0, min_ratio=0.0,
    )

    assert payload["ratio_ready"] is True
    # 基线无效 → 量比 None, 任何阈值下都不算命中 (也不会误判成 0 倍)
    assert payload["counts"] == {"scanned": 2, "high_open": 2, "hits": 0}
    assert payload["items"] == []
    assert svc._ratio(100.0, 0.0) is None
    assert svc._ratio(100.0, None) is None
    assert svc._ratio(100.0, 50.0) == pytest.approx(2.0)


# =====================================================================
# 名称补全
# =====================================================================


class _Repo:
    def __init__(self, names):
        self.names = names

    def get_name_map(self, symbols):
        return {symbol: self.names[symbol] for symbol in symbols if symbol in self.names}


def test_names_attached_from_repo_and_missing_stays_none(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)
    rows = [_record("600519.SH"), _record("000858.SZ")]
    repo = _Repo({"600519.SH": "贵州茅台"})

    payload = svc.get_auction_scan(
        data_dir, repo, now=_beijing(2026, 8, 31, 10, 0), provider=_FakeProvider(rows),
    )

    names = {item["symbol"]: item["name"] for item in payload["items"]}
    assert names == {"600519.SH": "贵州茅台", "000858.SZ": None}


def test_names_skipped_without_repo(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)

    payload = svc.get_auction_scan(
        data_dir, None, now=_beijing(2026, 8, 31, 10, 0),
        provider=_FakeProvider([_record("600519.SH")]),
    )

    assert payload["items"][0]["name"] is None


def test_repo_name_lookup_failure_does_not_break_scan(data_dir, monkeypatch):
    class _Boom:
        def get_name_map(self, symbols):
            raise RuntimeError("维表不可用")

    _set_trading_day(monkeypatch, True)

    payload = svc.get_auction_scan(
        data_dir, _Boom(), now=_beijing(2026, 8, 31, 10, 0),
        provider=_FakeProvider([_record("600519.SH")]),
    )

    assert payload["state"] == "ok"
    assert payload["items"][0]["symbol"] == "600519.SH"

# =====================================================================
# API 层 (路由 / 查询参数 / repo 透传)
# =====================================================================


def test_api_route_binds_query_params_and_repo(tmp_path, monkeypatch):
    """路由契约: 路径、查询参数与 repo 透传, 前端与文档都按这个形状调用。"""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api import abnormal

    seen: dict = {}

    def _fake_scan(data_dir, repo, **kwargs):
        seen.update({"data_dir": data_dir, "repo": repo, **kwargs})
        return {"state": "ok", "items": []}

    monkeypatch.setattr(abnormal, "get_auction_scan", _fake_scan)
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    app = FastAPI()
    app.include_router(abnormal.router)
    app.state.repo = repo

    with TestClient(app) as client:
        resp = client.get(
            "/api/abnormal/auction-scan?min_open_pct=3&min_ratio=8&limit=50&refresh=true",
        )

    assert resp.status_code == 200
    assert resp.json() == {"state": "ok", "items": []}
    assert seen == {
        "data_dir": tmp_path, "repo": repo, "min_open_pct": 3.0, "min_ratio": 8.0,
        "limit": 50, "refresh": True,
    }


def test_api_route_defaults_match_card_thresholds(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api import abnormal

    seen: dict = {}
    monkeypatch.setattr(
        abnormal, "get_auction_scan",
        lambda data_dir, repo, **kwargs: seen.update(kwargs) or {"state": "ok"},
    )
    app = FastAPI()
    app.include_router(abnormal.router)
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))

    with TestClient(app) as client:
        assert client.get("/api/abnormal/auction-scan").status_code == 200
        # 越界参数由 FastAPI 拦下 (limit 上限 1000)
        assert client.get("/api/abnormal/auction-scan?limit=5000").status_code == 422

    assert seen == {"min_open_pct": 5.0, "min_ratio": 10.0, "limit": 200, "refresh": False}
