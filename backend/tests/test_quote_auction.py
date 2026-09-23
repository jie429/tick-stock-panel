"""个股集合竞价读数 (GET /api/quote/auction) 契约测试。

覆盖:
- `auction_scan.get_symbol_auction` 当日/历史日两条路径的状态机
  (ok / not_ready / source_unavailable / no_data) 与竞价量比基线口径;
- 无基线 (首日) 时量比/额比为 None, 归档内无该标的时 item=None —— 都不伪造 0;
- `GET /api/quote/auction` 的参数校验 (标的代码 / 日期) 与响应形状。

日期用 2026-08-26/27/28/31 (写作时为过去交易日), 与仓库既有绝对日期测试风格一致。
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import quote as quote_api
from app.services import auction_scan as svc
from app.services import trading_day


def _beijing(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute)


def _record(symbol, *, open_=10.0, pre=9.0, amount=1.0e7):
    """Provider ``get_market_auction_snapshot`` 的行 (单位: 元 / 手)。"""
    return {
        "symbol": symbol,
        "open_price": open_,
        "prev_close": pre,
        "open_pct": (open_ - pre) / pre if pre else None,
        "change_pct": 0.0,
        "last_price": open_,
        "auction_amount": amount,
        "auction_volume": amount / open_ / 100.0,
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


def _seed_snapshot(data_dir: Path, day: str, rows: list[dict]) -> None:
    svc._write_snapshot(svc._snapshot_path(data_dir, date.fromisoformat(day)), rows)


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
    _write_kline(tmp_path, "2026-08-28", [("600519.SH", 8.0e8)])
    return tmp_path


@pytest.fixture(autouse=True)
def _clear_cache():
    svc.reset_cache()
    yield
    svc.reset_cache()


def _set_trading_day(monkeypatch, verdict) -> None:
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: verdict)


def _use_provider(monkeypatch, provider) -> None:
    monkeypatch.setattr(svc, "_resolve_provider", lambda: provider)


# =====================================================================
# 当日 (实时行情源 + 进程内缓存)
# =====================================================================


def test_today_ok_composes_ratios_and_share(data_dir, monkeypatch):
    """当日扫描: 量比/额比以上一可得快照日为基线, 额占比以昨日全天成交额兜底。"""
    _set_trading_day(monkeypatch, True)
    _seed_snapshot(data_dir, "2026-08-28", [_record("600519.SH", amount=2.0e6)])
    provider = _FakeProvider([_record("600519.SH", amount=1.0e7)])
    _use_provider(monkeypatch, provider)
    names = SimpleNamespace(get_name_map=lambda symbols: {"600519.SH": "贵州茅台"})

    payload = svc.get_symbol_auction(
        data_dir, "600519.SH", repo=names, now=_beijing(2026, 8, 31, 10, 0),
    )

    assert payload["state"] == "ok"
    assert payload["trade_date"] == "2026-08-31"
    assert payload["baseline_date"] == "2026-08-28"
    assert payload["ratio_ready"] is True
    assert provider.calls == 1
    # 当日扫描按既有约定落盘 (与 /auction-scan 共用同一份归档)
    assert svc.snapshot_days(data_dir) == [date(2026, 8, 28), date(2026, 8, 31)]

    item = payload["item"]
    assert item["name"] == "贵州茅台"
    assert item["symbol"] == "600519.SH"
    assert item["open_price"] == 10.0
    assert item["auction_volume"] == pytest.approx(1.0e7 / 10.0 / 100.0)
    # 竞价量比 = 今日竞价量 ÷ 上一可得快照日竞价量 (同为 09:25 口径) = 5 倍
    assert item["ratio_volume"] == pytest.approx(5.0)
    assert item["ratio_amount"] == pytest.approx(5.0)
    # 竞价额占比 = 竞价额 ÷ 昨日全天成交额 (本地日K兜底口径)
    assert item["prev_amount_share"] == pytest.approx(1.0e7 / 8.0e8)


def test_no_baseline_keeps_ratio_columns_null(data_dir, monkeypatch):
    """首日无任何历史快照: 量比/额比是 None (前端显 —), 不伪造成 0。"""
    _set_trading_day(monkeypatch, True)
    _use_provider(monkeypatch, _FakeProvider([_record("600519.SH")]))

    payload = svc.get_symbol_auction(data_dir, "600519.SH", now=_beijing(2026, 8, 31, 10, 0))

    assert payload["state"] == "ok"
    assert payload["baseline_date"] is None and payload["ratio_ready"] is False
    assert payload["item"]["ratio_volume"] is None
    assert payload["item"]["ratio_amount"] is None


def test_symbol_absent_from_snapshot_stays_ok_without_item(data_dir, monkeypatch):
    """快照采到了但这只标的不在其中 (停牌/未参与竞价) → item=None, 不用 0 凑数。"""
    _set_trading_day(monkeypatch, True)
    _use_provider(monkeypatch, _FakeProvider([_record("000858.SZ")]))

    payload = svc.get_symbol_auction(data_dir, "600519.SH", now=_beijing(2026, 8, 31, 10, 0))

    assert payload["state"] == "ok"
    assert payload["item"] is None
    assert "无竞价成交" in payload["message"]


def test_not_ready_before_auction_without_archive(data_dir, monkeypatch):
    """交易日 09:25 前且当日无归档 → not_ready, 不触发扫描。"""
    _set_trading_day(monkeypatch, True)
    provider = _FakeProvider([_record("600519.SH")])
    _use_provider(monkeypatch, provider)

    payload = svc.get_symbol_auction(data_dir, "600519.SH", now=_beijing(2026, 8, 31, 9, 10))

    assert payload["state"] == "not_ready"
    assert provider.calls == 0
    assert "09:25" in payload["message"]


def test_before_auction_serves_today_archive(data_dir, monkeypatch):
    """竞价未结束但当日已有归档 (此前扫描过): 按归档回答, 不再打上游。"""
    _set_trading_day(monkeypatch, True)
    _seed_snapshot(data_dir, "2026-08-31", [_record("600519.SH", amount=3.0e6)])
    provider = _FakeProvider([_record("600519.SH")])
    _use_provider(monkeypatch, provider)

    payload = svc.get_symbol_auction(data_dir, "600519.SH", now=_beijing(2026, 8, 31, 9, 10))

    assert payload["state"] == "ok"
    assert provider.calls == 0
    assert payload["item"]["auction_amount"] == 3.0e6


def test_source_unavailable_without_archive(data_dir, monkeypatch):
    """实时行情源未实现可选协议 → source_unavailable, 不换源也不编造。"""
    _set_trading_day(monkeypatch, True)
    _use_provider(monkeypatch, None)

    payload = svc.get_symbol_auction(data_dir, "600519.SH", now=_beijing(2026, 8, 31, 10, 0))

    assert payload["state"] == "source_unavailable"
    assert payload["item"] is None
    assert "get_market_auction_snapshot" in payload["message"]


def test_scan_soft_failure_falls_back_to_archive(data_dir, monkeypatch):
    """源在但本次拿不到 (软失败): 当日归档兜底, 不把已有读数变成错误态。"""
    _set_trading_day(monkeypatch, True)
    _seed_snapshot(data_dir, "2026-08-31", [_record("600519.SH", amount=4.0e6)])
    _use_provider(monkeypatch, _FakeProvider(None))

    payload = svc.get_symbol_auction(data_dir, "600519.SH", now=_beijing(2026, 8, 31, 10, 0))

    assert payload["state"] == "ok"
    assert payload["item"]["auction_amount"] == 4.0e6


def test_scan_failure_without_archive_is_no_data(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)
    _use_provider(monkeypatch, _FakeProvider(None))

    payload = svc.get_symbol_auction(data_dir, "600519.SH", now=_beijing(2026, 8, 31, 10, 0))

    assert payload["state"] == "no_data"
    assert payload["item"] is None


# =====================================================================
# 历史日 (只读归档) 与日期边界
# =====================================================================


def test_history_date_reads_archive_without_touching_provider(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)
    _seed_snapshot(data_dir, "2026-08-27", [_record("600519.SH", amount=1.0e6)])
    _seed_snapshot(data_dir, "2026-08-28", [_record("600519.SH", amount=2.0e6)])
    _use_provider(monkeypatch, None)

    payload = svc.get_symbol_auction(
        data_dir, "600519.SH", date="2026-08-28", now=_beijing(2026, 8, 31, 10, 0),
    )

    assert payload["state"] == "ok"
    assert payload["trade_date"] == "2026-08-28"
    assert payload["baseline_date"] == "2026-08-27"
    assert payload["item"]["ratio_volume"] == pytest.approx(2.0)


def test_history_date_without_archive_is_no_data(data_dir, monkeypatch):
    _set_trading_day(monkeypatch, True)

    payload = svc.get_symbol_auction(
        data_dir, "600519.SH", date="2026-08-27", now=_beijing(2026, 8, 31, 10, 0),
    )

    assert payload["state"] == "no_data"
    assert payload["trade_date"] == "2026-08-27"
    assert payload["item"] is None
    assert "无竞价快照" in payload["message"]


def test_holiday_future_and_invalid_dates(data_dir, monkeypatch):
    """休市 / 未来日 / 非法日期: 各自的 no_data 原因, 不混用 not_ready。"""
    _set_trading_day(monkeypatch, False)
    holiday = svc.get_symbol_auction(data_dir, "600519.SH", now=_beijing(2026, 8, 30, 10, 0))
    assert holiday["state"] == "no_data" and "休市" in holiday["message"]

    _set_trading_day(monkeypatch, True)
    future = svc.get_symbol_auction(
        data_dir, "600519.SH", date="2026-09-01", now=_beijing(2026, 8, 31, 10, 0),
    )
    assert future["state"] == "no_data" and "尚未到来" in future["message"]

    invalid = svc.get_symbol_auction(
        data_dir, "600519.SH", date="2026/08/31", now=_beijing(2026, 8, 31, 10, 0),
    )
    assert invalid["state"] == "no_data" and "日期非法" in invalid["message"]


# =====================================================================
# API: 参数校验与响应形状
# =====================================================================


def _client(data_dir: Path) -> TestClient:
    app = FastAPI()
    app.include_router(quote_api.router)
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=data_dir))
    return TestClient(app)


def test_api_rejects_bad_symbol_and_date(data_dir):
    client = _client(data_dir)
    assert client.get("/api/quote/auction?symbol=600519").status_code == 400
    assert client.get("/api/quote/auction?symbol=600519.SH&date=2026/08/31").status_code == 400


def test_api_normalizes_symbol_case_and_date(data_dir, monkeypatch):
    seen: dict = {}

    def _fake(data_dir_arg, symbol, *, date=None, repo=None):
        seen.update({"data_dir": data_dir_arg, "symbol": symbol, "date": date, "repo": repo})
        return {"state": "ok", "symbol": symbol, "item": {"symbol": symbol}}

    monkeypatch.setattr(quote_api, "get_symbol_auction", _fake)
    client = _client(data_dir)

    body = client.get("/api/quote/auction?symbol=600519.sh&date=2026-08-28").json()

    assert body["item"]["symbol"] == "600519.SH"
    assert seen["symbol"] == "600519.SH"
    assert seen["date"] == "2026-08-28"
    assert seen["data_dir"] == data_dir and seen["repo"] is not None


def test_api_omitted_date_means_today(data_dir, monkeypatch):
    seen: dict = {}

    def _fake(data_dir_arg, symbol, *, date=None, repo=None):
        seen["date"] = date
        return {"state": "not_ready", "symbol": symbol, "item": None}

    monkeypatch.setattr(quote_api, "get_symbol_auction", _fake)

    body = _client(data_dir).get("/api/quote/auction?symbol=600519.SH").json()

    assert seen["date"] is None
    assert body["state"] == "not_ready" and body["item"] is None


def test_api_reads_archived_day_end_to_end(data_dir):
    """历史日走真实链路 (归档基线 → 量比), 不依赖网络与进程状态。"""
    _seed_snapshot(data_dir, "2026-08-27", [_record("600519.SH", amount=1.0e6)])
    _seed_snapshot(data_dir, "2026-08-28", [_record("600519.SH", amount=2.0e6)])

    body = _client(data_dir).get("/api/quote/auction?symbol=600519.SH&date=2026-08-28").json()

    assert body["state"] == "ok"
    assert body["trade_date"] == "2026-08-28"
    assert body["baseline_date"] == "2026-08-27"
    assert body["item"]["ratio_volume"] == pytest.approx(2.0)
    assert body["item"]["open_price"] == 10.0

