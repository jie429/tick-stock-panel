from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.custom.dragon_quant import router, service
from app.custom.dragon_quant.account import DragonAccountConfig, run_account_backtest
from app.custom.dragon_quant.data import (
    DragonDataError,
    DragonScanOptions,
    _load_depth_snapshot,
    _load_minute_on_demand,
    _prepare_daily,
    _ranked_industry_samples,
    build_scan_input,
    load_account_market_data,
)
from app.custom.dragon_quant.models import DragonCandidate, DragonScanInput
from app.custom.dragon_quant.scoring import score_scan
from app.custom.dragon_quant.storage import DragonRecordStore
from app.extensions.loader import configure_backend_extensions


def _curve(*values: float) -> dict[datetime, float]:
    start = datetime(2026, 9, 8, 9, 30)
    return {start + timedelta(minutes=i): value for i, value in enumerate(values)}


def test_score_scan_ranks_only_candidates_passing_hard_floors() -> None:
    strong = DragonCandidate(
        symbol="600001.SH",
        name="强龙",
        industry="电子",
        industries=("电子",),
        board_count=3,
        five_day_return_pct=30.0,
        change_pct=0.10,
        turnover_rate_pct=18.0,
        volume_lots=100_000.0,
        amount_yuan=800_000_000.0,
        sealed_volume_lots=None,
        prev_raw_close=10.0,
        limit_up_price=11.0,
        minute_curve=_curve(0.00, 0.01, 0.04, 0.08, 0.10, 0.10),
        open_count=0,
    )
    weak = DragonCandidate(
        symbol="600002.SH",
        name="跟风",
        industry="电子",
        industries=("电子",),
        board_count=1,
        five_day_return_pct=8.0,
        change_pct=0.10,
        turnover_rate_pct=4.0,
        volume_lots=100_000.0,
        amount_yuan=300_000_000.0,
        sealed_volume_lots=0.0,
        prev_raw_close=10.0,
        limit_up_price=11.0,
        minute_curve=_curve(0.00, 0.00, 0.00, 0.01, 0.05, 0.10),
        open_count=4,
    )
    scan_input = DragonScanInput(
        as_of=date(2026, 9, 8),
        candidates=(strong, weak),
        industry_members={"电子": ("600001.SH", "600002.SH", "600003.SH")},
        industry_change_pct={"电子": 0.06},
        stock_change_pct={
            "600001.SH": 0.10,
            "600002.SH": 0.10,
            "600003.SH": 0.04,
        },
        industry_curves={"电子": _curve(0.00, 0.00, 0.01, 0.02, 0.04, 0.06)},
        market_curve=_curve(0.00, -0.002, -0.008, -0.004, 0.00, 0.01),
        industry_history={},
        data_quality={"complete": True, "missing": [], "adaptations": []},
    )

    result = score_scan(scan_input)

    assert result[0]["symbol"] == "600001.SH"
    assert result[0]["is_true_dragon"] is True
    assert result[0]["rank"] == 1
    assert result[0]["dimensions"]["liquidity"]["details"]["degraded"] is True
    assert result[1]["is_true_dragon"] is False
    assert result[1]["reject_reason"]


def test_account_backtest_enforces_t1_and_sells_half_on_next_day_limit_up() -> None:
    days = [date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 9)]
    daily = {
        "600001.SH": [
            {
                "date": days[0], "open": 10.0, "high": 10.5, "low": 9.8,
                "close": 10.0, "raw_close": 10.0, "ma5": 10.0,
                "amount": 500_000_000.0, "turnover_rate": 12.0,
                "volume": 100_000.0, "signal_limit_up": False,
            },
            {
                "date": days[1], "open": 10.1, "high": 11.11, "low": 10.0,
                "close": 11.11, "raw_close": 11.11, "ma5": 10.0,
                "amount": 600_000_000.0, "turnover_rate": 12.0,
                "volume": 100_000.0, "signal_limit_up": True,
            },
            {
                "date": days[2], "open": 11.2, "high": 12.22, "low": 11.0,
                "close": 12.22, "raw_close": 12.22, "ma5": 10.5,
                "amount": 600_000_000.0, "turnover_rate": 12.0,
                "volume": 100_000.0, "signal_limit_up": True,
            },
        ]
    }
    scans = {
        days[0]: [{
            "symbol": "600001.SH", "name": "强龙", "rank": 1,
            "composite_score": 80.0, "is_true_dragon": True,
        }]
    }

    result = run_account_backtest(
        trading_days=days,
        daily_by_symbol=daily,
        scans_by_date=scans,
        minute_by_symbol_date={},
        config=DragonAccountConfig(initial_cash=100_000.0, max_positions=1),
    )

    assert [trade["side"] for trade in result["trades"]] == ["buy", "sell"]
    assert result["trades"][0]["trade_date"] == "2026-09-08"
    assert result["trades"][1]["trade_date"] == "2026-09-09"
    assert result["trades"][1]["reason_code"] == "next_day_limit_up_half"
    assert result["trades"][1]["quantity"] < result["trades"][0]["quantity"]


def test_open_buy_does_not_use_same_day_close_liquidity() -> None:
    days = [date(2026, 9, 7), date(2026, 9, 8)]
    daily = {
        "600001.SH": [
            {
                "date": days[0], "open": 10.0, "high": 10.2, "low": 9.9,
                "close": 10.0, "raw_close": 10.0, "ma5": 10.0,
                "amount": 50_000_000.0, "turnover_rate": 2.0,
                "volume": 100_000.0, "signal_limit_up": False,
            },
            {
                "date": days[1], "open": 10.1, "high": 10.8, "low": 10.0,
                "close": 10.7, "raw_close": 10.7, "ma5": 10.0,
                "amount": 1_000_000_000.0, "turnover_rate": 20.0,
                "volume": 200_000.0, "signal_limit_up": False,
            },
        ]
    }
    scans = {days[0]: [{
        "symbol": "600001.SH", "name": "候选", "rank": 1,
        "composite_score": 80.0, "is_true_dragon": True,
        "amount_yuan": 50_000_000.0, "turnover_rate_pct": 2.0,
    }]}

    result = run_account_backtest(
        trading_days=days,
        daily_by_symbol=daily,
        scans_by_date=scans,
        minute_by_symbol_date={},
        config=DragonAccountConfig(initial_cash=100_000.0, max_positions=1),
    )

    assert result["trades"] == []


def test_scan_service_fails_closed_when_critical_market_inputs_are_incomplete(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scan_input = DragonScanInput(
        as_of=date(2026, 9, 8),
        candidates=(),
        industry_members={},
        industry_change_pct={},
        stock_change_pct={},
        industry_curves={},
        market_curve={},
        data_quality={"complete": False, "missing": ["candidate_minute"]},
    )
    monkeypatch.setattr(service, "build_scan_input", lambda *_args, **_kwargs: scan_input)
    monkeypatch.setattr(service, "score_scan", lambda _input: [{
        "symbol": "600001.SH",
        "name": "强龙",
        "is_true_dragon": True,
        "reject_reason": None,
        "composite_score": 88.0,
    }])

    record = service.run_scan(
        object(), None, tmp_path,
        as_of=date(2026, 9, 8),
        options=DragonScanOptions(),
    )

    assert record["rows"][0]["score_passed"] is True
    assert record["rows"][0]["is_true_dragon"] is False
    assert record["rows"][0]["rank"] is None
    assert "candidate_minute" in record["rows"][0]["reject_reason"]


@pytest.mark.parametrize("missing", ["absorption_minute_history", "depth5_sealed_snapshot"])
def test_scan_service_optional_gap_does_not_block_certification(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    missing: str,
) -> None:
    scan_input = DragonScanInput(
        as_of=date(2026, 9, 8),
        candidates=(),
        industry_members={},
        industry_change_pct={},
        stock_change_pct={},
        industry_curves={},
        market_curve={},
        data_quality={
            "complete": True,
            "missing": [missing],
            "critical_missing": [],
            "optional_missing": [missing],
        },
    )
    monkeypatch.setattr(service, "build_scan_input", lambda *_args, **_kwargs: scan_input)
    monkeypatch.setattr(service, "score_scan", lambda _input: [{
        "symbol": "600001.SH",
        "name": "强龙",
        "is_true_dragon": True,
        "reject_reason": None,
        "composite_score": 88.0,
        "rank": 1,
    }])

    record = service.run_scan(
        object(), None, tmp_path,
        as_of=date(2026, 9, 8),
        options=DragonScanOptions(),
    )

    assert record["rows"][0]["score_passed"] is True
    assert record["rows"][0]["is_true_dragon"] is True
    assert record["rows"][0]["rank"] == 1


def test_depth_snapshot_fetches_latest_enriched_day_and_reloads() -> None:
    day = date(2026, 9, 10)
    state = {"ready": False, "runs": 0}

    class DepthStub:
        def get_sealed_map(self, as_of, *, is_down):
            assert as_of == day
            assert is_down is False
            return {"600001.SH": {"ready": True, "vol": 123.0}} if state["ready"] else {}

        def is_sealed_ready(self, as_of):
            assert as_of == day
            return state["ready"]

        def run_once(self):
            state["runs"] += 1
            state["ready"] = True
            return {"ok": True, "count": 1, "msg": "已修正 1 只"}

    repo = SimpleNamespace(get_enriched_latest=lambda: (pl.DataFrame(), day))

    sealed, ready, attempted, result = _load_depth_snapshot(repo, DepthStub(), day)

    assert state["runs"] == 1
    assert ready is True
    assert attempted is True
    assert sealed["600001.SH"]["vol"] == 123.0
    assert result == {"ok": True, "count": 1, "msg": "已修正 1 只"}


def test_depth_snapshot_does_not_fetch_for_historical_day() -> None:
    historical = date(2026, 9, 9)
    latest = date(2026, 9, 10)

    class DepthStub:
        def get_sealed_map(self, _as_of, *, is_down):
            assert is_down is False
            return {}

        def is_sealed_ready(self, _as_of):
            return False

        def run_once(self):
            pytest.fail("历史交易日不得使用当前盘口补拉")

    repo = SimpleNamespace(get_enriched_latest=lambda: (pl.DataFrame(), latest))

    sealed, ready, attempted, result = _load_depth_snapshot(repo, DepthStub(), historical)

    assert sealed == {}
    assert ready is False
    assert attempted is False
    assert result is None


def test_industry_minute_samples_are_limited_but_keep_all_candidates() -> None:
    industry = "电子"
    members = {industry: [f"60000{index}.SH" for index in range(6)]}
    current_rows = {
        symbol: {"amount": float(index)}
        for index, symbol in enumerate(members[industry])
    }
    candidates = {industry: {members[industry][0], members[industry][1], members[industry][2]}}

    samples = _ranked_industry_samples(
        {industry}, members, current_rows, candidates, limit=2,
    )

    assert set(samples[industry][:3]) == candidates[industry]
    assert len(samples[industry]) == 3


def test_minute_on_demand_uses_local_data_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = date(2026, 9, 8)
    local = pl.DataFrame({
        "symbol": ["600001.SH"],
        "datetime": [datetime(2026, 9, 8, 9, 31)],
        "open": [10.0],
        "high": [10.1],
        "low": [9.9],
        "close": [10.0],
        "volume": [100.0],
        "amount": [1_000.0],
    })
    repo = SimpleNamespace(get_minute_by_dates=lambda *_args, **_kwargs: local)
    monkeypatch.setattr(
        "app.custom.dragon_quant.data.kline_sync.sync_minute_batch",
        lambda *_args, **_kwargs: pytest.fail("本地数据完整时不应请求分钟 Provider"),
    )

    frame, fetched, errors = _load_minute_on_demand(
        repo, ["600001.SH"], day, asset_type="stock", auto_fetch=True,
    )

    assert frame.height == 1
    assert fetched == []
    assert errors == []


def test_industry_mapping_missing_is_an_explicit_data_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    frame = pl.DataFrame({
        "symbol": ["600001.SH"],
        "date": [date(2026, 9, 8)],
        "open": [10.0],
        "high": [11.0],
        "low": [10.0],
        "close": [11.0],
        "volume": [100_000.0],
        "amount": [500_000_000.0],
    })
    monkeypatch.setattr(
        "app.custom.dragon_quant.data._prepare_daily",
        lambda *_args, **_kwargs: frame,
    )

    with pytest.raises(DragonDataError, match="行业映射不可用"):
        build_scan_input(object(), None, tmp_path, date(2026, 9, 8), DragonScanOptions())


def test_prepare_daily_falls_back_to_partition_batch_while_cache_is_warming() -> None:
    days = [date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10)]
    history = pl.DataFrame({
        "symbol": ["600001.SH", "600001.SH"],
        "date": days[:2],
        "open": [9.8, 10.0],
        "high": [10.1, 10.5],
        "low": [9.7, 9.9],
        "close": [10.0, 10.4],
        "raw_close": [10.0, 10.4],
        "volume": [100.0, 110.0],
        "amount": [1_000.0, 1_100.0],
    })
    latest = pl.DataFrame({
        "symbol": ["600001.SH"],
        "date": [days[2]],
        "open": [10.5],
        "high": [11.44],
        "low": [10.4],
        "close": [11.44],
        "raw_close": [11.44],
        "volume": [120.0],
        "amount": [1_200.0],
    })
    repo = SimpleNamespace(
        get_enriched_range=lambda *_args: None,
        get_name_map=lambda *_args: {"600001.SH": "样本"},
        get_daily_batch=lambda *_args: history,
        get_enriched_latest=lambda: (latest, days[2]),
    )

    frame = _prepare_daily(repo, days[2])

    assert frame["date"].unique().sort().to_list() == days
    current = frame.filter(pl.col("date") == days[2]).row(0, named=True)
    assert current["prev_raw_close"] == 10.4
    assert current["change_pct"] == pytest.approx(0.1)


def test_prepare_daily_loads_historical_date_when_latest_is_newer() -> None:
    target = date(2026, 9, 9)
    history = pl.DataFrame({
        "symbol": ["600001.SH", "600001.SH"],
        "date": [date(2026, 9, 8), target],
        "open": [9.8, 10.0],
        "high": [10.1, 11.0],
        "low": [9.7, 9.9],
        "close": [10.0, 11.0],
        "raw_close": [10.0, 11.0],
        "volume": [100.0, 110.0],
        "amount": [1_000.0, 1_100.0],
    })
    newer = pl.DataFrame({
        "symbol": ["600001.SH"],
        "date": [date(2026, 9, 10)],
        "open": [11.1],
        "high": [11.2],
        "low": [10.8],
        "close": [11.0],
        "volume": [120.0],
        "amount": [1_200.0],
    })
    repo = SimpleNamespace(
        get_enriched_range=lambda *_args: None,
        get_name_map=lambda *_args: {"600001.SH": "样本"},
        get_daily_batch=lambda *_args: history,
        get_enriched_latest=lambda: (newer, date(2026, 9, 10)),
    )

    frame = _prepare_daily(repo, target)

    assert frame["date"].max() == target
    assert frame.filter(pl.col("date") == target)["prev_raw_close"][0] == 10.0


def test_backtest_reports_missing_historical_scans(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    days = [date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 9)]
    repo = SimpleNamespace(
        get_enriched_range=lambda *_args: pl.DataFrame({"date": days}),
    )
    monkeypatch.setattr(
        service,
        "load_account_market_data",
        lambda *_args, **_kwargs: (days[1:], {}, {}),
    )

    record = service.run_backtest(
        repo,
        None,
        tmp_path,
        start=days[1],
        end=days[2],
        config=DragonAccountConfig(candidate_lookback_days=1),
    )

    assert record["warnings"]
    assert "缺少 2 个交易日" in record["warnings"][0]
    assert record["stats"]["trade_count"] == 0


def test_account_minute_data_is_aggregated_to_closed_five_minute_bars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade_date = date(2026, 9, 8)
    daily = pl.DataFrame({
        "symbol": ["600001.SH", "600001.SH"],
        "date": [date(2026, 9, 7), trade_date],
        "open": [10.0, 10.1],
        "high": [10.2, 10.5],
        "low": [9.9, 10.0],
        "close": [10.0, 10.4],
        "raw_close": [10.0, 10.4],
        "prev_raw_close": [9.8, 10.0],
        "volume": [100.0, 200.0],
        "amount": [1_000.0, 2_000.0],
        "name": ["样本", "样本"],
    })
    minute = pl.DataFrame({
        "symbol": ["600001.SH"] * 5,
        "datetime": [datetime(2026, 9, 8, 9, minute) for minute in range(31, 36)],
        "open": [10.1, 10.2, 10.3, 10.4, 10.5],
        "high": [10.2, 10.3, 10.4, 10.5, 10.6],
        "low": [10.0, 10.1, 10.2, 10.3, 10.4],
        "close": [10.2, 10.3, 10.4, 10.5, 10.6],
        "volume": [1.0] * 5,
        "amount": [100.0] * 5,
    })
    monkeypatch.setattr(
        "app.custom.dragon_quant.data._prepare_daily",
        lambda *_args, **_kwargs: daily,
    )
    repo = SimpleNamespace(get_minute_by_dates=lambda *_args, **_kwargs: minute)

    _days, _daily, bars = load_account_market_data(
        repo, ["600001.SH"], trade_date, trade_date,
    )

    first = bars[("600001.SH", trade_date)][0]
    assert first["datetime"] == datetime(2026, 9, 8, 9, 35)
    assert first["open"] == 10.1
    assert first["close"] == 10.6
    assert first["volume"] == 5.0


def test_record_store_and_router_support_list_detail_delete(tmp_path) -> None:
    store = DragonRecordStore(tmp_path, "scans.json", "scan")
    saved = store.save({"as_of": "2026-09-08", "rows": []})
    app = FastAPI()
    app.include_router(router)
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    app.state.depth_service = None
    client = TestClient(app)

    assert client.get("/api/custom/dragon-quant/scans").status_code == 200
    assert client.get(f"/api/custom/dragon-quant/scans/{saved['id']}").json()["as_of"] == "2026-09-08"
    assert client.delete(f"/api/custom/dragon-quant/scans/{saved['id']}").json() == {"ok": True}
    assert client.get(f"/api/custom/dragon-quant/scans/{saved['id']}").status_code == 404


def test_scan_api_maps_data_errors_to_422(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    app.state.depth_service = None
    monkeypatch.setattr(
        service,
        "run_scan",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(DragonDataError("分钟数据缺失")),
    )

    response = TestClient(app).post(
        "/api/custom/dragon-quant/scans",
        json={"as_of": "2026-09-08"},
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "分钟数据缺失"


def test_extension_loader_registers_dragon_quant_without_core_changes() -> None:
    app = FastAPI()

    registry, errors = configure_backend_extensions(app)

    assert "dragon.quant" in registry.extension_ids()
    assert not errors
    assert any(getattr(route, "path", "") == "/api/custom/dragon-quant/status" for route in app.routes)
