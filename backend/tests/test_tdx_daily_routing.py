"""免费 TDX 日K/维表路由回归测试。"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from app.jobs import daily_pipeline
from app.services import instrument_sync, kline_sync
from app.tickflow.repository import DataStore, KlineRepository


def _daily_frame(symbol: str) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": [symbol],
        "date": [date(2026, 9, 4)],
        "open": [10.0],
        "high": [10.5],
        "low": [9.8],
        "close": [10.2],
        "volume": [100.0],
        "amount": [1_020.0],
    })


def _select_daily_provider(monkeypatch, provider, name: str = "tdx_mcp") -> None:
    monkeypatch.setattr(kline_sync.preferences, "get_daily_data_provider", lambda: name)
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda provider_name, dataset: provider_name == name and dataset == "daily",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda provider_name: provider)


def test_daily_provider_routes_index_without_creating_tickflow_client(monkeypatch):
    provider = MagicMock()
    provider.daily_asset_types = {"stock", "index", "etf"}
    provider.get_daily.return_value = _daily_frame("000001.SH")
    _select_daily_provider(monkeypatch, provider)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("不应创建 TickFlow client")),
    )

    df = kline_sync.sync_daily_batch(
        ["000001.SH"], count=20, asset_type="index",
    )

    assert df["symbol"].to_list() == ["000001.SH"]
    provider.get_daily.assert_called_once()
    _, kwargs = provider.get_daily.call_args
    assert kwargs["asset_type"] == "index"
    assert kwargs["start_time"] is not None
    assert kwargs["end_time"] is not None


def test_tdx_default_daily_window_uses_beijing_clock(monkeypatch):
    provider = MagicMock()
    provider.daily_asset_types = {"stock", "index", "etf"}
    provider.get_daily.return_value = _daily_frame("600000.SH")
    _select_daily_provider(monkeypatch, provider)
    now = datetime(2026, 9, 4, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(kline_sync, "cn_now", lambda: now)

    kline_sync.sync_daily_batch(["600000.SH"], count=20)

    _, kwargs = provider.get_daily.call_args
    assert kwargs["end_time"] == now
    assert kwargs["start_time"] == now - timedelta(days=20)


def test_tdx_daily_failure_reporting_is_opt_in_for_compatible_provider(monkeypatch):
    provider = MagicMock()
    provider.daily_asset_types = {"stock", "index", "etf"}
    provider.supports_daily_failure_reporting = True
    provider.get_daily.return_value = _daily_frame("600000.SH")
    _select_daily_provider(monkeypatch, provider)
    failed_symbols: list[str] = []

    kline_sync.sync_daily_batch(["600000.SH"], count=20, failed_out=failed_symbols)

    _, kwargs = provider.get_daily.call_args
    assert kwargs["failed_out"] is failed_symbols


def test_provider_without_index_declaration_keeps_existing_tickflow_fallback(monkeypatch):
    provider = MagicMock()
    provider.daily_asset_types = {"stock"}
    _select_daily_provider(monkeypatch, provider, name="fuyao")
    tickflow = MagicMock()
    tickflow.klines.batch.return_value = []
    monkeypatch.setattr(kline_sync, "get_client", lambda: tickflow)

    df = kline_sync.sync_daily_batch(["510300.SH"], count=20, asset_type="etf")

    assert df.is_empty()
    provider.get_daily.assert_not_called()
    tickflow.klines.batch.assert_called_once()


def test_unavailable_tdx_daily_provider_fails_closed_without_tickflow(monkeypatch):
    monkeypatch.setattr(kline_sync.preferences, "get_daily_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", lambda *_args: False)
    monkeypatch.setattr("app.data_providers.custom.plugin_requires_source_isolation", lambda _name: True)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 不可用时不应回退 TickFlow")),
    )

    with pytest.raises(RuntimeError, match="tdx_mcp"):
        kline_sync.sync_daily_batch(["600000.SH"], count=20)


def test_daily_api_maps_tdx_connection_failure_to_502(monkeypatch):
    from app.api import kline as kline_api

    repo = MagicMock()
    repo.resolve_asset_type.return_value = "stock"
    repo.get_daily_asset.return_value = pl.DataFrame()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(repo=repo)))
    monkeypatch.setattr(kline_api, "_get_stock_info", lambda _repo, _symbol: {})
    monkeypatch.setattr(
        kline_api.kline_sync,
        "sync_daily_batch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("tdx TCP down")),
    )

    with pytest.raises(kline_api.HTTPException) as exc_info:
        kline_api.get_daily(
            request,
            symbol="600000.SH",
            days=120,
            start_date=None,
            end_date=None,
            ext_columns=None,
        )

    assert exc_info.value.status_code == 502
    assert "数据源拉取失败" in str(exc_info.value.detail)


def test_tdx_instrument_provider_populates_stock_universe_without_tickflow(monkeypatch, tmp_path):
    provider = MagicMock()
    provider.instrument_asset_types = {"stock", "index", "etf"}
    provider.get_instruments.return_value = [{
        "symbol": "600000.SH",
        "name": "600000",
        "code": "600000",
        "exchange": "SH",
        "region": "CN",
        "type": "stock",
        "ext": {},
    }]
    monkeypatch.setattr(
        "app.services.preferences.get_daily_data_provider", lambda: "tdx_mcp",
    )
    monkeypatch.setattr("app.data_providers.custom.is_custom_provider", lambda name: name == "tdx_mcp")
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)
    monkeypatch.setattr(
        instrument_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("不应创建 TickFlow client")),
    )

    written = instrument_sync.sync_instruments(tmp_path)

    assert written == 1
    provider.get_instruments.assert_called_once_with("stock")
    stored = pl.read_parquet(tmp_path / "instruments" / "instruments.parquet")
    assert stored["symbol"].to_list() == ["600000.SH"]


def test_unavailable_tdx_instruments_do_not_fallback_to_tickflow(monkeypatch, tmp_path):
    monkeypatch.setattr("app.services.preferences.get_daily_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr("app.data_providers.custom.is_custom_provider", lambda _name: False)
    monkeypatch.setattr("app.data_providers.custom.plugin_requires_source_isolation", lambda _name: True)
    monkeypatch.setattr(
        instrument_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 不可用时不应请求 TickFlow 标的池")),
    )

    with pytest.raises(RuntimeError, match="tdx_mcp"):
        instrument_sync.sync_instruments(tmp_path)


def test_tdx_instrument_failure_stops_pipeline_before_resolving_fallback_universe(monkeypatch, tmp_path):
    repo = MagicMock()
    repo.store.data_dir = tmp_path
    monkeypatch.setattr(
        daily_pipeline.instrument_sync,
        "sync_instruments",
        lambda _data_dir: (_ for _ in ()).throw(RuntimeError("tdx_mcp 代码表不可用")),
    )
    monkeypatch.setattr(
        daily_pipeline,
        "_resolve_universe",
        lambda *_args: (_ for _ in ()).throw(AssertionError("不应回退解析默认标的池")),
    )

    with pytest.raises(RuntimeError, match="tdx_mcp"):
        daily_pipeline.run_now(repo, SimpleNamespace(has=lambda _cap: False))


def test_pipeline_marks_partial_daily_failure_as_failed(monkeypatch, tmp_path):
    repo = KlineRepository(DataStore(tmp_path))
    progress: list[tuple[str, str]] = []
    monkeypatch.setattr(daily_pipeline, "cn_today", lambda: date(2026, 9, 4))
    monkeypatch.setattr(daily_pipeline, "cn_now", lambda: datetime(2026, 9, 4, 15, 5))
    monkeypatch.setattr(daily_pipeline.instrument_sync, "sync_instruments", lambda _data_dir: 0)
    monkeypatch.setattr(daily_pipeline, "_resolve_universe", lambda *_args: ["600000.SH", "000001.SZ"])
    monkeypatch.setattr(daily_pipeline, "_invalidate", lambda *_args: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_single_view", lambda *_args: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_views", lambda *_args: None)
    monkeypatch.setattr(daily_pipeline, "run_pipeline", lambda **_kwargs: 0)
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_a_share", lambda: True)
    monkeypatch.setattr(daily_pipeline._prefs, "get_adj_factor_provider", lambda: "tickflow")
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_index", lambda: False)
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_etf", lambda: False)
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_regime_enabled", lambda: False)
    monkeypatch.setattr("app.services.preferences.get_minute_sync_enabled", lambda: False)

    def _partial_batch(*_args, failed_out=None, **_kwargs):
        assert failed_out is not None
        failed_out.append("000001.SZ")
        return 1

    monkeypatch.setattr(kline_sync, "sync_and_persist_daily_batch", _partial_batch)

    def _progress(stage: str, _pct: int, message: str, **_kwargs) -> None:
        progress.append((stage, message))

    with pytest.raises(daily_pipeline.PipelineStageError, match="daily sync") as exc_info:
        daily_pipeline.run_now(
            repo,
            SimpleNamespace(has=lambda _cap: False),
            on_progress=_progress,
            override_start_date=date(2026, 9, 1),
        )

    assert exc_info.value.errors == ["daily sync: 1 只标的拉取失败 (样例: 000001.SZ)"]
    assert ("sync_daily", "日K部分失败, 已写入 1 行, 1 只标的未更新") in progress


def test_tdx_index_instruments_do_not_request_tickflow_supplement(monkeypatch):
    from app.services import index_sync

    monkeypatch.setattr(index_sync.preferences, "get_daily_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        index_sync.instrument_sync,
        "fetch_instruments_via_provider",
        lambda asset_type: (
            True,
            [{
                "symbol": "000001.SH",
                "name": "000001",
                "code": "000001",
                "exchange": "SH",
                "region": "CN",
                "type": asset_type,
                "ext": {},
            }],
        ),
    )
    monkeypatch.setattr(
        index_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 不能请求 TickFlow 指数池")),
    )

    written = index_sync.sync_index_instruments(MagicMock(), pull_index=True, pull_etf=False)

    assert written == 1


def test_custom_daily_provider_uses_local_instruments_instead_of_tickflow_universe(monkeypatch, tmp_path):
    repo = KlineRepository(DataStore(tmp_path))
    instruments_dir = tmp_path / "instruments"
    instruments_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600000.SH"]}).write_parquet(instruments_dir / "instruments.parquet")
    monkeypatch.setattr(daily_pipeline.settings, "data_dir", tmp_path)
    monkeypatch.setattr(daily_pipeline._prefs, "get_daily_data_provider", lambda: "tdx_mcp")
    pool_calls: list[str] = []

    def _pool(name: str, refresh: bool = False):
        pool_calls.append(name)
        if name == "CN_Equity_A":
            raise AssertionError("TDX 不能请求 TickFlow 全市场 pool")
        return []

    monkeypatch.setattr(daily_pipeline, "get_pool", _pool)
    monkeypatch.setattr(daily_pipeline, "DEMO_SYMBOLS", [])
    monkeypatch.setattr(repo, "get_index_symbol_set", lambda: set())
    capset = SimpleNamespace(has=lambda cap: True)

    universe = daily_pipeline._resolve_universe(capset, repo)

    assert universe == ["600000.SH"]
    assert "CN_Equity_A" not in pool_calls


def test_index_sync_requests_tdx_with_index_asset_type(monkeypatch):
    from app.services import index_sync
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

    capset = CapabilitySet()
    capset.grant(Cap.KLINE_DAILY_BATCH, CapabilityLimits(batch=100, rpm=None))
    repo = MagicMock()
    monkeypatch.setattr(index_sync.preferences, "get_index_daily_batch_size", lambda: 100)
    calls: list[str] = []

    def _daily(symbols, **kwargs):
        calls.append(kwargs["asset_type"])
        return _daily_frame(symbols[0])

    monkeypatch.setattr(index_sync.kline_sync, "sync_daily_batch", _daily)

    written = index_sync.sync_and_persist_index_daily(
        repo,
        capset,
        symbols_override=["000001.SH"],
        start_date=datetime(2026, 9, 1),
        end_date=datetime(2026, 9, 4),
    )

    assert written == 1
    assert calls == ["index"]


def test_index_sync_collects_partial_daily_failures(monkeypatch):
    from app.services import index_sync
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

    capset = CapabilitySet()
    capset.grant(Cap.KLINE_DAILY_BATCH, CapabilityLimits(batch=100, rpm=None))
    repo = MagicMock()
    failed_symbols: list[str] = []
    monkeypatch.setattr(index_sync.preferences, "get_index_daily_batch_size", lambda: 100)
    monkeypatch.setattr(index_sync, "compute_enriched", lambda raw, **_kwargs: raw)

    def _daily(symbols, **kwargs):
        kwargs["failed_out"].append("000002.SH")
        return _daily_frame(symbols[0])

    monkeypatch.setattr(index_sync.kline_sync, "sync_daily_batch", _daily)

    written = index_sync.sync_and_persist_index_daily(
        repo,
        capset,
        symbols_override=["000001.SH", "000002.SH"],
        failed_out=failed_symbols,
    )

    assert written == 1
    assert failed_symbols == ["000002.SH"]


def test_etf_sync_collects_partial_daily_failures(monkeypatch):
    from app.services import index_sync
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

    capset = CapabilitySet()
    capset.grant(Cap.KLINE_DAILY_BATCH, CapabilityLimits(batch=100, rpm=None))
    repo = MagicMock()
    failed_symbols: list[str] = []
    monkeypatch.setattr(index_sync.preferences, "get_index_daily_batch_size", lambda: 100)
    monkeypatch.setattr(index_sync, "_load_etf_factors", lambda _repo: pl.DataFrame())
    monkeypatch.setattr(index_sync, "compute_enriched", lambda raw, **_kwargs: raw)

    def _daily(symbols, **kwargs):
        kwargs["failed_out"].append("510500.SH")
        return _daily_frame(symbols[0])

    monkeypatch.setattr(index_sync.kline_sync, "sync_daily_batch", _daily)

    written = index_sync.sync_and_persist_etf_daily(
        repo,
        capset,
        symbols_override=["510300.SH", "510500.SH"],
        failed_out=failed_symbols,
    )

    assert written == 1
    assert failed_symbols == ["510500.SH"]


def test_pipeline_marks_partial_index_and_etf_daily_failures_as_failed(monkeypatch, tmp_path):
    from app.tickflow.capabilities import Cap

    repo = KlineRepository(DataStore(tmp_path))
    progress: list[tuple[str, str]] = []
    monkeypatch.setattr(daily_pipeline, "cn_today", lambda: date(2026, 9, 4))
    monkeypatch.setattr(daily_pipeline, "cn_now", lambda: datetime(2026, 9, 4, 15, 5))
    monkeypatch.setattr(daily_pipeline.instrument_sync, "sync_instruments", lambda _data_dir: 0)
    monkeypatch.setattr(daily_pipeline, "_resolve_universe", lambda *_args: [])
    monkeypatch.setattr(daily_pipeline, "_invalidate", lambda *_args: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_single_view", lambda *_args: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_views", lambda *_args: None)
    monkeypatch.setattr(daily_pipeline, "run_pipeline", lambda **_kwargs: 0)
    monkeypatch.setattr(repo, "refresh_index_views", lambda: None)
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_a_share", lambda: True)
    monkeypatch.setattr(daily_pipeline._prefs, "get_adj_factor_provider", lambda: "tickflow")
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_index", lambda: True)
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_etf", lambda: True)
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_regime_enabled", lambda: False)
    monkeypatch.setattr("app.services.preferences.get_minute_sync_enabled", lambda: False)
    monkeypatch.setattr(daily_pipeline.kline_sync, "sync_and_persist_daily_batch", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(daily_pipeline.index_sync, "sync_index_instruments", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(daily_pipeline.index_sync, "sync_etf_instruments", lambda *_args, **_kwargs: 1)

    def _partial_index(*_args, failed_out=None, **_kwargs):
        assert failed_out is not None
        failed_out.append("000002.SH")
        return 2

    def _partial_etf(*_args, failed_out=None, **_kwargs):
        assert failed_out is not None
        failed_out.append("510500.SH")
        return 3

    monkeypatch.setattr(daily_pipeline.index_sync, "sync_and_persist_index_daily", _partial_index)
    monkeypatch.setattr(daily_pipeline.index_sync, "sync_and_persist_etf_daily", _partial_etf)

    def _progress(stage: str, _pct: int, message: str, **_kwargs) -> None:
        progress.append((stage, message))

    capset = SimpleNamespace(has=lambda cap: cap == Cap.KLINE_DAILY_BATCH)
    with pytest.raises(daily_pipeline.PipelineStageError, match="index daily sync") as exc_info:
        daily_pipeline.run_now(
            repo,
            capset,
            on_progress=_progress,
            override_start_date=date(2026, 9, 1),
        )

    assert exc_info.value.errors == [
        "index daily sync: 1 只标的拉取失败 (样例: 000002.SH)",
        "ETF daily sync: 1 只标的拉取失败 (样例: 510500.SH)",
    ]
    assert ("sync_index", "指数日K部分失败, 已写入 2 行, 1 只标的未更新") in progress
    assert ("sync_index", "ETF 日K部分失败, 已写入 3 行, 1 只标的未更新") in progress


def test_index_sync_default_window_uses_beijing_clock(monkeypatch):
    from app.services import index_sync
    from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

    capset = CapabilitySet()
    capset.grant(Cap.KLINE_DAILY_BATCH, CapabilityLimits(batch=100, rpm=None))
    now = datetime(2026, 9, 4, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(index_sync, "cn_now", lambda: now)
    monkeypatch.setattr(index_sync.preferences, "get_index_daily_batch_size", lambda: 100)
    calls: list[dict] = []
    monkeypatch.setattr(
        index_sync.kline_sync,
        "sync_daily_batch",
        lambda _symbols, **kwargs: calls.append(kwargs) or pl.DataFrame(),
    )

    index_sync.sync_and_persist_index_daily(
        MagicMock(),
        capset,
        symbols_override=["000001.SH"],
    )

    assert calls[0]["end_time"] == now
    assert calls[0]["start_time"] == now - timedelta(days=365)


def test_index_sync_api_uses_beijing_clock(monkeypatch):
    from app.api import indices

    now = datetime(2026, 9, 4, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    captured: dict = {}
    monkeypatch.setattr(indices, "cn_now", lambda: now)
    monkeypatch.setattr(indices.index_sync, "sync_index_instruments", lambda _repo: 1)
    monkeypatch.setattr(
        indices.index_sync,
        "sync_and_persist_index_daily",
        lambda _repo, _capset, **kwargs: captured.update(kwargs) or 2,
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(repo=object(), capabilities=SimpleNamespace(has=lambda _cap: True)),
        ),
    )

    result = indices.sync_index_daily(request, days=30)

    assert result == {"status": "ok", "index_count": 1, "rows_written": 2}
    assert captured["end_date"] == now
    assert captured["start_date"] == now - timedelta(days=30)


def test_index_read_apis_default_to_beijing_date(monkeypatch):
    from app.api import indices

    today = date(2026, 9, 4)
    daily_window: dict = {}
    minute_call: dict = {}
    repo = MagicMock()
    repo.get_index_instruments.return_value = pl.DataFrame()
    repo.get_index_daily.side_effect = (
        lambda _symbol, start, end: daily_window.update(start=start, end=end) or pl.DataFrame()
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                repo=repo,
                capabilities=SimpleNamespace(has=lambda _cap: False),
            ),
        ),
    )
    monkeypatch.setattr(indices, "cn_today", lambda: today)
    monkeypatch.setattr(
        indices.kline_sync,
        "fetch_minute_single",
            lambda symbol, trade_date, *, asset_type, capset: (
            minute_call.update(symbol=symbol, trade_date=trade_date, asset_type=asset_type)
            or pl.DataFrame()
        ),
    )

    indices.get_index_daily(
        request,
        symbol="000001.SH",
        days=20,
        start_date=None,
        end_date=None,
    )
    indices.get_index_minute(request, symbol="000001.SH", trade_date=None)

    assert daily_window == {"start": today - timedelta(days=20), "end": today}
    assert minute_call == {
        "symbol": "000001.SH",
        "trade_date": today,
        "asset_type": "index",
    }
