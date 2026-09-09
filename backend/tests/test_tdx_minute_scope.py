"""TDX 分组分钟K与全市场分钟同步的边界测试。"""
from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest
from fastapi import HTTPException

from app.api import kline as kline_api
from app.services import kline_sync
from app.tickflow.capabilities import Cap, CapabilitySet


def _minute_frame(symbol: str = "600000.SH") -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": [symbol],
        "datetime": [datetime(2026, 9, 4, 9, 31)],
        "open": [10.0],
        "high": [10.2],
        "low": [9.9],
        "close": [10.1],
        "volume": [100.0],
        "amount": [1_010.0],
    })


def test_tdx_minute_provider_is_not_treated_as_full_market_sync(monkeypatch):
    provider = SimpleNamespace(supports_minute_universe_sync=False)
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "minute",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)

    assert kline_sync.minute_universe_sync_supported() is False


def test_tickflow_still_supports_full_market_minute_sync(monkeypatch):
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tickflow")

    assert kline_sync.minute_universe_sync_supported() is True


def test_legacy_yaml_minute_provider_keeps_full_market_sync_compatibility(monkeypatch):
    """旧 YAML Provider 未声明新属性时, 保留原有全市场同步行为。"""
    provider = SimpleNamespace()
    capset = CapabilitySet()
    capset.grant(Cap.KLINE_MINUTE_BATCH)
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "legacy_yaml")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "legacy_yaml" and dataset == "minute",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)

    assert kline_sync.minute_universe_sync_supported() is True
    assert kline_sync.minute_universe_sync_allowed(capset) is True


def test_full_market_minute_requires_capability_and_provider_support(monkeypatch):
    capset = CapabilitySet()
    monkeypatch.setattr(kline_sync, "minute_universe_sync_supported", lambda: False)
    capset.grant(Cap.KLINE_MINUTE_BATCH)

    assert kline_sync.minute_universe_sync_allowed(capset) is False


def test_full_market_minute_endpoint_rejects_tdx_without_reading_tickflow_pool(monkeypatch):
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(repo=object(), capabilities=object())),
    )
    monkeypatch.setattr(kline_api, "_minute_allowed", lambda capset: True)
    monkeypatch.setattr(kline_api.kline_sync, "minute_universe_sync_allowed", lambda capset: False)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(kline_api.sync_minute(request))

    assert exc_info.value.status_code == 403
    assert "分组分钟K" in str(exc_info.value.detail)


def test_service_rejects_tdx_full_market_minute_even_when_batch_cap_is_augmented(monkeypatch):
    capset = CapabilitySet()
    capset.grant(Cap.KLINE_MINUTE_BATCH)
    monkeypatch.setattr(kline_sync, "minute_universe_sync_allowed", lambda capset: False)

    with pytest.raises(PermissionError, match="不支持全市场"):
        kline_sync.sync_and_persist_minute(
            ["600000.SH"], object(), capset, universe_sync=True,
        )


def test_tdx_minute_tcp_failure_does_not_fallback_to_tickflow(monkeypatch):
    provider = MagicMock()
    provider.fallback_to_tickflow_on_error = False
    provider.get_minute.side_effect = RuntimeError("TCP timeout")
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "minute",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 失败时不应回退 TickFlow")),
    )

    df = kline_sync.sync_minute_batch(
        ["600000.SH"],
        start_time=None,
        end_time=None,
    )

    assert df.is_empty()


def test_tdx_minute_persistence_raises_tcp_failure_instead_of_reporting_zero_rows(monkeypatch, tmp_path):
    provider = MagicMock()
    provider.fallback_to_tickflow_on_error = False
    provider.get_minute.side_effect = RuntimeError("TCP timeout")
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "minute",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)
    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda *_args: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda *_args: None)
    monkeypatch.setattr(kline_sync, "_latest_minute_datetime", lambda *_args: None)
    monkeypatch.setattr(kline_sync, "resolve_limit", lambda *_args, **_kwargs: SimpleNamespace(batch=100, rpm=30))
    monkeypatch.setattr(kline_sync.preferences, "get_minute_sync_segment_days", lambda: 20)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 失败时不应回退 TickFlow")),
    )
    repo = MagicMock()
    repo.store.data_dir = tmp_path

    with pytest.raises(kline_sync.MinuteProviderError, match="TCP timeout"):
        kline_sync.sync_and_persist_minute(["600000.SH"], repo, MagicMock())


def test_tdx_minute_persistence_reports_partial_failure_after_writing_successes(monkeypatch, tmp_path):
    class _PartialProvider:
        fallback_to_tickflow_on_error = False
        supports_minute_failure_reporting = True

        def get_minute(self, _symbols, **kwargs):
            kwargs["failed_out"].append("000001.SZ")
            return _minute_frame()

    provider = _PartialProvider()
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "minute",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)
    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda *_args: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda *_args: None)
    monkeypatch.setattr(kline_sync, "_latest_minute_datetime", lambda *_args: None)
    monkeypatch.setattr(kline_sync, "resolve_limit", lambda *_args, **_kwargs: SimpleNamespace(batch=100, rpm=30))
    monkeypatch.setattr(kline_sync.preferences, "get_minute_sync_segment_days", lambda: 20)
    repo = MagicMock()
    repo.store.data_dir = tmp_path

    with pytest.raises(kline_sync.MinuteProviderError, match="部分拉取失败"):
        kline_sync.sync_and_persist_minute(["600000.SH", "000001.SZ"], repo, MagicMock())

    assert (tmp_path / "kline_minute").exists()


def test_legacy_minute_provider_is_not_passed_failure_output(monkeypatch):
    class _LegacyProvider:
        def get_minute(
            self,
            _symbols,
            *,
            start_time,
            end_time,
            asset_type,
            freq,
            on_chunk_done,
        ):
            return _minute_frame()

    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "legacy_yaml")
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", lambda *_args: True)
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda _name: _LegacyProvider())

    failed_symbols: list[str] = []
    df = kline_sync.sync_minute_batch(
        ["600000.SH"],
        start_time=datetime(2026, 9, 4, 9, 25),
        end_time=datetime(2026, 9, 4, 15, 5),
        failed_out=failed_symbols,
    )

    assert df.height == 1
    assert failed_symbols == []


def test_tdx_minute_batch_maps_reported_partial_failure_to_502(monkeypatch):
    def _partial_sync(_symbols, *, failed_out=None, **_kwargs):
        assert failed_out is not None
        failed_out.append("000001.SZ")
        return _minute_frame()

    repo = MagicMock()
    repo.get_etf_symbol_set.return_value = set()
    repo.get_minute_batch.return_value = pl.DataFrame()
    capset = MagicMock()
    capset.has.return_value = True
    capset.limits.return_value = None
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(repo=repo, capabilities=capset)))
    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", _partial_sync)

    with pytest.raises(HTTPException) as exc_info:
        kline_api.get_minute_batch(
            request,
            {"symbols": ["600000.SH"], "date": "2026-09-04"},
        )

    assert exc_info.value.status_code == 502
    assert "000001.SZ" in str(exc_info.value.detail)


def test_unavailable_tdx_minute_provider_stays_fail_closed(monkeypatch):
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", lambda *_args: False)
    monkeypatch.setattr("app.data_providers.custom.plugin_requires_source_isolation", lambda _name: True)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 不可用时不应回退 TickFlow")),
    )

    assert kline_sync.minute_universe_sync_supported() is False
    assert kline_sync.sync_minute_batch(["600000.SH"]).is_empty()


def test_unavailable_tdx_minute_provider_raises_for_persistence_without_tickflow_capability(monkeypatch, tmp_path):
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", lambda *_args: False)
    monkeypatch.setattr("app.data_providers.custom.plugin_requires_source_isolation", lambda _name: True)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 不可用时不应回退 TickFlow")),
    )
    repo = MagicMock()
    repo.store.data_dir = tmp_path
    capset = SimpleNamespace(has=lambda _cap: False)

    with pytest.raises(kline_sync.MinuteProviderError, match="tdx_mcp"):
        kline_sync.sync_and_persist_minute(["600000.SH"], repo, capset)


def test_unavailable_tdx_minute_provider_cannot_fallback_in_intraday_monitor(monkeypatch):
    capset = CapabilitySet()
    capset.grant(Cap.KLINE_MINUTE_BATCH)
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", lambda *_args: False)
    monkeypatch.setattr("app.data_providers.custom.plugin_requires_source_isolation", lambda _name: True)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("分时监控不应回退 TickFlow")),
    )

    support = kline_sync.intraday_monitor_support(capset)

    assert support["available"] is False
    assert kline_sync.fetch_intraday_monitor_batch(["600000.SH"], capset).is_empty()
