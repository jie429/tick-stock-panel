"""TDX 新增数据集的服务路由与来源隔离测试。"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest

from app.services import kline_sync
from app.services.quote_service import QuoteService
from app.tickflow.capabilities import Cap, CapabilitySet


def test_selected_tdx_adj_factor_unavailable_does_not_fallback_to_tickflow(monkeypatch):
    capset = CapabilitySet()
    capset.grant(Cap.ADJ_FACTOR)
    monkeypatch.setattr(kline_sync.preferences, "get_adj_factor_provider", lambda: "tdx_mcp")
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", lambda *_args: False)
    monkeypatch.setattr("app.data_providers.custom.plugin_requires_source_isolation", lambda _name: True)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 不可用时不应回退 TickFlow")),
    )

    with pytest.raises(RuntimeError, match="tdx_mcp"):
        kline_sync.sync_adj_factor(["600000.SH"], MagicMock(), capset)


def test_fetch_single_adj_factor_uses_selected_tdx_provider(monkeypatch):
    expected = pl.DataFrame({
        "symbol": ["600000.SH"],
        "trade_date": ["2026-07-16"],
        "ex_factor": [1.047244094488189],
    })
    provider = MagicMock()
    provider.get_adj_factors.return_value = expected
    monkeypatch.setattr(kline_sync.preferences, "get_adj_factor_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "adj_factor",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda _name: provider)
    monkeypatch.setattr(
        kline_sync,
        "get_client",
        lambda: (_ for _ in ()).throw(AssertionError("不应绕过 TDX 调 TickFlow")),
    )

    result = kline_sync.fetch_adj_factor_single("600000.SH")

    assert result.equals(expected)
    provider.get_adj_factors.assert_called_once_with(
        ["600000.SH"], start_time=None, end_time=None, asset_type="stock",
    )


def test_selected_tdx_realtime_unavailable_does_not_fallback_to_tickflow(monkeypatch):
    service = QuoteService.__new__(QuoteService)
    from app.services import preferences

    monkeypatch.setattr(preferences, "get_realtime_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", lambda *_args: False)
    monkeypatch.setattr("app.data_providers.custom.plugin_requires_source_isolation", lambda _name: True)
    monkeypatch.setattr(
        "app.tickflow.client.get_paid_realtime_client",
        lambda: (_ for _ in ()).throw(AssertionError("TDX 不可用时不应回退 TickFlow")),
    )

    service._fetch_full_market_quotes()


def test_tdx_realtime_interval_uses_provider_floor(monkeypatch):
    from app.services import preferences
    provider = SimpleNamespace(realtime_min_interval=30.0)
    monkeypatch.setattr(preferences, "get_realtime_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "realtime",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda _name: provider)

    service = QuoteService.__new__(QuoteService)

    assert service.get_min_interval() == 30.0
    assert service._clamp_interval(6.0) == 30.0
