"""能力标准统一: 自定义/插件数据源能力增广回归测试。

对应 _augment_custom_sources 的数据集→能力映射
(daily/adj_factor/minute/depth5/financial/full_minute):
某数据集的当前 provider 非 tickflow 且声明了该数据集 → grant 对应能力;
取数路由仍按 preferences 分流, 不会误调 TickFlow。
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet
from app.tickflow.policy import _augment_custom_sources


def _set_providers(monkeypatch, *, daily="tickflow", adj="tickflow",
                   minute="tickflow", depth5="tickflow", financial="tickflow",
                   full_minute="tickflow") -> None:
    """mock preferences 各数据集 provider getter。"""
    from app.services import preferences
    monkeypatch.setattr(preferences, "get_daily_data_provider", lambda: daily)
    monkeypatch.setattr(preferences, "get_adj_factor_provider", lambda: adj)
    monkeypatch.setattr(preferences, "get_minute_data_provider", lambda: minute)
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: depth5)
    monkeypatch.setattr(preferences, "get_financial_provider", lambda: financial)
    monkeypatch.setattr(preferences, "get_full_minute_data_provider", lambda: full_minute)


def _set_datasets(monkeypatch, datasets: set[str]) -> None:
    """mock provider_has_dataset: 非 tickflow provider 对给定数据集返回 True。"""
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: name != "tickflow" and ds in datasets,
    )


def test_daily_custom_source_grants_daily_batch(monkeypatch):
    _set_providers(monkeypatch, daily="mock_src")
    _set_datasets(monkeypatch, {"daily"})
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert capset.has(Cap.KLINE_DAILY_BATCH)
    # 未声明其他数据集 → 不补
    assert not capset.has(Cap.ADJ_FACTOR)
    assert not capset.has(Cap.KLINE_MINUTE_BATCH)
    assert not capset.has(Cap.FINANCIAL)


def test_adj_custom_source_grants_adj_factor(monkeypatch):
    """adj 显式路由到声明除权的自定义源 → 补授能力 (跟随日K已下线, 独立判定)。"""
    _set_providers(monkeypatch, adj="mock_src")
    _set_datasets(monkeypatch, {"adj_factor"})
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert capset.has(Cap.ADJ_FACTOR)


def test_full_minute_custom_source_grants_intraday_universe(monkeypatch):
    """全量分钟: 声明 full_minute 数据集且被路由 → 补授 INTRADAY_UNIVERSE,
    minute_refresh 服务门控与 TickFlow Expert 口径统一。"""
    _set_providers(monkeypatch, full_minute="mock_src")
    _set_datasets(monkeypatch, {"full_minute"})
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert capset.has(Cap.INTRADAY_UNIVERSE)
    # 声明了 full_minute 不等于声明 minute → 不补逐标的分钟K能力
    assert not capset.has(Cap.KLINE_MINUTE_BATCH)


def test_full_minute_dataset_without_routing_not_granted(monkeypatch):
    """源声明了 full_minute 但路由仍是 tickflow → 不增广 (TickFlow 档位自决)。"""
    _set_providers(monkeypatch, full_minute="tickflow")
    _set_datasets(monkeypatch, {"full_minute"})
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert not capset.has(Cap.INTRADAY_UNIVERSE)


def test_minute_custom_source_grants_minute_batch(monkeypatch):
    """原有 minute 增广行为保持。"""
    _set_providers(monkeypatch, minute="mock_src")
    _set_datasets(monkeypatch, {"minute"})
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert capset.has(Cap.KLINE_MINUTE_BATCH)


def test_depth5_builtin_provider_grants_depth_batch(monkeypatch):
    """选中声明 depth5 的内置插件时, 旧能力门控也必须放行。"""
    _set_providers(monkeypatch, depth5="tdx_mcp")
    _set_datasets(monkeypatch, {"depth5"})
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert capset.has(Cap.DEPTH5_BATCH)


def test_financial_custom_source_grants_financial(monkeypatch):
    _set_providers(monkeypatch, financial="mock_src")
    _set_datasets(monkeypatch, {"financial"})
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert capset.has(Cap.FINANCIAL)


def test_provider_active_but_dataset_not_declared_no_grant(monkeypatch):
    """provider 被选为当前源但未声明该数据集 → 不 grant (回退 TickFlow 语义)。"""
    _set_providers(monkeypatch, daily="mock_src", minute="mock_src",
                   adj="mock_src", depth5="mock_src", financial="mock_src")
    _set_datasets(monkeypatch, set())  # 什么都不声明
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert not capset.has(Cap.KLINE_DAILY_BATCH)
    assert not capset.has(Cap.ADJ_FACTOR)
    assert not capset.has(Cap.KLINE_MINUTE_BATCH)
    assert not capset.has(Cap.DEPTH5_BATCH)
    assert not capset.has(Cap.FINANCIAL)


def test_tickflow_active_no_grant(monkeypatch):
    """全部数据集仍走 tickflow → 不补任何能力。"""
    _set_providers(monkeypatch)  # 默认全 tickflow
    _set_datasets(monkeypatch, {"daily", "adj_factor", "minute", "depth5", "financial"})
    capset = CapabilitySet()
    _augment_custom_sources(capset)
    assert not capset.has(Cap.KLINE_DAILY_BATCH)
    assert not capset.has(Cap.ADJ_FACTOR)
    assert not capset.has(Cap.KLINE_MINUTE_BATCH)
    assert not capset.has(Cap.DEPTH5_BATCH)
    assert not capset.has(Cap.FINANCIAL)


def test_grant_does_not_override_tickflow_limits(monkeypatch):
    """grant 不覆盖 TickFlow 已有能力及其限制。"""
    _set_providers(monkeypatch, minute="mock_src")
    _set_datasets(monkeypatch, {"minute"})
    capset = CapabilitySet()
    capset.grant(Cap.KLINE_MINUTE_BATCH, CapabilityLimits(rpm=30, batch=100))
    _augment_custom_sources(capset)
    lim = capset.limits(Cap.KLINE_MINUTE_BATCH)
    assert lim is not None and lim.rpm == 30 and lim.batch == 100


def test_update_data_providers_refreshes_capability_snapshot(monkeypatch):
    """切换数据源后 app.state.capabilities 快照应刷新 (读缓存+增广, 无网络)。"""
    from app.api import settings as settings_api

    monkeypatch.setattr("app.services.preferences.save", lambda upd: None)
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "mock_src" and dataset == "daily",
    )
    sentinel = CapabilitySet()
    monkeypatch.setattr(settings_api, "detect_capabilities", lambda: sentinel)

    mock_request = MagicMock()
    settings_api.update_data_providers(
        MagicMock(model_dump=lambda exclude_none: {"daily_data_provider": "mock_src"}),
        mock_request,
    )
    assert mock_request.app.state.capabilities is sentinel


def test_update_depth5_provider_syncs_runtime_after_capability_refresh(monkeypatch):
    """切换五档源时, 先替换能力快照, 再重启/清理 depth 服务运行态。"""
    from app.api import settings as settings_api
    from app.services import preferences

    current = {
        "daily": "tickflow",
        "adj": "tickflow",
        "minute": "tickflow",
        "depth5": "tickflow",
        "realtime": "tickflow",
        "financial": "tickflow",
    }
    events: list[str] = []
    refreshed_capset = CapabilitySet()
    request = MagicMock()
    request.app.state.capabilities = CapabilitySet()
    depth_service = MagicMock()
    request.app.state.depth_service = depth_service

    def save(updates: dict[str, str]) -> None:
        events.append("save")
        current["depth5"] = updates["depth5_data_provider"]

    def sync_provider_change() -> None:
        events.append("sync")
        assert current["depth5"] == "tdx_mcp"
        assert request.app.state.capabilities is refreshed_capset

    depth_service.sync_provider_change.side_effect = sync_provider_change
    depth_service.begin_provider_change.side_effect = lambda: events.append("begin")
    monkeypatch.setattr(preferences, "save", save)
    monkeypatch.setattr(preferences, "get_daily_data_provider", lambda: current["daily"])
    monkeypatch.setattr(preferences, "get_adj_factor_provider", lambda: current["adj"])
    monkeypatch.setattr(preferences, "get_minute_data_provider", lambda: current["minute"])
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: current["depth5"])
    monkeypatch.setattr(preferences, "get_realtime_data_provider", lambda: current["realtime"])
    monkeypatch.setattr(preferences, "get_financial_provider", lambda: current["financial"])
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "depth5",
    )
    monkeypatch.setattr(
        settings_api,
        "detect_capabilities",
        lambda *args, **kwargs: (events.append("detect") or refreshed_capset),
    )

    settings_api.update_data_providers(
        settings_api.DataProvidersIn(depth5_data_provider="tdx_mcp"),
        request,
    )

    assert events == ["begin", "save", "detect", "sync"]
    depth_service.begin_provider_change.assert_called_once_with()
    depth_service.sync_provider_change.assert_called_once_with()


def test_reload_data_sources_refreshes_capabilities_and_syncs_depth_service(monkeypatch):
    """插件重载后, depth5 能力与轮询状态必须基于新注册表同步。"""
    from app.api import settings as settings_api

    events: list[str] = []
    refreshed_capset = CapabilitySet()
    request = MagicMock()
    depth_service = MagicMock()
    request.app.state.depth_service = depth_service
    depth_service.begin_provider_change.side_effect = lambda: events.append("begin")

    monkeypatch.setattr(
        "app.data_providers.custom.load_all",
        lambda: events.append("load"),
    )
    monkeypatch.setattr(
        settings_api,
        "detect_capabilities",
        lambda *args, **kwargs: (events.append("detect") or refreshed_capset),
    )
    monkeypatch.setattr(settings_api, "list_data_sources", lambda: {"sources": []})

    result = settings_api.reload_data_sources(request)

    assert result == {"sources": []}
    assert events == ["begin", "load", "detect"]
    assert request.app.state.capabilities is refreshed_capset
    depth_service.begin_provider_change.assert_called_once_with()
    depth_service.sync_provider_change.assert_called_once_with()


def test_reload_data_sources_failure_aborts_depth_transition(monkeypatch):
    from app.api import settings as settings_api

    request = MagicMock()
    depth_service = MagicMock()
    request.app.state.depth_service = depth_service
    monkeypatch.setattr(
        "app.data_providers.custom.load_all",
        lambda: (_ for _ in ()).throw(RuntimeError("reload failed")),
    )

    with pytest.raises(RuntimeError, match="reload failed"):
        settings_api.reload_data_sources(request)

    depth_service.begin_provider_change.assert_called_once_with()
    depth_service.abort_provider_change.assert_called_once_with()
    depth_service.sync_provider_change.assert_not_called()


def test_update_depth5_provider_failure_aborts_depth_transition(monkeypatch):
    from app.api import settings as settings_api

    request = MagicMock()
    depth_service = MagicMock()
    request.app.state.depth_service = depth_service
    monkeypatch.setattr(
        "app.services.preferences.save",
        lambda _updates: (_ for _ in ()).throw(RuntimeError("save failed")),
    )
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "depth5",
    )

    with pytest.raises(RuntimeError, match="save failed"):
        settings_api.update_data_providers(
            settings_api.DataProvidersIn(depth5_data_provider="tdx_mcp"),
            request,
        )

    depth_service.begin_provider_change.assert_called_once_with()
    depth_service.abort_provider_change.assert_called_once_with()
    depth_service.sync_provider_change.assert_not_called()


@pytest.mark.parametrize(
    ("field", "dataset"),
    [
        ("adj_factor_provider", "adj_factor"),
        ("realtime_data_provider", "realtime"),
        ("financial_data_provider", "financial"),
    ],
)
def test_update_data_providers_rejects_tdx_for_undeclared_dataset(monkeypatch, field, dataset):
    """设置端拒绝未声明的 TDX 数据集, 避免下游静默走 TickFlow。"""
    from fastapi import HTTPException

    from app.api import settings as settings_api

    saved = MagicMock()
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr("app.services.preferences.save", saved)
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, checked_dataset: calls.append((name, checked_dataset)) or False,
    )

    with pytest.raises(HTTPException) as exc_info:
        settings_api.update_data_providers(
            settings_api.DataProvidersIn(**{field: "tdx_mcp"}),
            MagicMock(),
        )

    assert exc_info.value.status_code == 422
    assert calls == [("tdx_mcp", dataset)]
    saved.assert_not_called()


def test_install_selected_plugin_refreshes_capabilities_and_depth_runtime(monkeypatch):
    from app.api import settings as settings_api
    from app.services import preferences

    events: list[str] = []
    refreshed_capset = CapabilitySet()
    request = MagicMock()
    request.app.state.capabilities = CapabilitySet()
    depth_service = MagicMock()
    request.app.state.depth_service = depth_service
    depth_service.begin_provider_change.side_effect = lambda: events.append("begin")
    depth_service.sync_provider_change.side_effect = lambda: events.append("sync")
    monkeypatch.setattr("app.data_providers.custom.is_builtin", lambda name: name == "tdx_mcp")
    monkeypatch.setattr(
        "app.data_providers.custom.install_plugin",
        lambda name: (events.append("install") or (True, "installed")),
    )
    monkeypatch.setattr("app.data_providers.custom.load_all", lambda: events.append("load"))
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        settings_api,
        "detect_capabilities",
        lambda: (events.append("detect") or refreshed_capset),
    )
    monkeypatch.setattr(settings_api, "list_data_sources", lambda: {"plugins": []})

    result = settings_api.install_plugin("tdx_mcp", request)

    assert result["install_ok"] is True
    assert result["install_message"] == "installed"
    assert events == ["begin", "install", "load", "detect", "sync"]
    assert request.app.state.capabilities is refreshed_capset
    depth_service.begin_provider_change.assert_called_once_with()
    depth_service.sync_provider_change.assert_called_once_with()


def test_update_depth5_provider_fences_tickflow_before_capability_refresh(monkeypatch):
    """save→detect 的窗口不能借用旧 custom capability 调 TickFlow。"""
    from types import SimpleNamespace

    from app.api import settings as settings_api
    from app.services import preferences
    from app.services.depth_service import DepthService

    current = {
        "daily": "tickflow",
        "adj": "tickflow",
        "minute": "tickflow",
        "depth5": "tdx_mcp",
        "realtime": "tickflow",
        "financial": "tickflow",
    }
    stale_capset = CapabilitySet()
    stale_capset.grant(Cap.DEPTH5_BATCH, CapabilityLimits(batch=1, rpm=None))
    refreshed_capset = CapabilitySet()
    depth_service = DepthService()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                capabilities=stale_capset,
                depth_service=depth_service,
            )
        )
    )
    depth_service.set_app_state(request.app.state)

    def save(updates: dict[str, str]) -> None:
        current["depth5"] = updates["depth5_data_provider"]
        assert depth_service._call_depth_batch(["600000.SH"]) == {}

    monkeypatch.setattr(preferences, "save", save)
    monkeypatch.setattr(preferences, "get_daily_data_provider", lambda: current["daily"])
    monkeypatch.setattr(preferences, "get_adj_factor_provider", lambda: current["adj"])
    monkeypatch.setattr(preferences, "get_minute_data_provider", lambda: current["minute"])
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: current["depth5"])
    monkeypatch.setattr(preferences, "get_realtime_data_provider", lambda: current["realtime"])
    monkeypatch.setattr(preferences, "get_financial_provider", lambda: current["financial"])
    monkeypatch.setattr(settings_api, "detect_capabilities", lambda *args, **kwargs: refreshed_capset)
    monkeypatch.setattr(
        "app.tickflow.client.get_client",
        lambda: (_ for _ in ()).throw(AssertionError("capability 刷新前不得创建 TickFlow client")),
    )

    settings_api.update_data_providers(
        settings_api.DataProvidersIn(depth5_data_provider="tickflow"),
        request,
    )

    assert request.app.state.capabilities is refreshed_capset
