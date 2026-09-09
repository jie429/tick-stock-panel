from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.api import settings
from app.data_providers import custom as custom_sources
from app.services import preferences


def test_uninstall_current_depth5_provider_refreshes_caps_and_syncs_runtime(monkeypatch):
    saved: list[dict[str, str]] = []
    events: list[str] = []
    current = {
        "daily": "tickflow",
        "minute": "tickflow",
        "depth5": "tdx_mcp",
        "realtime": "tickflow",
        "financial": "tickflow",
    }
    refreshed_capset = object()
    depth_service = MagicMock()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                capabilities=object(),
                depth_service=depth_service,
            )
        )
    )

    def save(updates: dict[str, str]) -> None:
        events.append("save")
        saved.append(dict(updates))
        if "depth5_data_provider" in updates:
            current["depth5"] = updates["depth5_data_provider"]

    def sync_provider_change() -> None:
        events.append("sync")
        assert current["depth5"] == "tickflow"
        assert request.app.state.capabilities is refreshed_capset

    depth_service.sync_provider_change.side_effect = sync_provider_change
    depth_service.begin_provider_change.side_effect = lambda: events.append("begin")

    monkeypatch.setattr(custom_sources, "is_builtin", lambda name: name == "tdx_mcp")
    monkeypatch.setattr(
        custom_sources,
        "uninstall_plugin",
        lambda name: (events.append("uninstall") or (True, "uninstalled")),
    )
    monkeypatch.setattr(custom_sources, "load_all", lambda: events.append("load"))
    monkeypatch.setattr(settings, "list_data_sources", lambda: {"plugins": []})
    monkeypatch.setattr(preferences, "get_daily_data_provider", lambda: current["daily"])
    monkeypatch.setattr(preferences, "get_minute_data_provider", lambda: current["minute"])
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: current["depth5"])
    monkeypatch.setattr(preferences, "get_realtime_data_provider", lambda: current["realtime"])
    monkeypatch.setattr(preferences, "get_financial_provider", lambda: current["financial"])
    monkeypatch.setattr(preferences, "save", save)
    monkeypatch.setattr(
        settings,
        "detect_capabilities",
        lambda *args, **kwargs: (events.append("detect") or refreshed_capset),
    )

    result = settings.uninstall_plugin("tdx_mcp", request)

    assert saved == [{"depth5_data_provider": "tickflow"}]
    assert events == ["begin", "uninstall", "save", "load", "detect", "sync"]
    depth_service.begin_provider_change.assert_called_once_with()
    depth_service.sync_provider_change.assert_called_once_with()
    assert result["uninstall_ok"] is True
    assert result["uninstall_message"] == "uninstalled"


def test_failed_uninstall_keeps_provider_and_aborts_depth_transition(monkeypatch):
    saved: list[dict[str, str]] = []
    events: list[str] = []
    current = {
        "daily": "tickflow",
        "minute": "tickflow",
        "depth5": "tdx_mcp",
        "realtime": "tickflow",
        "financial": "tickflow",
    }
    depth_service = MagicMock()
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(capabilities=object(), depth_service=depth_service)),
    )
    depth_service.begin_provider_change.side_effect = lambda: events.append("begin")
    depth_service.abort_provider_change.side_effect = lambda: events.append("abort")

    monkeypatch.setattr(custom_sources, "is_builtin", lambda name: name == "tdx_mcp")
    monkeypatch.setattr(
        custom_sources,
        "uninstall_plugin",
        lambda name: (events.append("uninstall") or (False, "uninstall failed")),
    )
    monkeypatch.setattr(custom_sources, "load_all", lambda: pytest.fail("失败卸载不应重载"))
    monkeypatch.setattr(settings, "detect_capabilities", lambda: pytest.fail("失败卸载不应刷新能力"))
    monkeypatch.setattr(settings, "list_data_sources", lambda: {"plugins": []})
    monkeypatch.setattr(preferences, "get_daily_data_provider", lambda: current["daily"])
    monkeypatch.setattr(preferences, "get_minute_data_provider", lambda: current["minute"])
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: current["depth5"])
    monkeypatch.setattr(preferences, "get_realtime_data_provider", lambda: current["realtime"])
    monkeypatch.setattr(preferences, "get_financial_provider", lambda: current["financial"])
    monkeypatch.setattr(preferences, "save", lambda updates: saved.append(dict(updates)))

    result = settings.uninstall_plugin("tdx_mcp", request)

    assert result["uninstall_ok"] is False
    assert result["uninstall_message"] == "uninstall failed"
    assert saved == []
    assert events == ["begin", "uninstall", "abort"]
    depth_service.sync_provider_change.assert_not_called()
