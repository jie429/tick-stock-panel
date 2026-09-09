"""自定义 Provider 重载/关闭资源回收测试。"""
from __future__ import annotations

from unittest.mock import MagicMock

from app.data_providers.custom import loader


def test_close_all_unregisters_before_closing_and_isolates_failures(monkeypatch):
    broken = MagicMock(name="broken")
    broken.name = "broken"
    broken.close.side_effect = RuntimeError("close failed")
    healthy = MagicMock(name="healthy")
    healthy.name = "healthy"
    monkeypatch.setattr(loader, "_PROVIDERS", {"broken": broken, "healthy": healthy})
    monkeypatch.setattr(loader, "_PLUGIN_STATUS", {"tdx_mcp": {"name": "tdx_mcp"}})

    loader.close_all()

    broken.close.assert_called_once_with()
    healthy.close.assert_called_once_with()
    assert loader.names() == set()
    assert loader.list_plugins() == []


def test_load_all_reuses_close_all_for_existing_providers(monkeypatch, tmp_path):
    provider = MagicMock()
    provider.name = "old_provider"
    monkeypatch.setattr(loader, "_PROVIDERS", {"old_provider": provider})
    monkeypatch.setattr(loader, "_load_builtin_plugins", lambda: None)

    loader.load_all(tmp_path)

    provider.close.assert_called_once_with()
    assert loader.names() == set()
