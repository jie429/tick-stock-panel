"""五档盘口的第三方 Provider 路由测试。"""
from __future__ import annotations

import threading
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest

from app.services.depth_service import DepthService
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

_VALID_DEPTH = {
    "600000.SH": {
        "ask_volumes": [0, 1, 2, 3, 4],
        "bid_volumes": [5, 6, 7, 8, 9],
        "timestamp": 1_788_185_600_000,
    },
}


def _configure_custom_depth(monkeypatch, provider) -> None:
    from app.services import preferences

    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: "tdx_mcp")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "tdx_mcp" and dataset == "depth5",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)


def test_custom_depth_provider_is_a_capability_without_tickflow_capset(monkeypatch):
    provider = MagicMock()
    provider.get_depth5 = MagicMock(return_value=_VALID_DEPTH)
    _configure_custom_depth(monkeypatch, provider)

    assert DepthService()._has_capability() is True


def test_custom_depth_provider_is_called_without_tickflow_fallback(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.return_value = _VALID_DEPTH
    _configure_custom_depth(monkeypatch, provider)
    monkeypatch.setattr(
        "app.tickflow.client.get_client",
        lambda: (_ for _ in ()).throw(AssertionError("不应调用 TickFlow")),
    )

    result = DepthService()._call_depth_batch(["600000.SH"])

    assert result == _VALID_DEPTH
    provider.get_depth5.assert_called_once_with(["600000.SH"])


def test_custom_depth_failure_is_fail_closed_and_does_not_call_tickflow(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.side_effect = RuntimeError("TDX timeout")
    _configure_custom_depth(monkeypatch, provider)
    monkeypatch.setattr(
        "app.tickflow.client.get_client",
        lambda: (_ for _ in ()).throw(AssertionError("不应回退 TickFlow")),
    )

    assert DepthService()._call_depth_batch(["600000.SH"]) == {}


def test_missing_custom_depth_method_is_not_reported_as_available(monkeypatch):
    _configure_custom_depth(monkeypatch, object())
    service = DepthService()

    assert service._has_capability() is False
    assert service._call_depth_batch(["600000.SH"]) == {}


def test_malformed_custom_depth_response_is_dropped_fail_closed(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.return_value = {
        "600000.SH": {"ask_volumes": "not-a-list", "bid_volumes": [1], "timestamp": 1},
    }
    _configure_custom_depth(monkeypatch, provider)

    assert DepthService()._call_depth_batch(["600000.SH"]) == {}


def test_tickflow_depth_route_keeps_existing_chunking_and_limits(monkeypatch):
    from app.services import preferences

    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: "tickflow")
    capset = CapabilitySet()
    capset.grant(Cap.DEPTH5_BATCH, CapabilityLimits(batch=1, rpm=None))
    service = DepthService()
    monkeypatch.setattr(service, "_get_capset", lambda: capset)

    tickflow = MagicMock()
    tickflow.depth.batch.side_effect = lambda symbols: {
        symbol: {"ask_volumes": [0], "bid_volumes": [1], "timestamp": 1}
        for symbol in symbols
    }
    monkeypatch.setattr("app.tickflow.client.get_client", lambda: tickflow)

    result = service._call_depth_batch(["600000.SH", "000001.SZ"])

    assert set(result) == {"600000.SH", "000001.SZ"}
    assert tickflow.depth.batch.call_count == 2
    assert tickflow.depth.batch.call_args_list[0].args == (["600000.SH"],)
    assert tickflow.depth.batch.call_args_list[1].args == (["000001.SZ"],)


@pytest.mark.parametrize(
    ("has_capability", "expected_transition"),
    [(True, "start"), (False, "stop")],
)
def test_sync_provider_change_invalidates_stale_cache_and_resyncs_polling(
    monkeypatch,
    has_capability,
    expected_transition,
):
    """运行中切换源不能继续用旧线程或旧 provider 产生的 sealed 缓存。"""
    service = DepthService()
    stale_date = date(2026, 9, 4)
    service._running = True
    service._sealed_cache = {
        "600000.SH": {
            "sealed_up": True,
            "sealed_down": None,
            "ask1_vol": 0,
            "bid1_vol": 100,
            "status": "limit_up",
            "fetched_ts": 1.0,
        }
    }
    service._sealed_ready = True
    service._sealed_date = stale_date
    service._sealed_fetched_ts = 1.0
    service._sealed_fetched_at = 2.0
    transitions: list[str] = []

    def assert_invalidated() -> None:
        assert service._sealed_cache == {}
        assert service._sealed_ready is False
        assert service._sealed_date is None
        assert service._sealed_fetched_ts == 0.0
        assert service._sealed_fetched_at == 0.0

    def stop_polling() -> None:
        assert_invalidated()
        transitions.append("stop")
        service._running = False

    def start_polling() -> None:
        assert_invalidated()
        transitions.append("start")

    monkeypatch.setattr(service, "stop_polling", stop_polling)
    monkeypatch.setattr(service, "start_polling", start_polling)
    monkeypatch.setattr(service, "_has_capability", lambda: has_capability)

    service.sync_provider_change()

    assert transitions == [expected_transition]
    assert service.is_sealed_ready(stale_date) is False


def test_old_fetch_cannot_commit_after_provider_generation_changes(monkeypatch, tmp_path):
    """Provider 在请求返回前切换时, 旧请求结果不能重新写入当前 sealed 缓存。"""
    trade_date = date(2026, 9, 4)
    service = DepthService()
    service._fetch_lock = threading.RLock()
    service.set_repo(
        SimpleNamespace(
            get_enriched_latest=lambda: (
                pl.DataFrame({"symbol": ["600000.SH"], "signal_limit_up": [True]}),
                trade_date,
            ),
            store=SimpleNamespace(data_dir=tmp_path),
        )
    )
    monkeypatch.setattr("app.services.depth_service.cn_today", lambda: trade_date)
    monkeypatch.setattr(service, "_has_capability", lambda: False)
    monkeypatch.setattr(service, "stop_polling", lambda: None)
    monkeypatch.setattr(service, "_notify_depth_updated", lambda _count: None)

    def old_provider_fetch(_symbols: list[str]) -> dict:
        # 此时旧源请求已经开始; 返回前设置页完成了 Provider 切换。
        service.sync_provider_change()
        return {
            "600000.SH": {
                "ask_volumes": [0],
                "bid_volumes": [100],
                "timestamp": 1_788_185_600_000,
            }
        }

    monkeypatch.setattr(service, "_call_depth_batch", old_provider_fetch)

    service._fetch_and_seal()

    assert service._sealed_cache == {}
    assert service._sealed_ready is False
    assert service.get_sealed_map(trade_date, is_down=False) == {}


def test_tickflow_depth_dispatch_fails_closed_after_capability_is_removed(monkeypatch):
    """切回无五档能力的 TickFlow 后, 竞态中的后续 dispatch 也不能发网络请求。"""
    from app.services import preferences

    service = DepthService()
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: "tickflow")
    monkeypatch.setattr(service, "_get_capset", CapabilitySet)
    monkeypatch.setattr(
        "app.tickflow.client.get_client",
        lambda: (_ for _ in ()).throw(AssertionError("失能后不得创建 TickFlow depth 客户端")),
    )

    assert service._call_depth_batch(["600000.SH"]) == {}


def test_provider_transition_fence_rejects_tickflow_before_capability_refresh(monkeypatch):
    """切到 TickFlow 但 capability 尚未刷新时, 过渡栅栏仍必须禁止网络请求。"""
    from app.services import preferences

    current = {"provider": "tdx_mcp"}
    capset = CapabilitySet()
    capset.grant(Cap.DEPTH5_BATCH, CapabilityLimits(batch=1, rpm=None))
    service = DepthService()
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: current["provider"])
    monkeypatch.setattr(service, "_get_capset", lambda: capset)
    monkeypatch.setattr(
        "app.tickflow.client.get_client",
        lambda: (_ for _ in ()).throw(AssertionError("过渡期不得创建 TickFlow client")),
    )

    service._provider_snapshot()
    service.begin_provider_change()
    current["provider"] = "tickflow"

    assert service._call_depth_batch(["600000.SH"]) == {}


def test_abort_provider_change_reopens_depth_dispatch_after_settings_failure():
    service = DepthService()

    service.begin_provider_change()
    assert service._provider_transitioning is True

    service.abort_provider_change()

    assert service._provider_transitioning is False


def _write_depth_parquet(data_dir, trade_date: date, provider_name: str) -> None:
    out = data_dir / "depth5" / f"date={trade_date.isoformat()}" / "part.parquet"
    out.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "sealed_up": [True],
            "sealed_down": [False],
            "ask1_vol": [0],
            "bid1_vol": [100],
            "status": ["limit_up"],
            "fetched_at": [1_788_185_600.0],
            "provider_name": [provider_name],
        }
    ).write_parquet(out)


@pytest.mark.parametrize("operation", ["read", "restore"])
def test_old_provider_parquet_is_rejected_after_selecting_new_provider(
    monkeypatch,
    tmp_path,
    operation,
):
    """当日 depth parquet 标注旧源时, 直接读取和重启恢复都必须 fail-closed。"""
    from app.services import preferences

    trade_date = date(2026, 9, 4)
    _write_depth_parquet(tmp_path, trade_date, provider_name="tickflow")
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: "tdx_mcp")

    service = DepthService()
    service.set_repo(SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path)))

    if operation == "read":
        assert service.get_sealed_map(trade_date, is_down=False) == {}
        assert service.is_sealed_ready(trade_date) is False
    else:
        service._restore_from_parquet(trade_date)
        assert service._sealed_cache == {}
        assert service._sealed_ready is False
        assert service._sealed_date is None


def test_provider_epoch_rejects_same_source_snapshot_after_round_trip_switch(monkeypatch, tmp_path):
    """TDX→TickFlow→TDX 后重启不能把第一次 TDX 的当天 sealed 文件重新当作当前数据。"""
    from app.services import preferences

    trade_date = date(2026, 9, 4)
    current = {"provider": "tdx_mcp"}
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: current["provider"])

    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    service = DepthService()
    service.set_repo(repo)
    first_context = service._provider_snapshot().context
    service._persist(
        trade_date,
        {
            "600000.SH": {
                "sealed_up": True,
                "sealed_down": None,
                "ask1_vol": 0,
                "bid1_vol": 100,
                "status": "limit_up",
                "fetched_ts": 1_788_185_600.0,
            },
        },
        first_context,
    )
    assert service.get_sealed_map(trade_date, is_down=False)["600000.SH"]["sealed"] is True

    monkeypatch.setattr(service, "_has_capability", lambda: False)
    monkeypatch.setattr(service, "stop_polling", lambda: None)
    monkeypatch.setattr(service, "_notify_depth_updated", lambda _count: None)
    current["provider"] = "tickflow"
    service.sync_provider_change()
    current["provider"] = "tdx_mcp"
    service.sync_provider_change()

    restarted = DepthService()
    restarted.set_repo(repo)
    assert restarted._provider_snapshot().context != first_context
    assert restarted.get_sealed_map(trade_date, is_down=False) == {}
    assert restarted.is_sealed_ready(trade_date) is False


def test_provider_epoch_is_renewed_when_intermediate_context_write_fails(monkeypatch, tmp_path):
    """P→Q 的 context 未写盘时, 切回 P 仍不能复用旧 P 的 parquet。"""
    from app.services import preferences

    trade_date = date(2026, 9, 4)
    current = {"provider": "tdx_mcp"}
    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: current["provider"])

    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    service = DepthService()
    service.set_repo(repo)
    first_context = service._provider_snapshot().context
    service._persist(
        trade_date,
        {
            "600000.SH": {
                "sealed_up": True,
                "sealed_down": None,
                "ask1_vol": 0,
                "bid1_vol": 100,
                "status": "limit_up",
                "fetched_ts": 1_788_185_600.0,
            },
        },
        first_context,
    )
    real_write_context = service._write_provider_context

    def drop_intermediate_context(context) -> None:
        if context.name != "tickflow":
            real_write_context(context)

    monkeypatch.setattr(service, "_write_provider_context", drop_intermediate_context)
    monkeypatch.setattr(service, "_has_capability", lambda: False)
    monkeypatch.setattr(service, "stop_polling", lambda: None)
    monkeypatch.setattr(service, "_notify_depth_updated", lambda _count: None)

    current["provider"] = "tickflow"
    service.sync_provider_change()
    current["provider"] = "tdx_mcp"
    service.sync_provider_change()

    assert service._provider_snapshot().context != first_context
    restarted = DepthService()
    restarted.set_repo(repo)
    assert restarted.get_sealed_map(trade_date, is_down=False) == {}


def test_transient_capability_gap_does_not_retire_polling_thread(monkeypatch):
    """插件重载瞬间查不到 Provider 时, 轮询应等待下一轮而非永久退出。"""
    service = DepthService()
    calls = 0
    polled = threading.Event()

    def has_capability() -> bool:
        nonlocal calls
        calls += 1
        return calls > 1

    def poll_once() -> None:
        polled.set()
        with service._lifecycle_lock:
            service._running = False

    monkeypatch.setattr(service, "_has_capability", has_capability)
    monkeypatch.setattr(service, "_is_continuous_trading", lambda: True)
    monkeypatch.setattr(service, "_poll_once", poll_once)
    monkeypatch.setattr(service, "_current_sleep_interval", lambda: 0.0)

    thread = threading.Thread(target=service._poll_loop)
    with service._lifecycle_lock:
        service._running = True
        service._thread = thread
    thread.start()
    thread.join(timeout=1)

    assert polled.is_set()
    assert not thread.is_alive()
