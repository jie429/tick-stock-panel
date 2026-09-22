"""个股盘口 (五档价 + 量, 可选成交方向) 契约测试。

覆盖三层:
- provider 边界收口 `_validate_custom_depth_result` 的价格扩展与 `_validate_trade_flow`;
- `DepthService.get_book` 的合并/门控/fail-closed 语义;
- `GET /api/quote/book` 的参数校验与响应形状。
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import quote as quote_api
from app.services.depth_service import DepthService, _validate_trade_flow

_FULL_ROW = {
    "600000.SH": {
        "ask_volumes": [10, 20, None, None, None],
        "bid_volumes": [30, 40, 50, 60, 70],
        "ask_prices": [10.01, 10.02, None, None, None],
        "bid_prices": [10.0, 9.99, 9.98, 9.97, 9.96],
        "timestamp": 1_788_185_600_000,
    },
}


def _configure_custom_depth(monkeypatch, provider) -> None:
    from app.services import preferences

    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: "mairui")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "mairui" and dataset == "depth5",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)


def _forbid_tickflow(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.tickflow.client.get_client",
        lambda: (_ for _ in ()).throw(AssertionError("不应调用 TickFlow")),
    )


def _client(service) -> TestClient:
    app = FastAPI()
    app.include_router(quote_api.router)
    app.state.depth_service = service
    return TestClient(app)


# ===== get_book: 价与量同源, 成交方向可选 =====


def test_get_book_maps_prices_volumes_and_trade_flow(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.return_value = dict(_FULL_ROW)
    provider.get_trade_flow.return_value = {
        "600000.SH": {"inside_volume": 600, "outside_volume": 634, "timestamp": 1},
    }
    _configure_custom_depth(monkeypatch, provider)

    book = DepthService().get_book(["600000.SH"])

    assert book == {
        "600000.SH": {
            "ask_prices": [10.01, 10.02, None, None, None],
            "ask_volumes": [10, 20, None, None, None],
            "bid_prices": [10.0, 9.99, 9.98, 9.97, 9.96],
            "bid_volumes": [30, 40, 50, 60, 70],
            "timestamp": 1_788_185_600_000,
            "inside_volume": 600.0,
            "outside_volume": 634.0,
        },
    }
    provider.get_trade_flow.assert_called_once_with(["600000.SH"])


def test_get_book_reports_missing_price_series_as_null(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.return_value = {
        "600000.SH": {
            "ask_volumes": [10, 20, 30, 40, 50],
            "bid_volumes": [5, 6, 7, 8, 9],
            "timestamp": 1_788_185_600_000,
        },
    }
    _configure_custom_depth(monkeypatch, provider)

    book = DepthService().get_book(["600000.SH"])["600000.SH"]

    # 数据源没有价格时整段为 None (前端显示 —), 不用 0 或昨收凑数
    assert book["ask_prices"] is None and book["bid_prices"] is None
    assert book["ask_volumes"] == [10, 20, 30, 40, 50]


def test_get_book_drops_malformed_price_series_but_keeps_volumes(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.return_value = {
        "600000.SH": {
            "ask_volumes": [10, 20, 30, 40, 50],
            "bid_volumes": [5, 6, 7, 8, 9],
            "ask_prices": "not-a-list",
            "bid_prices": [1.0, 2.0, 3.0, 4.0, 5.0],
            "timestamp": 1_788_185_600_000,
        },
    }
    _configure_custom_depth(monkeypatch, provider)

    book = DepthService().get_book(["600000.SH"])["600000.SH"]

    # 非法价格只丢该段 (置 None), 同一行的量与另一侧价格不受影响
    assert book["ask_prices"] is None
    assert book["bid_prices"] == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert book["bid_volumes"] == [5, 6, 7, 8, 9]


def test_get_book_drops_row_without_volumes_or_timestamp(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.return_value = {
        "600000.SH": {"ask_volumes": "bad", "bid_volumes": [1], "timestamp": 1},
        "600001.SH": {"ask_volumes": [1], "bid_volumes": [1]},
    }
    _configure_custom_depth(monkeypatch, provider)

    assert DepthService().get_book(["600000.SH", "600001.SH"]) == {}


def test_get_book_without_capability_returns_empty_without_upstream_calls(monkeypatch):
    provider = SimpleNamespace(get_trade_flow=MagicMock())
    _configure_custom_depth(monkeypatch, provider)
    _forbid_tickflow(monkeypatch)
    service = DepthService()

    assert service._has_capability() is False
    assert service.get_book(["600000.SH"]) == {}
    provider.get_trade_flow.assert_not_called()


def test_get_book_provider_failure_is_fail_closed(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.side_effect = RuntimeError("自定义源超时")
    _configure_custom_depth(monkeypatch, provider)
    _forbid_tickflow(monkeypatch)

    assert DepthService().get_book(["600000.SH"]) == {}


def test_get_book_missing_trade_flow_protocol_only_drops_direction(monkeypatch):
    provider = SimpleNamespace(get_depth5=lambda symbols: dict(_FULL_ROW))
    _configure_custom_depth(monkeypatch, provider)

    book = DepthService().get_book(["600000.SH"])["600000.SH"]

    assert book["ask_volumes"] == [10, 20, None, None, None]
    assert "inside_volume" not in book and "outside_volume" not in book


def test_get_book_trade_flow_failure_only_drops_direction(monkeypatch):
    provider = MagicMock()
    provider.get_depth5.return_value = dict(_FULL_ROW)
    provider.get_trade_flow.side_effect = RuntimeError("成交方向超时")
    _configure_custom_depth(monkeypatch, provider)

    book = DepthService().get_book(["600000.SH"])["600000.SH"]

    assert book["bid_volumes"] == [30, 40, 50, 60, 70]
    assert "inside_volume" not in book and "outside_volume" not in book


def test_fetch_trade_flow_returns_empty_on_tickflow_route_without_client(monkeypatch):
    from app.services import preferences

    monkeypatch.setattr(preferences, "get_depth5_data_provider", lambda: "tickflow")
    _forbid_tickflow(monkeypatch)

    assert DepthService()._fetch_trade_flow(["600000.SH"]) == {}


def test_validate_trade_flow_keeps_only_requested_rows_with_a_side():
    data = {
        "600000.SH": {"inside_volume": 600, "outside_volume": 634},
        "600001.SH": {"inside_volume": None, "outside_volume": None},
        "600002.SH": {"inside_volume": 12},
        "600003.SH": "bad",
        "600004.SH": {"inside_volume": True, "outside_volume": 1},
    }

    assert _validate_trade_flow(data, ["600000.SH", "600001.SH", "600002.SH", "600003.SH"], "mairui") == {
        "600000.SH": {"inside_volume": 600.0, "outside_volume": 634.0},
        "600002.SH": {"inside_volume": 12.0, "outside_volume": None},
    }
    # 未请求的标的即便有数据也不返回 (600005.SH 不在 data 中)
    assert _validate_trade_flow(data, ["600005.SH"], "mairui") == {}
    # bool 不算数值: 只丢该侧, 不整行丢弃
    assert _validate_trade_flow(data, ["600004.SH"], "mairui") == {
        "600004.SH": {"inside_volume": None, "outside_volume": 1.0},
    }
    assert _validate_trade_flow("bad", ["600000.SH"], "mairui") == {}


# ===== GET /api/quote/book =====


def test_book_api_rejects_illegal_symbol():
    client = _client(SimpleNamespace(has_capability=lambda: True, get_book=lambda s: {}))
    assert client.get("/api/quote/book?symbol=600000").status_code == 400
    assert client.get("/api/quote/book?symbol=600000.XX").status_code == 400


def test_book_api_returns_503_when_service_missing():
    assert _client(None).get("/api/quote/book?symbol=600000.SH").status_code == 503


def test_book_api_reports_unavailable_without_book():
    calls: list[list[str]] = []
    service = SimpleNamespace(
        has_capability=lambda: False,
        get_book=lambda symbols: calls.append(symbols) or {"600000.SH": {"bid_volumes": [1]}},
    )

    body = _client(service).get("/api/quote/book?symbol=600000.SH").json()

    assert body == {"symbol": "600000.SH", "available": False, "book": None}
    assert calls == []  # 无能力时不该触发取数


def test_book_api_normalizes_symbol_and_returns_book():
    book = {"ask_volumes": [1, 2, 3, 4, 5], "bid_volumes": [5, 4, 3, 2, 1], "timestamp": 1}
    service = SimpleNamespace(
        has_capability=lambda: True,
        get_book=lambda symbols: {symbols[0]: book},
    )

    body = _client(service).get("/api/quote/book?symbol=600000.sh").json()

    assert body == {"symbol": "600000.SH", "available": True, "book": book}


def test_book_api_returns_null_book_when_service_has_no_row():
    service = SimpleNamespace(has_capability=lambda: True, get_book=lambda symbols: {})

    body = _client(service).get("/api/quote/book?symbol=600000.SH").json()

    assert body == {"symbol": "600000.SH", "available": True, "book": None}
