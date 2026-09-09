"""多日分时 API 契约。"""

import asyncio
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest
from fastapi import HTTPException

from app.api import kline as kline_api


def _request(repo=None, capset=None):
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                repo=repo or MagicMock(),
                capabilities=capset or MagicMock(),
            )
        )
    )


def test_minute_range_returns_latest_sessions_with_previous_closes():
    repo = MagicMock()
    repo.resolve_asset_type.return_value = "stock"
    # _get_stock_info 走 instruments 内存缓存 (不再走 execute_one DuckDB 查询)
    repo.get_instruments.return_value = pl.DataFrame({
        "symbol": ["600000.SH"],
        "name": ["浦发银行"],
        "total_shares": [1.0],
        "float_shares": [1.0],
    })
    repo.get_minute_range.return_value = pl.DataFrame({
        "symbol": ["600000.SH"] * 3,
        "datetime": [
            datetime(2026, 8, 5, 1, 30),
            datetime(2026, 8, 6, 1, 30),
            datetime(2026, 8, 7, 1, 30),
        ],
        "open": [10.0, 11.0, 12.0],
        "high": [10.2, 11.2, 12.2],
        "low": [9.8, 10.8, 11.8],
        "close": [10.1, 11.1, 12.1],
        "volume": [100.0, 110.0, 120.0],
        "amount": [101_000.0, 122_100.0, 145_200.0],
    })
    repo.get_daily_asset.return_value = pl.DataFrame({
        "date": [
            date(2026, 8, 4),
            date(2026, 8, 5),
            date(2026, 8, 6),
            date(2026, 8, 7),
        ],
        "close": [9.9, 10.1, 11.1, 12.1],
    })

    result = kline_api.get_minute_range(_request(repo), "600000.SH", 2)

    assert result["name"] == "浦发银行"
    assert result["requested_days"] == 2
    assert result["source"] == "local"
    assert [session["date"] for session in result["sessions"]] == [
        "2026-08-06",
        "2026-08-07",
    ]
    assert [session["prev_close"] for session in result["sessions"]] == [
        10.1,
        11.1,
    ]
    assert result["sessions"][0]["rows"][0]["close"] == 11.1


def test_minute_range_does_not_read_stock_store_for_index():
    repo = MagicMock()
    repo.resolve_asset_type.return_value = "index"
    repo.get_instruments_asset.return_value = pl.DataFrame()

    result = kline_api.get_minute_range(_request(repo), "000001.SH", 10)

    assert result["asset_type"] == "index"
    assert result["sessions"] == []
    repo.get_minute_range.assert_not_called()


def test_sync_minute_single_uses_requested_days(monkeypatch):
    repo = MagicMock()
    repo.resolve_asset_type.return_value = "stock"
    capset = MagicMock()
    sync = MagicMock(return_value=2400)
    refresh = MagicMock()
    monkeypatch.setattr(kline_api, "_minute_allowed", lambda _: True)
    monkeypatch.setattr(kline_api.kline_sync, "sync_and_persist_minute", sync)
    monkeypatch.setattr("app.jobs.daily_pipeline._refresh_single_view", refresh)

    result = asyncio.run(kline_api.sync_minute_single(
        _request(repo, capset),
        {"symbol": "600000.SH", "days": 10},
    ))

    assert result["rows"] == 2400
    sync.assert_called_once_with(
        ["600000.SH"], repo, capset, days=10, force_full_days=True, asset_type="stock",
    )
    refresh.assert_called_once_with(repo, "kline_minute")


def test_sync_minute_single_persists_etf_to_etf_store(monkeypatch):
    repo = MagicMock()
    repo.resolve_asset_type.return_value = "etf"
    capset = MagicMock()
    sync = MagicMock(return_value=2400)
    refresh = MagicMock()
    monkeypatch.setattr(kline_api, "_minute_allowed", lambda _: True)
    monkeypatch.setattr(kline_api.kline_sync, "sync_and_persist_minute", sync)
    monkeypatch.setattr("app.jobs.daily_pipeline._refresh_single_view", refresh)

    result = asyncio.run(kline_api.sync_minute_single(
        _request(repo, capset),
        {"symbol": "510300.SH", "days": 10},
    ))

    assert result["rows"] == 2400
    sync.assert_called_once_with(
        ["510300.SH"], repo, capset, days=10, force_full_days=True, asset_type="etf",
    )
    refresh.assert_called_once_with(repo, "kline_etf_minute")


def test_sync_minute_single_maps_source_isolated_failure_to_502(monkeypatch):
    repo = MagicMock()
    repo.resolve_asset_type.return_value = "stock"
    monkeypatch.setattr(kline_api, "_minute_allowed", lambda _: True)
    monkeypatch.setattr(
        kline_api.kline_sync,
        "sync_and_persist_minute",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(kline_api.kline_sync.MinuteProviderError("TDX TCP down")),
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(kline_api.sync_minute_single(
            _request(repo, MagicMock()),
            {"symbol": "600000.SH"},
        ))

    assert exc_info.value.status_code == 502
    assert "分钟数据源拉取失败" in str(exc_info.value.detail)


def test_clear_minute_removes_stock_and_etf_stores(monkeypatch, tmp_path):
    repo = MagicMock()
    repo.store.data_dir = tmp_path
    repo.execute_one.side_effect = lambda sql: (3,) if "kline_minute" in sql and "etf" not in sql else (2,)
    request = _request(repo, MagicMock())
    request.method = "POST"

    async def _json():
        return {"confirm": True}

    request.json = _json
    (tmp_path / "kline_minute").mkdir()
    (tmp_path / "kline_etf_minute").mkdir()
    refresh = MagicMock()
    monkeypatch.setattr("app.jobs.daily_pipeline._refresh_single_view", refresh)
    monkeypatch.setattr("app.api.data.invalidate_storage_cache", lambda: None)

    result = asyncio.run(kline_api.clear_minute(request))

    assert result == {"status": "ok", "removed": 5}
    assert not (tmp_path / "kline_minute").exists()
    assert not (tmp_path / "kline_etf_minute").exists()
    assert refresh.call_args_list == [
        ((repo, "kline_minute"), {}),
        ((repo, "kline_etf_minute"), {}),
    ]


def test_sync_minute_single_rejects_invalid_days():
    with pytest.raises(HTTPException, match="days 必须在 1 到 30 之间"):
        asyncio.run(kline_api.sync_minute_single(
            _request(),
            {"symbol": "600000.SH", "days": 0},
        ))
