from __future__ import annotations

import logging
from datetime import date, datetime

import polars as pl
import pytest

from app.plugins.mairui import client as mc
from app.plugins.mairui import provider as mp
from app.plugins.mairui.provider import MairuiProvider


class _FakeClient:
    def __init__(self) -> None:
        self.closed = False
        self.realtime_calls: list[list[str]] = []
        self.depth_rows: dict[str, dict] = {}
        self.financial_rows: dict[tuple[str, str], list[dict]] = {}
        self.daily_rows: dict[str, list[dict] | Exception] = {}

    def close(self) -> None:
        self.closed = True

    def stock_list(self) -> list[dict]:
        return [
            {"dm": "600000", "mc": "浦发银行", "jys": "sh"},
            # 官方文档示例是裸代码, 2026-09 实际响应已变为带后缀; 两种都兼容。
            {"dm": "000001.SZ", "mc": "平安银行", "jys": "SZ"},
        ]

    def history(self, symbol, period, dividend, *, start=None, end=None):
        assert period == "d"
        assert dividend == "n"
        value = self.daily_rows.get(symbol, [])
        if isinstance(value, Exception):
            raise value
        return value

    def realtime(self, codes: list[str]) -> list[dict]:
        self.realtime_calls.append(list(codes))
        return [
            {
                "dm": code,
                "p": 10.2,
                "yc": 10.0,
                "o": 10.1,
                "h": 10.3,
                "l": 9.9,
                "v": 12_345,
                "cje": 1_234_500.0,
                "pc": 2.0,
                "ud": 0.2,
                "zf": 4.0,
                "tr": 1.5,
                "t": "2026-09-11 10:00:00",
            }
            for code in codes
        ]

    def depth5(self, code: str) -> dict:
        return self.depth_rows.get(code, {})

    def financial(self, endpoint: str, symbol: str) -> list[dict]:
        return self.financial_rows.get((endpoint, symbol), [])


def _provider_with(monkeypatch, fake: _FakeClient) -> MairuiProvider:
    monkeypatch.setattr(mp, "get_license", lambda: "test-licence")
    monkeypatch.setattr(mp, "MairuiClient", lambda **_kwargs: fake)
    return MairuiProvider()


def test_symbol_conversion_requires_supported_exchange():
    assert mp._normalize_symbol("600000.sh") == "600000.SH"
    assert mp._bare_code("000001.SZ") == "000001"
    with pytest.raises(ValueError, match="交易所后缀"):
        mp._normalize_symbol("600000")
    with pytest.raises(ValueError, match="不支持"):
        mp._normalize_symbol("920001.BJ")


def test_instruments_map_stock_list_without_fabricating_metadata(monkeypatch):
    provider = _provider_with(monkeypatch, _FakeClient())
    rows = provider.get_instruments("stock")
    assert rows == [
        {
            "symbol": "600000.SH",
            "name": "浦发银行",
            "code": "600000",
            "exchange": "SH",
            "region": "CN",
            "type": "stock",
            "ext": {},
        },
        {
            "symbol": "000001.SZ",
            "name": "平安银行",
            "code": "000001",
            "exchange": "SZ",
            "region": "CN",
            "type": "stock",
            "ext": {},
        },
    ]
    assert provider.get_instruments("etf") == []


def test_daily_maps_raw_prices_and_keeps_volume_in_lots(monkeypatch):
    fake = _FakeClient()
    fake.daily_rows["600000.SH"] = [
        {"t": "2026-09-11", "o": 10, "h": 10.5, "l": 9.8, "c": 10.2,
         "v": 179_522, "a": 1_792_200.0, "sf": 0},
        {"t": "2026-09-10", "o": 0, "h": 0, "l": 0, "c": 10.0,
         "v": 0, "a": 0, "sf": 1},
    ]
    provider = _provider_with(monkeypatch, fake)

    df = provider.get_daily(
        ["600000.SH"], datetime(2026, 9, 1), datetime(2026, 9, 12)
    )

    assert df.columns == ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    assert df.schema["date"] == pl.Date
    assert df.to_dicts() == [{
        "symbol": "600000.SH", "date": date(2026, 9, 11),
        "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2,
        "volume": 179_522.0, "amount": 1_792_200.0,
    }]


def test_daily_partial_failure_reports_symbol_and_keeps_success(monkeypatch):
    fake = _FakeClient()
    fake.daily_rows["600000.SH"] = [
        {"t": "2026-09-11", "o": 10, "h": 10.5, "l": 9.8, "c": 10.2,
         "v": 100, "a": 100_000, "sf": 0},
    ]
    fake.daily_rows["000001.SZ"] = mc.MairuiError("timeout")
    provider = _provider_with(monkeypatch, fake)
    failed: list[str] = []

    df = provider.get_daily(["600000.SH", "000001.SZ"], None, None, failed_out=failed)

    assert df["symbol"].to_list() == ["600000.SH"]
    assert failed == ["000001.SZ"]


def test_realtime_batches_twenty_and_normalizes_percentages(monkeypatch):
    fake = _FakeClient()
    provider = _provider_with(monkeypatch, fake)
    rows = provider.get_realtime()

    assert fake.realtime_calls == [["600000", "000001"]]
    first = rows[0]
    assert first["symbol"] == "600000.SH"
    assert first["volume"] == 12_345.0
    assert first["amount"] == 1_234_500.0
    assert first["change_pct"] == pytest.approx(0.02)
    assert first["amplitude"] == pytest.approx(0.04)
    assert first["turnover_rate"] == pytest.approx(0.015)
    assert first["timestamp"] == 1789092000000


def test_realtime_is_all_or_nothing_on_batch_error(monkeypatch):
    class BadRealtime(_FakeClient):
        def realtime(self, codes):
            raise mc.MairuiError("rate limited")

    provider = _provider_with(monkeypatch, BadRealtime())
    assert provider.get_realtime() == []


def test_depth5_maps_lots_and_preserves_missing_levels(monkeypatch):
    fake = _FakeClient()
    fake.depth_rows["600000"] = {
        "ps": [10.3, 10.4], "pb": [10.2],
        "vs": [300, 0], "vb": [200], "t": "2026-09-11 10:00:00",
    }
    provider = _provider_with(monkeypatch, fake)

    row = provider.get_depth5(["600000.SH"])["600000.SH"]

    assert row["ask_volumes"] == [300.0, 0.0, None, None, None]
    assert row["bid_volumes"] == [200.0, None, None, None, None]
    assert row["timestamp"] == 1789092000000


def test_financial_tables_map_canonical_fields_and_announcement_date(monkeypatch):
    fake = _FakeClient()
    fake.financial_rows[("pershareindex", "600000.SH")] = [{
        "jzrq": "20260630", "plrq": "20260815", "jqjzcsyl": 8.2,
        "tbzzcsyl": 0.9, "xsmlv": 42.0, "jlv": 19.0, "zcfzl": 40.0,
        "zyyrsrzz": 10.0, "gsmgsyzzdjlrzz": 12.0, "xsxjlyysr": 80.0,
        "chzzl": 3.0, "mgjzc": 5.2, "jbmgsy": 0.8,
    }]
    fake.financial_rows[("capital", "600000.SH")] = [{
        "bdrq": "20260630", "plrq": "20260815",
        "zgb": 2_000_000_000, "ysltag": 1_500_000_000, "xsltgf": 500_000_000,
    }]
    provider = _provider_with(monkeypatch, fake)

    metrics = provider.get_financials("metrics", ["600000.SH"], latest_only=False).to_dicts()[0]
    assert metrics["period_end"] == "2026-06-30"
    assert metrics["announce_date"] == "2026-08-15"
    assert metrics["roe"] == pytest.approx(8.2)
    assert metrics["debt_to_asset_ratio"] == pytest.approx(40.0)
    assert metrics["bps"] == pytest.approx(5.2)

    shares = provider.get_financials("shares", ["600000.SH"], latest_only=False).to_dicts()[0]
    assert shares == {
        "symbol": "600000.SH", "period_end": "2026-06-30",
        "announce_date": "2026-08-15", "total_shares": 2_000_000_000.0,
        "float_shares": 1_500_000_000.0, "restricted_shares": 500_000_000.0,
        "zgb": 2_000_000_000.0, "ysltag": 1_500_000_000.0,
        "xsltgf": 500_000_000.0,
    }


def test_datasets_declare_only_usable_project_contracts():
    datasets = MairuiProvider().config.datasets
    assert set(datasets) == {"daily", "realtime", "depth5", "financial"}
    assert "minute" not in datasets
    assert "full_minute" not in datasets
    assert "adj_factor" not in datasets


def test_availability_and_probe_use_local_secret(monkeypatch):
    monkeypatch.setattr(mp, "get_license", lambda: "")
    ok, reason = mp.availability()
    assert ok is False and mp.API_KEY_ENV in reason

    class ProbeClient(_FakeClient):
        pass

    monkeypatch.setattr(mp, "MairuiClient", lambda **_kwargs: ProbeClient())
    assert mp.probe_api_key("valid") == (True, "ok")
    assert mp.probe_api_key("")[0] is False


def test_http_transport_logs_redact_path_credential(caplog):
    licence = "test-licence-must-not-appear"
    client = mc.MairuiClient(licence, request_interval=0)
    try:
        with caplog.at_level(logging.INFO, logger="httpx"):
            logging.getLogger("httpx").info(
                "HTTP Request: GET https://api.mairuiapi.com/path/%s",
                licence,
            )
        assert licence not in caplog.text
        assert "<redacted>" in caplog.text
    finally:
        client.close()
