"""内置预设「数据源型」(ext_gn_tdx): 取数走 Provider 协议, 不走 HTTP 配方。

ext_gn_tdx 是第一个不靠 URL 配方取数的内置预设 —— 数据来自 eltdx 可选协议
get_board_groups("concept")。这里覆盖出厂形状、启动不建表, 以及 fetch_preset 的分流:
数据源缺失 / 协议缺失 / 软失败 / 0 条归属都必须直接报错, 端到端不留下一张填不满的空表
(概念分析页会按 id 顺序默认选中它)。
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import polars as pl
import pytest

from app.data_providers import custom as custom_sources
from app.services.ext_data import ExtConfigStore
from app.services.ext_presets import (
    _PROVIDER_PRESETS,
    _concept_preset,
    _flatten_board_rows,
    _presets,
    ensure_builtin_presets,
    fetch_preset,
    get_preset,
)


def _table_dir(data_dir: Path) -> Path:
    return data_dir / "ext_data" / "ext_gn_tdx"


class _FakeBoardProvider:
    """只实现可选协议 get_board_groups 的假数据源, 记录被请求的分类。"""

    def __init__(self, rows: list[dict] | None) -> None:
        self.rows = rows
        self.kinds: list[str] = []

    def get_board_groups(self, kind: str = "concept") -> list[dict] | None:
        self.kinds.append(kind)
        return self.rows


class _NoProtocolProvider:
    """老版本插件: 没有板块分组协议。"""


def _install_provider(monkeypatch: pytest.MonkeyPatch, provider: object) -> None:
    monkeypatch.setattr(custom_sources, "is_custom_provider", lambda _name: True)
    monkeypatch.setattr(custom_sources, "get_provider", lambda _name: provider)


def test_tdx_concept_preset_ships_without_http_recipe() -> None:
    """数据源型预设的出厂形状: 无 url / pull 禁用 / schema 与同花顺概念对齐。"""
    preset = get_preset("ext_gn_tdx")
    assert preset is not None and preset.pull is not None
    assert preset.pull.url == "", "取数走协议, 不该有 HTTP 配方"
    assert preset.pull.enabled is False, "被调度器扫到会触发一次无谓的 40 秒拉取"

    concept = _concept_preset()
    assert preset.mode == concept.mode == "snapshot"
    # 与同花顺概念的唯一差别: 通达信板块文件只有代码, 没有股票简称列 —— 概念分析页 /
    # 成分弹窗的股票名走实时快照 marketMap, 所以这里不补这一列。
    assert [f.name for f in preset.fields] == [
        f.name for f in concept.fields if f.name != "股票简称"
    ]


def test_provider_preset_ids_point_at_real_presets() -> None:
    """分流表里的 id 必须是 _presets() 里真实存在的预设 (防拼写漂移)。"""
    assert set(_PROVIDER_PRESETS) <= {c.id for c in _presets()}


def test_ensure_builtin_presets_skips_provider_presets(tmp_path: Path) -> None:
    """启动不为数据源型预设建表: 未装该数据源的用户不该多出一张空表。"""
    asyncio.run(ensure_builtin_presets(tmp_path))

    store = ExtConfigStore(tmp_path)
    assert store.get("ext_gn_tdx") is None
    for cid in ("ext_gn_ths", "ext_hy_ths"):
        assert store.get(cid) is not None, "原有预设的启动契约不能被影响 (#199)"
    assert not _table_dir(tmp_path).exists()


def test_flatten_board_rows_drops_empty_and_joins_groups() -> None:
    rows = _flatten_board_rows([
        {"symbol": "600000.SH", "groups": ["金融科技", "芯片"]},
        {"symbol": "000001.SZ", "groups": []},
        {"symbol": "", "groups": ["芯片"]},
        {"symbol": "  ", "groups": ["芯片"]},
        {"symbol": "300750.SZ", "groups": [None, "", " nan ", "锂电池"]},
    ])

    assert rows == [
        {
            "股票代码": "600000.SH",
            "所属概念": "金融科技;芯片",
            "symbol": "600000.SH",
            "code": "600000",
        },
        {
            "股票代码": "300750.SZ",
            "所属概念": "锂电池",
            "symbol": "300750.SZ",
            "code": "300750",
        },
    ]


def test_fetch_preset_needs_the_data_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """数据源没装: 直接报错, 且不在数据目录留下任何残留。"""
    monkeypatch.setattr(custom_sources, "is_custom_provider", lambda _name: False)

    with pytest.raises(ValueError, match="未安装"):
        asyncio.run(fetch_preset("ext_gn_tdx", tmp_path))

    assert not _table_dir(tmp_path).exists(), "报错前不该建目录/配置"


def test_fetch_preset_rejects_a_source_without_the_protocol(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_provider(monkeypatch, _NoProtocolProvider())

    with pytest.raises(ValueError, match="不提供板块分组协议"):
        asyncio.run(fetch_preset("ext_gn_tdx", tmp_path))

    assert not _table_dir(tmp_path).exists()


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (None, "软失败"),
        ([], "0 条归属"),
        ([{"symbol": "600000.SH", "groups": []}], "0 条归属"),
    ],
)
def test_fetch_preset_rejects_soft_failure_and_zero_rows(
    rows: list[dict] | None, message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """软失败与空结果都报错, 不写出一张空 parquet 覆盖用户已有数据。"""
    _install_provider(monkeypatch, _FakeBoardProvider(rows))

    with pytest.raises(ValueError, match=message):
        asyncio.run(fetch_preset("ext_gn_tdx", tmp_path))

    assert not (_table_dir(tmp_path) / "part.parquet").exists()


def test_fetch_preset_writes_board_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _FakeBoardProvider([
        {"symbol": "600000.SH", "groups": ["金融科技", "芯片"]},
        {"symbol": "000001.SZ", "groups": ["银行"]},
        {"symbol": "300750.SZ", "groups": []},
    ])
    _install_provider(monkeypatch, provider)

    written = asyncio.run(fetch_preset("ext_gn_tdx", tmp_path))

    assert written == 2, "无归属的行不该落盘"
    assert provider.kinds == ["concept"], "只支持概念板块, 不能悄悄换分类"
    df = pl.read_parquet(_table_dir(tmp_path) / "part.parquet")
    assert dict(zip(df["symbol"].to_list(), df["所属概念"].to_list(), strict=True)) == {
        "600000.SH": "金融科技;芯片",
        "000001.SZ": "银行",
    }
    assert ExtConfigStore(tmp_path).get("ext_gn_tdx") is not None, "首次获取须补建配置"


def test_fetch_preset_still_rejects_unknown_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="未知的内置预设"):
        asyncio.run(fetch_preset("ext_nope", tmp_path))

