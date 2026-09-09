"""TDX Provider 桌面发行版的静态打包契约。"""
from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_desktop_extra_installs_tdx_runtime_dependencies() -> None:
    project = tomllib.loads((ROOT / "backend" / "pyproject.toml").read_text(encoding="utf-8"))
    desktop = project["project"]["optional-dependencies"]["desktop"]

    assert "eltdx>=0.5.0,<1.0" in desktop
    assert "packaging>=24" in desktop


def test_pyinstaller_collects_tdx_plugin_and_distribution_metadata() -> None:
    spec = (ROOT / "packaging" / "tickflow.spec").read_text(encoding="utf-8")

    assert 'collect_all("eltdx")' in spec
    assert 'collect_submodules("app.plugins.tdx_mcp")' in spec
    assert '"eltdx"' in spec
    assert '"app/plugins/tdx_mcp"' in spec
