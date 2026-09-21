"""eltdx Provider 桌面发行版的静态打包契约。

桌面版 (PyInstaller onedir) 必须在发行包里固化免费 TCP 客户端与其元数据:
frozen 进程里既没有 pip/uv 可装依赖, 也无法沿普通 import 链发现由 plugin.yaml
字符串动态导入的插件目录。
"""

from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_desktop_extra_installs_eltdx_runtime_dependency() -> None:
    project = tomllib.loads((ROOT / "backend" / "pyproject.toml").read_text(encoding="utf-8"))
    desktop = project["project"]["optional-dependencies"]["desktop"]

    assert "eltdx>=3.2,<4" in desktop
    assert "packaging>=24" in desktop


def test_plugin_requirements_match_desktop_extra() -> None:
    """源码/容器安装走 requirements.txt, 桌面走 extra; 两者的版本区间必须一致。"""
    project = tomllib.loads((ROOT / "backend" / "pyproject.toml").read_text(encoding="utf-8"))
    requirements = (ROOT / "backend" / "app" / "plugins" / "eltdx" / "requirements.txt").read_text(
        encoding="utf-8"
    )
    specifier = next(line.strip() for line in requirements.splitlines() if line.startswith("eltdx"))

    assert specifier in project["project"]["optional-dependencies"]["desktop"]


def test_pyinstaller_collects_eltdx_plugin_and_distribution_metadata() -> None:
    spec = (ROOT / "packaging" / "tickflow.spec").read_text(encoding="utf-8")

    assert 'collect_all("eltdx")' in spec
    assert 'collect_submodules("app.plugins.eltdx")' in spec
    assert '"eltdx"' in spec
    assert '"app/plugins/eltdx"' in spec
