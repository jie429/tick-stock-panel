"""可选 eltdx 依赖的轻量边界。

项目后端不启动 eltdx 的 MCP / HTTP 服务, 直接复用其 ``TdxClient`` TCP 客户端。
``availability`` 只做包导入、版本范围与接口存在性检查, 不探测 TCP 主机 ——
插件扫描阶段不能因为测速或连接失败阻塞应用启动。
"""
from __future__ import annotations

from contextlib import suppress
from importlib.metadata import PackageNotFoundError, version

from packaging.version import InvalidVersion, Version

_ELTDX_MIN_VERSION = Version("3.2")
_ELTDX_MAX_VERSION = Version("4.0")
# 3.x 起 API 按命名空间拆分, 0.5.x 的扁平方法已全部移除; 逐项探测实际调用入口。
_REQUIRED_API = {
    "bars": ("get",),
    "quotes": ("get_snapshots",),
    "corporate": ("capital_changes",),
    "codes": ("all_a_shares", "all_etfs", "a_shares", "etfs", "indices"),
    "helpers": ("full_quotes",),
}


class EltdxBridgeError(RuntimeError):
    """eltdx 依赖不可用时的可读错误。"""


def _client_class():
    from eltdx import TdxClient

    return TdxClient


def _missing_api(client_cls) -> list[str]:
    """探测运行时真正会用到的入口。

    3.x 的子 API 在 ``__init__`` 里挂载到实例, 类属性为 None, 因此必须探测实例;
    构造本身不建连 (等待首次请求), 不会引入启动期网络阻塞。
    """
    try:
        client = client_cls(timeout=5.0, probe_hosts=False)
    except Exception as exc:
        return [f"构造客户端失败: {exc}"]
    try:
        missing: list[str] = []
        for namespace, methods in _REQUIRED_API.items():
            holder = getattr(client, namespace, None)
            if holder is None:
                missing.append(namespace)
                continue
            missing.extend(
                f"{namespace}.{method}"
                for method in methods
                if not callable(getattr(holder, method, None))
            )
        return missing
    finally:
        with suppress(Exception):
            client.close()


def availability() -> tuple[bool, str]:
    """供 plugin.yaml 的 check 使用; 不抛异常。"""
    try:
        client_cls = _client_class()
    except ImportError as exc:
        return False, f"缺少 eltdx 依赖: {exc}"
    try:
        installed_version = version("eltdx")
    except PackageNotFoundError:
        return False, "无法读取 eltdx 版本, 请重新安装兼容版本 (>=3.2,<4)"
    try:
        parsed_version = Version(installed_version)
    except InvalidVersion:
        return False, f"eltdx 版本不可识别: {installed_version!r}"
    if parsed_version.is_prerelease or not _ELTDX_MIN_VERSION <= parsed_version < _ELTDX_MAX_VERSION:
        return False, (
            f"eltdx {installed_version} 不兼容, 需要 >=3.2,<4"
            " (3.0 起 API 重写, 与 0.5.x 扁平接口不通用)"
        )
    missing = _missing_api(client_cls)
    if missing:
        return False, f"eltdx {installed_version} 缺少所需接口: {', '.join(missing)}"
    return True, f"ok (eltdx {installed_version})"


def create_client():
    """建立默认通达信 TCP 客户端。

    不接受来自设置页或环境变量的任意 host, 避免可选数据源变成任意 TCP 访问入口;
    eltdx 自带维护的默认行情服务器列表, 首次请求时才建立连接。
    """
    try:
        client_cls = _client_class()
    except ImportError as exc:
        raise EltdxBridgeError(f"缺少 eltdx 依赖: {exc}") from exc
    return client_cls(timeout=8.0, pool_size=2, probe_hosts=False)
