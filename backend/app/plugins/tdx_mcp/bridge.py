"""可选 eltdx 依赖的轻量边界。

这里不启动 MCP stdio 子进程; 后端直接复用 tdx-mcp 所依赖的 eltdx TCP 客户端。
availability 只验证包能否导入, 避免插件扫描阶段产生网络连接。
"""
from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from packaging.version import InvalidVersion, Version

_ELTDX_MIN_VERSION = Version("0.5.0")
_ELTDX_MAX_VERSION = Version("1.0")
_REQUIRED_CLIENT_METHODS = (
    "connect",
    "close",
    "get_kline",
    "get_kline_all",
    "get_xdxr",
    "get_quote",
    "get_a_share_codes_all",
    "get_index_codes_all",
    "get_etf_codes_all",
    "get_codes_all",
)


class TdxMcpBridgeError(RuntimeError):
    """eltdx 依赖不可用时的可读错误。"""


def _client_class():
    from eltdx import TdxClient

    return TdxClient


def availability() -> tuple[bool, str]:
    """检查 Python 依赖; 不探测 TCP 主机, 避免启动时阻塞。"""
    try:
        client_cls = _client_class()
    except ImportError as exc:
        return False, f"缺少 eltdx 依赖: {exc}"
    try:
        installed_version = version("eltdx")
    except PackageNotFoundError:
        return False, "无法读取 eltdx 版本, 请重新安装兼容版本 (>=0.5.0,<1.0)"
    try:
        parsed_version = Version(installed_version)
    except InvalidVersion:
        return False, f"eltdx 版本不可识别: {installed_version!r}"
    if parsed_version.is_prerelease or not _ELTDX_MIN_VERSION <= parsed_version < _ELTDX_MAX_VERSION:
        return False, (
            f"eltdx {installed_version} 不兼容, "
            "需要 >=0.5.0,<1.0"
        )
    missing = [name for name in _REQUIRED_CLIENT_METHODS if not callable(getattr(client_cls, name, None))]
    if missing:
        return False, f"eltdx {installed_version} 缺少所需接口: {', '.join(missing)}"
    return True, f"ok (eltdx {installed_version})"


def create_client():
    """建立默认通达信 TCP 客户端。

    不接受来自设置页或环境变量的任意 host, 避免可选数据源变成任意 TCP 访问入口。
    eltdx 自带维护的默认行情服务器列表, 并在首次请求时才建立连接。
    """
    try:
        client_cls = _client_class()
    except ImportError as exc:
        raise TdxMcpBridgeError(f"缺少 eltdx 依赖: {exc}") from exc
    return client_cls(timeout=8.0, pool_size=2, batch_size=80, probe_hosts=False)
