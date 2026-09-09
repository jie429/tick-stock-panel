"""tdx-mcp 通达信 TCP 内置数据源插件。"""

from app.plugins.tdx_mcp.provider import TdxMcpProvider

PROVIDER_NAME = "tdx_mcp"

__all__ = ["PROVIDER_NAME", "TdxMcpProvider"]
