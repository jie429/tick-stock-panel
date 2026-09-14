"""tdx-api 通达信 HTTP 数据源插件。"""

from app.plugins.tdx_api.provider import TdxApiProvider

PROVIDER_NAME = "tdx_api"

__all__ = ["PROVIDER_NAME", "TdxApiProvider"]
