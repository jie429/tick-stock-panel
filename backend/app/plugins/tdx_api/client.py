"""tdx-api HTTP 客户端。

对应外部项目 tdx-api-main(Go): 通达信协议转 HTTP, 统一信封
``{"code": 0, "message": "success", "data": ...}``, ``code != 0`` 即业务失败。

本模块只负责取数与信封解包, 不做单位换算与代码归一(在 provider 层完成)。
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://127.0.0.1:8080"
BASE_URL_ENV = "TDX_API_BASE_URL"
# 单标的日K全量(6000+ 根)响应约 1MB, 慢盘/慢网需要余量。
DEFAULT_TIMEOUT = 30.0
# 启动/重载时的可用性探测: 只看服务是否在监听, 不能拖慢后端启动。
PROBE_TIMEOUT = 3.0
# 上游 /api/batch-quote 单次上限 50 只, 超限整批返回失败。
BATCH_QUOTE_LIMIT = 50


def configured_base_url() -> str:
    """服务地址: secrets.json > .env/环境变量 > 默认本地 8080。"""
    from app import secrets_store

    value = secrets_store.get_env_backed_secret("tdx_api_base_url", BASE_URL_ENV)
    return (value or DEFAULT_BASE_URL).rstrip("/")


class TdxApiError(RuntimeError):
    """tdx-api 调用失败: 网络错误、HTTP 状态错误或业务 code != 0。"""


class TdxApiClient:
    """tdx-api REST 客户端(线程安全, 可被并发拉取共享)。"""

    def __init__(self, base_url: str | None = None, timeout: float = DEFAULT_TIMEOUT) -> None:
        self.base_url = (base_url or configured_base_url()).rstrip("/")
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    # ---------------------------------------------------------------- 请求基础

    @staticmethod
    def _unwrap(response: httpx.Response) -> Any:
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise TdxApiError(f"tdx-api 返回非 JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise TdxApiError("tdx-api 返回结构异常(非对象)")
        code = payload.get("code")
        try:
            ok = int(code) == 0
        except (TypeError, ValueError):
            ok = False
        if not ok:
            raise TdxApiError(str(payload.get("message") or f"tdx-api 返回 code={code}"))
        return payload.get("data")

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            response = self._client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise TdxApiError(f"tdx-api 请求失败 {path}: {exc}") from exc
        return self._unwrap(response)

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._request("GET", path, params=params)

    def post_json(self, path: str, payload: dict[str, Any]) -> Any:
        return self._request("POST", path, json=payload)

    # ---------------------------------------------------------------- 端点封装

    def server_status(self) -> dict:
        data = self.get("/api/server-status")
        return data if isinstance(data, dict) else {}

    def list_stock_codes(self) -> list[dict]:
        """全市场股票代码表(含名称与交易所),用于维表与实时行情代码池。"""
        data = self.get("/api/codes", {"exchange": "all"})
        rows = data.get("codes") if isinstance(data, dict) else None
        return [row for row in (rows or []) if isinstance(row, dict)]

    def list_etf(self) -> list[dict]:
        """全部 ETF 列表(含名称与交易所)。"""
        data = self.get("/api/etf")
        rows = data.get("list") if isinstance(data, dict) else None
        return [row for row in (rows or []) if isinstance(row, dict)]

    def batch_quote(self, codes: list[str]) -> list[dict]:
        """批量实时行情(五档+盘口)。codes 必须 <= BATCH_QUOTE_LIMIT 只。"""
        if not codes:
            return []
        data = self.post_json("/api/batch-quote", {"codes": list(codes)})
        return [row for row in (data or []) if isinstance(row, dict)]

    def kline_all_tdx(self, code: str, ktype: str = "day", limit: int | None = None) -> list[dict]:
        """股票/ETF 的原始(不复权)历史K线, 6 位裸码。"""
        params: dict[str, Any] = {"code": code, "type": ktype}
        if limit and limit > 0:
            params["limit"] = int(limit)
        data = self.get("/api/kline-all/tdx", params)
        rows = data.get("list") if isinstance(data, dict) else None
        return [row for row in (rows or []) if isinstance(row, dict)]

    def kline(self, code: str, ktype: str = "minute1") -> list[dict]:
        """实时窗口K线(分钟级为原始价), code 为 6 位裸码或带交易所前缀。"""
        data = self.get("/api/kline", {"code": code, "type": ktype})
        rows = data.get("List") if isinstance(data, dict) else None
        return [row for row in (rows or []) if isinstance(row, dict)]

    def index_all(self, code: str, ktype: str = "day", limit: int | None = None) -> list[dict]:
        """指数全量历史K线, code 形如 ``sh000001``。"""
        params: dict[str, Any] = {"code": code, "type": ktype}
        if limit and limit > 0:
            params["limit"] = int(limit)
        data = self.get("/api/index/all", params)
        rows = data.get("list") if isinstance(data, dict) else None
        return [row for row in (rows or []) if isinstance(row, dict)]

    def index(self, code: str, ktype: str = "day", limit: int | None = None) -> list[dict]:
        """指数窗口K线, code 形如 ``sh000001``。"""
        params: dict[str, Any] = {"code": code, "type": ktype}
        if limit and limit > 0:
            params["limit"] = int(limit)
        data = self.get("/api/index", params)
        rows = data.get("List") if isinstance(data, dict) else None
        return [row for row in (rows or []) if isinstance(row, dict)]
