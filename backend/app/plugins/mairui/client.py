"""麦蕊智数 HTTP 客户端。

官方文档: https://www.mairuiapi.com/hsdata
鉴权 licence 位于 URL 路径中, 因此所有异常消息都必须先脱敏, 避免日志泄露证书。
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime

import httpx

BASE_URL = "https://api.mairuiapi.com"

_HTTP_LOG_SECRET_LOCK = threading.Lock()
_HTTP_LOG_SECRETS: set[str] = set()


class _CredentialRedactionFilter(logging.Filter):
    """隐藏 httpx/httpcore 日志中位于 URL 路径内的 licence。"""

    def filter(self, record: logging.LogRecord) -> bool:
        with _HTTP_LOG_SECRET_LOCK:
            secrets = tuple(_HTTP_LOG_SECRETS)
        if not secrets:
            return True
        message = record.getMessage()
        redacted = message
        for secret in secrets:
            redacted = redacted.replace(secret, "<redacted>")
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


_HTTP_LOG_FILTER = _CredentialRedactionFilter()
for _logger_name in ("httpx", "httpcore"):
    _logger = logging.getLogger(_logger_name)
    if _HTTP_LOG_FILTER not in _logger.filters:
        _logger.addFilter(_HTTP_LOG_FILTER)


def _register_http_log_secret(secret: str) -> None:
    with _HTTP_LOG_SECRET_LOCK:
        _HTTP_LOG_SECRETS.add(secret)


class MairuiError(Exception):
    """麦蕊接口、鉴权或网络错误。"""


class MairuiClient:
    """线程安全的麦蕊 REST 客户端, 统一处理限频与 licence 脱敏。"""

    def __init__(
        self,
        licence: str,
        *,
        base_url: str = BASE_URL,
        timeout: float = 20.0,
        request_interval: float = 0.21,
    ) -> None:
        self._licence = str(licence or "").strip()
        if not self._licence:
            raise MairuiError("未配置 MAIRUI_LICENSE")
        _register_http_log_secret(self._licence)
        self._http = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "tick-stock-panel/mairui"},
        )
        self._request_interval = max(0.0, float(request_interval))
        self._rate_lock = threading.Lock()
        self._last_request = 0.0

    def close(self) -> None:
        self._http.close()

    def _redact(self, text: object) -> str:
        return str(text).replace(self._licence, "<redacted>")

    def _wait_for_slot(self) -> None:
        with self._rate_lock:
            wait = self._request_interval - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

    def _get(self, path: str, params: dict | None = None):
        self._wait_for_slot()
        try:
            response = self._http.get(path, params=params or {})
        except httpx.HTTPError as exc:
            raise MairuiError(f"网络请求失败: {self._redact(exc)}") from exc
        if response.status_code != 200:
            raise MairuiError(f"HTTP {response.status_code}: {self._redact(path)}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise MairuiError(f"响应不是 JSON: {self._redact(path)}") from exc
        if isinstance(payload, dict) and payload.get("code") not in (None, 0, "0"):
            message = payload.get("message") or payload.get("msg") or payload.get("error") or ""
            raise MairuiError(f"接口错误 code={payload.get('code')}: {self._redact(message)}")
        return payload

    def stock_list(self) -> list[dict]:
        payload = self._get(f"/hslt/list/{self._licence}")
        if not isinstance(payload, list):
            raise MairuiError("股票列表响应结构异常")
        return [row for row in payload if isinstance(row, dict)]

    def history(
        self,
        symbol: str,
        period: str,
        dividend: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict]:
        params: dict[str, str] = {}
        if start is not None:
            params["st"] = start.strftime("%Y%m%d%H%M%S" if period != "d" else "%Y%m%d")
        if end is not None:
            params["et"] = end.strftime("%Y%m%d%H%M%S" if period != "d" else "%Y%m%d")
        payload = self._get(
            f"/hsstock/history/{symbol}/{period}/{dividend}/{self._licence}",
            params,
        )
        if not isinstance(payload, list):
            raise MairuiError("历史行情响应结构异常")
        return [row for row in payload if isinstance(row, dict)]

    def realtime(self, codes: list[str]) -> list[dict]:
        if not codes:
            return []
        if len(codes) > 20:
            raise ValueError("麦蕊多股实时接口单次最多 20 只")
        payload = self._get(
            f"/hsrl/ssjy_more/{self._licence}",
            {"stock_codes": ",".join(codes)},
        )
        if not isinstance(payload, list):
            raise MairuiError("实时行情响应结构异常")
        return [row for row in payload if isinstance(row, dict)]

    def depth5(self, code: str) -> dict:
        payload = self._get(f"/hsstock/real/five/{code}/{self._licence}")
        if payload in (None, []):
            return {}
        if not isinstance(payload, dict):
            raise MairuiError("五档盘口响应结构异常")
        return payload

    def financial(self, endpoint: str, symbol: str) -> list[dict]:
        payload = self._get(
            f"/hsstock/financial/{endpoint}/{symbol}/{self._licence}",
        )
        if not isinstance(payload, list):
            raise MairuiError("财务数据响应结构异常")
        return [row for row in payload if isinstance(row, dict)]
