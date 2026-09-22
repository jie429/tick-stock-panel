"""个股盘口 API — 五档买卖盘 (价 + 量) 与成交方向 (内盘/外盘)。

取数与归一都在 `DepthService` (与连板梯队封单走同一条 depth5 能力路由和限速),
这里只做参数校验与响应映射。单只标的按需拉取, 不落盘、不进入盘中轮询热路径。
"""
from __future__ import annotations

import logging
import re

from fastapi import APIRouter, HTTPException, Query, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/quote", tags=["quotes"])

# 与全项目 symbol 口径一致: 6 位代码 + 交易所后缀 (不得让入参直接拼进下游请求)
_SYMBOL_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")


@router.get("/book")
def get_book(request: Request, symbol: str = Query(..., min_length=1, max_length=16)) -> dict:
    """单只标的的五档盘口 + 成交方向。

    响应 `book` 为 `null` 表示当前拿不到盘口 (非交易时段/停牌/数据源失败/未配置能力),
    调用方按缺失展示, 不用 0 或缺档填充; `available` 说明能力路由是否已配好, 便于
    前端区分「去配置数据源」与「数据源暂无这份盘口」。
    """
    code = str(symbol or "").strip().upper()
    if not _SYMBOL_RE.match(code):
        raise HTTPException(400, f"非法标的代码: {symbol!r} (需 6 位代码 + .SH/.SZ/.BJ)")

    depth_service = getattr(request.app.state, "depth_service", None)
    if depth_service is None:
        raise HTTPException(503, "五档盘口服务未初始化")

    available = depth_service.has_capability()
    book = depth_service.get_book([code]).get(code) if available else None
    return {"symbol": code, "available": available, "book": book}

