"""异动监控 API — 竞价/盘中/偏移三类异动。

- /auction-scan: 全市场竞价扫描 (09:25 竞价终态快照 + 竞价量比, 可选插件协议)
- /intraday: 盘中量价信号聚合 (enriched 当日信号列, 零新增采集)
- /overview: 偏移异动边缘总览 (交易所异动规则口径的接近度)
"""
from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.services.abnormal_moves import build_intraday, build_overview
from app.services.auction_scan import get_auction_scan

router = APIRouter(prefix="/api/abnormal", tags=["abnormal"])


@router.get("/intraday")
def abnormal_intraday(
    request: Request,
    limit: int = Query(500, ge=1, le=2000),
):
    """盘中异动: 涨停/炸板/跌停翘板/跌停/新高/新低/放量 信号命中行。"""
    repo = request.app.state.repo
    return build_intraday(repo, limit=limit)


@router.get("/overview")
def abnormal_overview(
    request: Request,
    min_closeness: float = Query(0.5, ge=0.0, le=1.0),
    limit: int = Query(200, ge=1, le=1000),
):
    """异动边缘总览: 规则表 + 各窗口实时偏离 + 接近度排序。

    min_closeness: 0.5=观察 / 0.7=边缘 / 1.0=已触发。
    """
    repo = request.app.state.repo
    quote_service = getattr(request.app.state, "quote_service", None)
    return build_overview(repo, quote_service, min_closeness=min_closeness, limit=limit)

@router.get("/auction-scan")
def abnormal_auction_scan(
    request: Request,
    min_open_pct: float = Query(5.0, ge=0.0, le=30.0, description="开盘涨幅门槛 (百分数)"),
    min_ratio: float = Query(10.0, ge=0.0, le=1000.0, description="竞价量比门槛 (倍)"),
    limit: int = Query(200, ge=1, le=1000),
    refresh: bool = Query(False, description="忽略进程内缓存, 强制重扫"),
):
    """全市场竞价扫描: 09:25 集合竞价终态快照 + 竞价量比 (可选插件协议)。

    数据源 = 「实时行情」路由源的可选协议 get_market_auction_snapshot (eltdx 支持);
    未实现该协议 → state=source_unavailable, 竞价未结束且无历史快照 → state=not_ready。
    竞价量比基线取上一份落盘快照, 无基线时该列为 null (按日落盘, 自启用之日起积累)。
    """
    repo = request.app.state.repo
    return get_auction_scan(
        repo.store.data_dir,
        repo,
        min_open_pct=min_open_pct,
        min_ratio=min_ratio,
        limit=limit,
        refresh=refresh,
    )
