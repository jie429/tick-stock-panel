"""内置扩展数据预设 — 概念/行业启动时只创建配置, 等待用户手动获取 (#199)。

设计原则:
  - 扩展数据通用逻辑零改动 (ExtConfig / fetch_and_ingest / API / 前端均不动)
  - 仅在本模块做「接口结构 → 本地 schema」的转换
  - 「已存在则跳过」: 绝不覆盖用户已有数据, 老用户零影响
  - 拉取失败只记 warning, 不阻断启动 (保持「没数据也能跑」)

种子数据来源 (概念/行业各自独立配置):
  - 概念: https://shy313.com/api/plugins/market_flow/exports/ths-concepts
  - 行业: https://shy313.com/api/plugins/market_flow/exports/ths-industries
作者更新数据只需改接口上的 JSON, 用户下次拉取自动同步, 无需发版。


第三类预设是「数据源型」(无 HTTP 配方): 数据由 Provider 可选协议现场拉取, 见
_PROVIDER_PRESETS; 目前只有通达信概念板块 (eltdx 的 get_board_groups)。这类预设
不在启动时建表, 而是用户首次获取时才创建, 未装该数据源的用户不会多出一张空表。
接入点: app.main.lifespan → ensure_builtin_presets(store.data_dir)
"""
from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Callable
from pathlib import Path

from app.services.ext_data import (
    ExtConfig,
    ExtConfigStore,
    ExtField,
    PullConfig,
    rows_to_parquet,
)

logger = logging.getLogger(__name__)

# 种子数据源 (概念/行业各自独立配置, 作者维护)
_CONCEPT_DATA_URL = "https://shy313.com/api/plugins/market_flow/exports/ths-concepts"
_INDUSTRY_DATA_URL = "https://shy313.com/api/plugins/market_flow/exports/ths-industries"


# ---------------------------------------------------------------------------
# 预设定义: 字段结构 + 拉取配方
# ---------------------------------------------------------------------------

def _concept_preset() -> ExtConfig:
    """扩展概念 (ext_gn_ths)。

    接口结构: [{symbol, name, concepts: [概念1, 概念2, ...]}]
    本地 schema: 股票代码 / 股票简称 / 所属概念(分号拼接) / symbol / code
    """
    return ExtConfig(
        id="ext_gn_ths",
        label="扩展概念",
        mode="snapshot",
        fields=[
            ExtField("symbol", "string", "标的代码"),
            ExtField("code", "string", "代码"),
            ExtField("股票代码", "string", "股票代码"),
            ExtField("股票简称", "string", "股票简称"),
            ExtField("所属概念", "string", "所属概念"),
        ],
        description="同花顺概念分类 (启动仅创建配置, 在概念/行业页手动获取)",
        symbol_map={"type": "mapped", "col": "股票代码"},
        code_map={"type": "computed", "from": "symbol", "method": "strip_exchange"},
        pull=PullConfig(
            url=_CONCEPT_DATA_URL,
            method="GET",
            schedule_minutes=1440,
            # enabled=False: ensure_builtin_presets 承诺启动不拉取, PullScheduler
            # 只调度 enabled 配置; 手动获取走 fetch_preset 独立路径不受影响 (#199)
            enabled=False,
        ),
    )


def _industry_preset() -> ExtConfig:
    """扩展行业 (ext_hy_ths)。

    接口结构: [{symbol, name, industries: [一级行业, 二级行业, 三级行业]}]
    本地 schema: 股票代码 / 股票简称 / 所属同花顺行业(横杠拼接) / symbol / code
    """
    return ExtConfig(
        id="ext_hy_ths",
        label="扩展行业",
        mode="snapshot",
        fields=[
            ExtField("symbol", "string", "标的代码"),
            ExtField("code", "string", "代码"),
            ExtField("股票代码", "string", "股票代码"),
            ExtField("股票简称", "string", "股票简称"),
            ExtField("所属同花顺行业", "string", "所属同花顺行业"),
        ],
        description="同花顺行业分类 (启动仅创建配置, 在概念/行业页手动获取)",
        symbol_map={"type": "mapped", "col": "股票代码"},
        code_map={"type": "computed", "from": "symbol", "method": "strip_exchange"},
        pull=PullConfig(
            url=_INDUSTRY_DATA_URL,
            method="GET",
            schedule_minutes=1440,
            # 同概念 preset: 出厂禁用, 避免启动即网络拉取 (#199)
            enabled=False,
        ),
    )


# 数据源型预设: 数据来自 Provider 可选协议, 由 fetch_preset 分流 (不走 HTTP 配方)。
# 这是预设自己的来源声明, 与 _CONCEPT_DATA_URL 同类 —— 预设本就绑定一个确定来源,
# 不参与能力矩阵路由; 数据源缺失时报错而不是静默换源。
_PROVIDER_PRESETS: dict[str, tuple[str, str]] = {"ext_gn_tdx": ("eltdx", "concept")}


def _tdx_concept_preset() -> ExtConfig:
    """通达信概念 (ext_gn_tdx)。

    来源: Provider 可选协议 get_board_groups("concept") → [{symbol, groups}]
    本地 schema: 股票代码 / 所属概念(分号拼接) / symbol / code

    字段与同花顺概念预设对齐, 因此概念分析 / RPS 轮动 / 概念字段筛选等消费方
    无需区分两者的来源。
    """
    return ExtConfig(
        id="ext_gn_tdx",
        label="通达信概念",
        mode="snapshot",
        fields=[
            ExtField("symbol", "string", "标的代码"),
            ExtField("code", "string", "代码"),
            ExtField("股票代码", "string", "股票代码"),
            ExtField("所属概念", "string", "所属概念"),
        ],
        description="通达信概念板块成分 (eltdx 数据源; 在概念分析页「从通达信获取」时创建)",
        symbol_map={"type": "mapped", "col": "股票代码"},
        code_map={"type": "computed", "from": "symbol", "method": "strip_exchange"},
        pull=PullConfig(
            # 无 HTTP 配方: 取数走 Provider 协议; enabled=False 保证 PullScheduler
            # 不会把它当普通接口表调度 (#199), 手动获取走 fetch_preset 独立路径。
            url="",
            method="GET",
            schedule_minutes=1440,
            enabled=False,
        ),
    )


def _presets() -> list[ExtConfig]:
    return [_concept_preset(), _industry_preset(), _tdx_concept_preset()]


# ---------------------------------------------------------------------------
# 接口结构 → 本地 schema 转换 (仅预设使用)
# ---------------------------------------------------------------------------

def _symbol_to_code(symbol: str) -> str:
    """symbol (000001.SZ) → code (000001)。"""
    return symbol.split(".", 1)[0] if "." in symbol else symbol


def _dimension_label(value: object) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "null"} else text


def _flatten_concept_rows(raw_rows: list[dict]) -> list[dict]:
    """概念: concepts 数组 → 分号拼接成「所属概念」字符串。

    [{symbol, name, concepts:[...]}] → [{股票代码, 股票简称, 所属概念, symbol, code}]
    注: code 由 symbol 派生 (000001.SZ → 000001), 因 rows_to_parquet 不执行 code_map。
    """
    out: list[dict] = []
    for r in raw_rows:
        sym = (r.get("symbol") or "").strip()
        if not sym:
            continue
        concepts = r.get("concepts") or []
        labels = [label for c in concepts if (label := _dimension_label(c))]
        out.append({
            "股票代码": sym,
            "股票简称": r.get("name") or "",
            "所属概念": ";".join(labels),
            "symbol": sym,
            "code": _symbol_to_code(sym),
        })
    return out


def _flatten_industry_rows(raw_rows: list[dict]) -> list[dict]:
    """行业: industries 数组 → 横杠拼接成「所属同花顺行业」字符串。

    [{symbol, name, industries:[...]}] → [{股票代码, 股票简称, 所属同花顺行业, symbol, code}]
    """
    out: list[dict] = []
    for r in raw_rows:
        sym = (r.get("symbol") or "").strip()
        if not sym:
            continue
        inds = r.get("industries") or []
        labels = [label for i in inds if (label := _dimension_label(i))]
        out.append({
            "股票代码": sym,
            "股票简称": r.get("name") or "",
            "所属同花顺行业": "-".join(labels),
            "symbol": sym,
            "code": _symbol_to_code(sym),
        })
    return out


# ---------------------------------------------------------------------------
def _flatten_board_rows(rows: list[dict]) -> list[dict]:
    """板块分组: [{symbol, groups}] → 「所属概念」分号拼接 (同花顺概念同 schema)。"""
    out: list[dict] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        groups = [label for g in (row.get("groups") or []) if (label := _dimension_label(g))]
        if not symbol or not groups:
            continue
        out.append({
            "股票代码": symbol,
            "所属概念": ";".join(groups),
            "symbol": symbol,
            "code": _symbol_to_code(symbol),
        })
    return out


# 拉取执行 (复用 httpx, 不依赖 fetch_and_ingest 的 PullConfig 路径)
# ---------------------------------------------------------------------------

# 部分网络环境 (CDN/WAF/网关) 会把数组包成 {data: [...]}/{list: [...]}/{rows: [...]} 信封。
# 这里做一次兼容解包, 避免误判为「接口返回不是数组」。
_ENVELOPE_KEYS = ("data", "list", "rows", "result", "results")


async def _fetch_json(url: str) -> list[dict]:
    """请求 JSON 接口, 返回行数组。超时 30s, 失败抛异常由调用方兜底。

    兼容两种上游返回形态:
      - 直接是数组: [{...}, ...]          → 原样返回
      - 信封包裹: {data: [{...}]} 等       → 自动解包
    """
    import httpx

    # 延迟导入避免与 ext_pull 循环依赖; 出站请求带 tsp 标识头
    from app.services.ext_pull import outbound_headers

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, headers=outbound_headers())
        resp.raise_for_status()
        data = resp.json()

    if isinstance(data, list):
        return data

    # 信封解包: 在常见键里找第一个值为数组的
    if isinstance(data, dict):
        for key in _ENVELOPE_KEYS:
            inner = data.get(key)
            if isinstance(inner, list):
                return inner
        # 兜底: 遍历所有值, 取第一个数组
        for v in data.values():
            if isinstance(v, list):
                return v

    raise ValueError(
        f"接口返回不是数组 (type={type(data).__name__}), "
        f"响应预览: {str(data)[:200]}"
    )


async def _seed_one(config: ExtConfig, flatten, data_dir: Path) -> int:
    """拉取 + 转换 + 写入单个预设。返回写入行数。"""
    from datetime import date

    raw = await _fetch_json(config.pull.url)
    rows = flatten(raw)
    if not rows:
        raise ValueError(f"接口返回 0 行: {config.pull.url}")
    n = rows_to_parquet(rows, config, data_dir, snapshot_date=date.today())
    return n


# ---------------------------------------------------------------------------
# 对外入口
# ---------------------------------------------------------------------------

def get_preset(config_id: str) -> ExtConfig | None:
    """按 id 取预设定义 (供 API 层校验 id 合法性)。"""
    for c in _presets():
        if c.id == config_id:
            return c
    return None


async def ensure_builtin_presets(data_dir: Path) -> None:
    """启动时: 为缺失的预设创建 config.json (含 pull 配置), 但【不拉取数据】。

    设计: 数据获取改为用户在概念/行业页手动点「获取数据」触发, 避免启动时
    网络请求阻塞, 也避免「自动拉取」与「用户自主控制」的预期冲突。

    安全保证:
      - 已存在则完全跳过 (绝不覆盖用户数据)
      - 只写 config.json, 失败只记 warning 不阻断启动
    """
    store = ExtConfigStore(data_dir)

    for config in _presets():
        if config.id in _PROVIDER_PRESETS:
            # 数据源型预设不强加: 取数要现场连本机数据源 (实测约 40 秒), 且只在用户
            # 主动获取时才需要这张表 —— 首次 fetch_preset 会按需创建配置。
            continue
        existing = store.get(config.id)
        if existing is not None:
            # 用户已有此表 (老用户 / 自己重建过) → 一律不动
            continue
        try:
            store.upsert(config)
            logger.info("内置扩展表 %s 配置已就绪 (待用户手动获取数据)", config.id)
        except Exception as e:
            logger.warning("内置扩展表 %s 配置写入失败 (不影响启动): %s", config.id, e)


async def fetch_preset(config_id: str, data_dir: Path) -> int:
    """手动触发某个预设的数据拉取 (供 API 调用)。

    Raises:
        ValueError: config_id 不是内置预设
        Exception: 网络请求/解析/写入失败 (由 API 层转 HTTP 错误)
    """
    config = get_preset(config_id)
    if config is None:
        raise ValueError(f"未知的内置预设: {config_id}")

    provider_ref = _PROVIDER_PRESETS.get(config_id)
    # 数据源型预设先解析数据源与协议再建配置: 数据源缺失时不该留下一张永远填不满的
    # 空表 (概念分析页会按 id 顺序默认选中它)。
    fetch_board = _board_group_fetcher(config, provider_ref) if provider_ref is not None else None

    # 确保 config.json 存在 (用户可能从未启动过 ensure_builtin_presets)
    store = ExtConfigStore(data_dir)
    if store.get(config_id) is None:
        store.upsert(config)

    if fetch_board is not None:
        n = await _fetch_provider_preset(config, fetch_board, data_dir)
    else:
        flatten = _flatten_concept_rows if config_id == "ext_gn_ths" else _flatten_industry_rows
        n = await _seed_one(config, flatten, data_dir)
    logger.info("内置扩展表 %s 手动拉取成功: %d 行", config_id, n)
    return n


def _board_group_fetcher(config: ExtConfig, ref: tuple[str, str]) -> Callable[[], list[dict] | None]:
    """解析数据源型预设的取数函数 (数据源缺失/协议缺失都直接报错, 不静默换源)。"""
    from app.data_providers import custom as custom_sources

    provider_name, kind = ref
    if not custom_sources.is_custom_provider(provider_name):
        raise ValueError(f"数据源 {provider_name} 未安装或不可用, 无法获取{config.label}")
    fetch = getattr(custom_sources.get_provider(provider_name), "get_board_groups", None)
    if not callable(fetch):
        raise ValueError(f"数据源 {provider_name} 不提供板块分组协议")
    return lambda: fetch(kind)


async def _fetch_provider_preset(
    config: ExtConfig, fetch: Callable[[], list[dict] | None], data_dir: Path,
) -> int:
    """数据源型预设: 取数走 Provider 可选协议, 不走 HTTP 配方。

    与 HTTP 预设的区别只在取数一步: 结构转换仍收口在本模块, 落盘仍走 rows_to_parquet,
    因此 schema、合并去重与缓存失效行为与同花顺预设一致。
    """
    # 本机 TCP 批量拉取 (实测约 40 秒), 放线程里跑, 不阻塞事件循环
    rows = await asyncio.to_thread(fetch)
    if rows is None:
        raise ValueError(f"{config.label} 拉取失败: 板块分组接口软失败 (数据源不可达或上游变更)")
    flattened = _flatten_board_rows(rows)
    if not flattened:
        raise ValueError(f"{config.label} 拉取失败: 板块分组返回 0 条归属")
    return rows_to_parquet(flattened, config, data_dir)
