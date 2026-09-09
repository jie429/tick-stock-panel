"""标的维表同步服务。

盘前 9:10 调用 tf.exchanges.get_instruments("SH"/"SZ"/"BJ", type="stock")
获取全量标的元数据，flatten ext 字段，写入 instruments.parquet。

Starter+ 盘后可用 quotes.get(universes) 顺便补充 name。
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from app.tickflow.client import get_client

logger = logging.getLogger(__name__)

_EXCHANGES = ["SH", "SZ", "BJ"]


def _flatten_instruments(items: list[dict]) -> list[dict]:
    """把 SDK 返回的 Instrument 列表 flatten 成扁平行。"""
    rows = []
    for item in items:
        row = {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "code": item.get("code"),
            "exchange": item.get("exchange"),
            "region": item.get("region"),
            "type": item.get("type"),
        }
        ext = item.get("ext") or {}
        row["listing_date"] = ext.get("listing_date")
        row["total_shares"] = ext.get("total_shares")
        row["float_shares"] = ext.get("float_shares")
        row["tick_size"] = ext.get("tick_size")
        row["limit_up"] = ext.get("limit_up")
        row["limit_down"] = ext.get("limit_down")
        rows.append(row)
    return rows


def _provider_supports_instrument_asset(provider, asset_type: str) -> bool:
    """兼容旧 Provider: 未声明时仅支持股票维表。"""
    declared = getattr(provider, "instrument_asset_types", {"stock"})
    try:
        return asset_type in {str(value).lower() for value in declared}
    except TypeError:
        return asset_type == "stock"


def fetch_instruments_via_provider(asset_type: str = "stock") -> tuple[bool, list[dict]]:
    """从当前日K Provider 拉基础维表。

    返回 ``(handled, rows)``: ``handled=False`` 才允许调用方使用 TickFlow。普通历史
    Provider 维持 ``handled=True, rows=[]`` 的兼容语义; 声明来源隔离的 Provider 在
    不可用、失败或返回空维表时直接报错, 禁止把“全量同步”静默降级成旧标的池或 DEMO。
    """
    from app.services import preferences

    provider_name = preferences.get_daily_data_provider()
    if provider_name == "tickflow":
        return False, []
    from app.data_providers import custom as custom_sources

    source_isolated = custom_sources.plugin_requires_source_isolation(provider_name)
    if not custom_sources.is_custom_provider(provider_name):
        if source_isolated:
            raise RuntimeError(f"日K Provider {provider_name} 当前不可用, 无法同步标的维表")
        return False, []
    try:
        provider = custom_sources.get_provider(provider_name)
    except Exception as e:
        logger.warning("provider %s 解析失败: %s", provider_name, e)
        if source_isolated:
            raise RuntimeError(f"日K Provider {provider_name} 解析失败, 无法同步标的维表: {e}") from e
        return True, []
    get_instruments = getattr(provider, "get_instruments", None)
    if not callable(get_instruments) or not _provider_supports_instrument_asset(provider, asset_type):
        if source_isolated:
            raise RuntimeError(f"日K Provider {provider_name} 不支持 {asset_type} 标的维表")
        return False, []
    try:
        items = get_instruments(asset_type) or []
        rows = _flatten_instruments(items)
    except Exception as e:
        logger.warning("provider %s get_instruments(%s) 失败: %s", provider_name, asset_type, e)
        if source_isolated:
            raise RuntimeError(
                f"日K Provider {provider_name} 获取 {asset_type} 标的维表失败: {e}",
            ) from e
        return True, []
    if source_isolated and not rows:
        raise RuntimeError(f"日K Provider {provider_name} 未返回可用 {asset_type} 标的维表")
    logger.info("instruments via %s: %d %s", provider_name, len(rows), asset_type)
    return True, rows


def sync_instruments(data_dir: Path) -> int:
    """全量同步标的维表 → data/instruments/instruments.parquet。

    返回写入的行数。
    """
    handled, all_rows = fetch_instruments_via_provider("stock")
    if not handled:
        # 未命中非 tickflow provider → 走 tickflow 直连
        tf = get_client()
        all_rows = []
        for ex in _EXCHANGES:
            try:
                items = tf.exchanges.get_instruments(ex, instrument_type="stock")
                if items:
                    all_rows.extend(_flatten_instruments(items))
                    logger.info("instruments %s: %d stocks", ex, len(items))
            except Exception as e:
                logger.warning("get_instruments(%s) failed: %s", ex, e)

    if not all_rows:
        return 0

    df = pl.DataFrame(all_rows)
    df = df.with_columns(pl.lit(date.today()).alias("as_of"))

    out = data_dir / "instruments" / "instruments.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)

    logger.info("instruments synced: %d rows → %s", df.height, out)
    return df.height


def enrich_names_from_quotes(
    data_dir: Path,
    quotes_data: list[dict],
) -> int:
    """从 quotes 响应中提取 name，更新 instruments 维表（兜底补充）。

    盘后 quotes.get(universes) 返回的数据中包含 ext.name，
    用来补充 instruments 中可能缺失的 name。
    """
    if not quotes_data:
        return 0

    # 构建 symbol → name 映射
    name_map: dict[str, str] = {}
    for q in quotes_data:
        symbol = q.get("symbol", "")
        ext = q.get("ext") or {}
        name = ext.get("name") or q.get("name", "")
        if symbol and name:
            name_map[symbol] = name

    if not name_map:
        return 0

    inst_path = data_dir / "instruments" / "instruments.parquet"
    if not inst_path.exists():
        return 0

    df = pl.read_parquet(inst_path)

    # 只更新空 name 的行
    updates = pl.DataFrame({
        "symbol": list(name_map.keys()),
        "_new_name": list(name_map.values()),
    })
    df = df.join(updates, on="symbol", how="left")
    df = df.with_columns(
        pl.when(pl.col("name").is_null() | (pl.col("name") == ""))
        .then(pl.col("_new_name"))
        .otherwise(pl.col("name"))
        .alias("name"),
    ).drop("_new_name")

    df.write_parquet(inst_path)
    logger.info("instruments name enriched from quotes: %d names", len(name_map))
    return len(name_map)
