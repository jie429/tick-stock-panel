from __future__ import annotations

from contextlib import suppress
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from app.custom.dragon_quant.account import DragonAccountConfig, run_account_backtest
from app.custom.dragon_quant.data import (
    DragonScanOptions,
    build_scan_input,
    load_account_market_data,
    load_industry_map,
)
from app.custom.dragon_quant.scoring import score_scan
from app.custom.dragon_quant.storage import DragonRecordStore

SOURCE = {
    "project": "gitBingxu/dragon-quant",
    "version": "0.5.1",
    "license": "MIT",
}


def _stores(data_dir: Path) -> tuple[DragonRecordStore, DragonRecordStore]:
    return (
        DragonRecordStore(data_dir, "scans.json", "scan", limit=120),
        DragonRecordStore(data_dir, "backtests.json", "backtest", limit=60),
    )


def _summary(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in record.items()
        if key not in {"rows", "trades", "equity_curve", "open_positions"}
    }


def extension_status(repo, depth_service, data_dir: Path) -> dict[str, Any]:
    scan_store, backtest_store = _stores(data_dir)
    industry_map, industry_source = load_industry_map(data_dir)
    latest_date = None
    with suppress(Exception):
        _frame, latest_date = repo.get_enriched_latest()
    return {
        "status": "ready" if industry_map else "needs_industry_data",
        "source": SOURCE,
        "data_source": "tick-stock-panel",
        "industry_source": industry_source,
        "industry_symbols": len(industry_map),
        "latest_enriched_date": latest_date,
        "depth_service_available": depth_service is not None,
        "scan_count": len(scan_store.list()),
        "backtest_count": len(backtest_store.list()),
        "adaptations": [
            "行业排名使用扩展行业映射的成分股等权涨跌幅",
            "候选与行业样本分钟线本地优先, 缺失时通过当前分钟 Provider 按需补取",
            "市场分钟基准使用上证指数 000001.SH, 不依赖全市场分钟能力",
            "五档封单使用本项目 DepthService, 最新交易日缺失时自动补拉, 仍缺失则中性降级",
            "资金承接历史缺失时软降级为 50 分, 不参与硬门槛否决",
        ],
    }


def run_scan(
    repo,
    depth_service,
    data_dir: Path,
    *,
    as_of: date,
    options: DragonScanOptions,
) -> dict[str, Any]:
    scan_input = build_scan_input(repo, depth_service, data_dir, as_of, options)
    rows = score_scan(scan_input)
    critical = scan_input.data_quality.get("critical_missing")
    if critical is None:
        critical = scan_input.data_quality.get("missing", [])
    critical_missing = [str(value) for value in critical]
    complete = not critical_missing
    for row in rows:
        row["score_passed"] = bool(row["is_true_dragon"])
        if not complete:
            row["is_true_dragon"] = False
            row["rank"] = None
            quality_reason = f"关键数据不完整: {', '.join(critical_missing) or '未知缺项'}"
            original = row.get("reject_reason")
            row["reject_reason"] = f"{quality_reason}; {original}" if original else quality_reason
    rows = rows[: options.result_limit]
    record = {
        "as_of": as_of.isoformat(),
        "source": SOURCE,
        "data_source": "tick-stock-panel",
        "options": asdict(options),
        "data_quality": scan_input.data_quality,
        "summary": {
            "candidate_count": len(scan_input.candidates),
            "returned_count": len(rows),
            "score_passed_count": sum(bool(row["score_passed"]) for row in rows),
            "true_dragon_count": sum(bool(row["is_true_dragon"]) for row in rows),
        },
        "rows": rows,
    }
    scan_store, _backtest_store = _stores(data_dir)
    return scan_store.save(record)


def list_scans(data_dir: Path) -> list[dict[str, Any]]:
    scan_store, _backtest_store = _stores(data_dir)
    return [_summary(item) for item in scan_store.list()]


def get_scan(data_dir: Path, record_id: str) -> dict[str, Any] | None:
    scan_store, _backtest_store = _stores(data_dir)
    return scan_store.get(record_id)


def delete_scan(data_dir: Path, record_id: str) -> bool:
    scan_store, _backtest_store = _stores(data_dir)
    return scan_store.delete(record_id)


def _calendar(repo, start: date, end: date, prepend: int) -> list[date]:
    frame = repo.get_enriched_range(start - timedelta(days=max(60, prepend * 5)), end)
    if frame is None or frame.is_empty() or "date" not in frame.columns:
        return []
    all_days = frame["date"].unique().sort().to_list()
    earlier = [value for value in all_days if value < start][-prepend:]
    return earlier + [value for value in all_days if start <= value <= end]


def run_backtest(
    repo,
    depth_service,
    data_dir: Path,
    *,
    start: date,
    end: date,
    config: DragonAccountConfig,
    auto_scan_missing: bool = False,
    max_auto_scan_days: int = 20,
) -> dict[str, Any]:
    if start > end:
        raise ValueError("开始日期不能晚于结束日期")
    calendar = _calendar(repo, start, end, max(1, config.candidate_lookback_days))
    trading_days = [value for value in calendar if start <= value <= end]
    if not trading_days:
        raise ValueError("回测区间没有可用交易日数据")

    scan_store, backtest_store = _stores(data_dir)
    stored = scan_store.list()
    scans_by_day: dict[date, dict] = {}
    for item in stored:
        if item.get("as_of"):
            scans_by_day.setdefault(date.fromisoformat(str(item["as_of"])), item)
    required_days = calendar[:-1]
    missing_days = [value for value in required_days if value not in scans_by_day]
    warnings: list[str] = []
    if auto_scan_missing and missing_days:
        if len(missing_days) > max_auto_scan_days:
            raise ValueError(
                f"缺少 {len(missing_days)} 个扫描交易日, 自动补扫上限为 {max_auto_scan_days} 日"
            )
        options = DragonScanOptions()
        for scan_day in missing_days:
            record = run_scan(
                repo,
                depth_service,
                data_dir,
                as_of=scan_day,
                options=options,
            )
            scans_by_day[scan_day] = record
    elif missing_days:
        display = ", ".join(value.isoformat() for value in missing_days[:8])
        suffix = "..." if len(missing_days) > 8 else ""
        warnings.append(f"缺少 {len(missing_days)} 个交易日的五维扫描: {display}{suffix}")

    candidate_scans: dict[date, list[dict]] = {}
    used_scan_ids: list[str] = []
    for scan_day, record in scans_by_day.items():
        if scan_day not in required_days:
            continue
        rows = [row for row in record.get("rows", []) if row.get("is_true_dragon")]
        candidate_scans[scan_day] = rows
        used_scan_ids.append(str(record.get("id") or ""))
    symbols = sorted({row["symbol"] for rows in candidate_scans.values() for row in rows})
    actual_days, daily_by_symbol, minute_by_symbol_date = load_account_market_data(
        repo, symbols, start, end,
    )
    result = run_account_backtest(
        trading_days=actual_days,
        daily_by_symbol=daily_by_symbol,
        scans_by_date=candidate_scans,
        minute_by_symbol_date=minute_by_symbol_date,
        config=config,
    )
    record = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "source": SOURCE,
        "data_source": "tick-stock-panel",
        "warnings": warnings,
        "used_scan_ids": sorted(value for value in used_scan_ids if value),
        **result,
    }
    return backtest_store.save(record)


def list_backtests(data_dir: Path) -> list[dict[str, Any]]:
    _scan_store, backtest_store = _stores(data_dir)
    return [_summary(item) for item in backtest_store.list()]


def get_backtest(data_dir: Path, record_id: str) -> dict[str, Any] | None:
    _scan_store, backtest_store = _stores(data_dir)
    return backtest_store.get(record_id)


def delete_backtest(data_dir: Path, record_id: str) -> bool:
    _scan_store, backtest_store = _stores(data_dir)
    return backtest_store.delete(record_id)
