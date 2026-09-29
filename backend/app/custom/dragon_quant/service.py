from __future__ import annotations

from contextlib import suppress
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from app.custom.dragon_quant.data import (
    DragonScanOptions,
    build_scan_input,
    load_industry_map,
)
from app.custom.dragon_quant.scoring import score_scan
from app.custom.dragon_quant.storage import DragonRecordStore

SOURCE = {
    "project": "gitBingxu/dragon-quant",
    "version": "0.5.1",
    "license": "MIT",
}


def _scan_store(data_dir: Path) -> DragonRecordStore:
    return DragonRecordStore(data_dir, "scans.json", "scan", limit=120)


def _summary(record: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "rows"}


def extension_status(repo, depth_service, data_dir: Path) -> dict[str, Any]:
    store = _scan_store(data_dir)
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
        "scan_count": len(store.list()),
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
    return _scan_store(data_dir).save(record)


def list_scans(data_dir: Path) -> list[dict[str, Any]]:
    return [_summary(item) for item in _scan_store(data_dir).list()]


def get_scan(data_dir: Path, record_id: str) -> dict[str, Any] | None:
    return _scan_store(data_dir).get(record_id)


def delete_scan(data_dir: Path, record_id: str) -> bool:
    return _scan_store(data_dir).delete(record_id)
