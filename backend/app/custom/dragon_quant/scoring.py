"""dragon-quant 五维评分的本地数据适配实现。

算法权重与门槛来自 MIT 项目 dragon-quant 0.5.1; 数据读取全部由当前项目负责。
"""
from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime

from app.custom.dragon_quant.models import DragonCandidate, DragonScanInput, MinuteCurve

DIM_WEIGHTS = {
    "drive": 0.30,
    "leadership": 0.25,
    "anti_drop": 0.15,
    "liquidity": 0.20,
    "absorption": 0.10,
}
DIM_FLOORS = {"drive": 40.0, "leadership": 40.0, "anti_drop": 35.0, "liquidity": 35.0}


def _clip(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _aligned(*curves: MinuteCurve) -> tuple[list[datetime], list[list[float | None]]]:
    axis = sorted({point for curve in curves for point in curve})
    output: list[list[float | None]] = []
    for curve in curves:
        last: float | None = None
        values: list[float | None] = []
        for point in axis:
            if point in curve:
                last = curve[point]
            values.append(last)
        output.append(values)
    return axis, output


def _desc_rank(value: float, sample: Iterable[float]) -> float:
    values = list(sample)
    if len(values) <= 1:
        return 0.0
    rank = sum(1 for item in values if item > value) + 1
    return (1.0 - rank / len(values)) * 100.0


def _early_seal(candidate: DragonCandidate, peers: list[DragonCandidate]) -> tuple[float, dict]:
    seal_times: dict[str, datetime] = {}
    for item in peers:
        if item.limit_up_price <= 0:
            continue
        threshold = item.limit_up_price * 0.999
        for point, gain in sorted(item.minute_curve.items()):
            if item.prev_raw_close > 0 and item.prev_raw_close * (1.0 + gain) >= threshold:
                seal_times[item.symbol] = point
                break
    if candidate.symbol not in seal_times:
        return 0.0, {"sealed": False, "pool_size": len(seal_times)}
    ordered = sorted(seal_times, key=seal_times.get)
    rank = ordered.index(candidate.symbol) + 1
    score = 100.0 if len(ordered) == 1 else (1.0 - rank / len(ordered)) * 100.0
    return score, {
        "sealed": True,
        "seal_time": seal_times[candidate.symbol].strftime("%H:%M"),
        "rank": rank,
        "pool_size": len(ordered),
    }


def _lead_sector(stock: MinuteCurve, industry: MinuteCurve) -> tuple[float, dict]:
    if not stock or not industry:
        return 40.0, {"score": 40.0, "degraded": True, "reason": "个股或行业分钟线缺失"}
    _axis, (stock_values, industry_values) = _aligned(stock, industry)
    window = 3
    follow = 3
    thrust = 0.03
    industry_follow = 0.003
    leads = 0
    followers = 0
    last_stock = -10
    for index in range(window, len(stock_values)):
        start = stock_values[index - window]
        end = stock_values[index]
        if start is None or end is None or end - start < thrust or index - last_stock <= window:
            continue
        last_stock = index
        base = industry_values[index - window]
        after = [v for v in industry_values[index - window + 1:index - window + follow + 1] if v is not None]
        before = [v for v in industry_values[max(0, index - window - follow):index - window] if v is not None]
        if base is None or not after:
            continue
        moved = max(after) - base >= industry_follow
        front_run = bool(before) and base - min(before) >= industry_follow
        if moved and not front_run:
            leads += 1
        elif moved:
            followers += 1
    last_industry = -10
    for index in range(window, len(industry_values)):
        start = industry_values[index - window]
        end = industry_values[index]
        if start is None or end is None or end - start < industry_follow or index - last_industry <= window:
            continue
        stock_base = stock_values[index - window]
        after = [v for v in stock_values[index - window + 1:index - window + follow + 1] if v is not None]
        if stock_base is not None and after and max(after) - stock_base >= thrust:
            followers += 1
            last_industry = index
    score = 0.0 if leads == 0 else _clip(
        70.0 + (leads - 1) * 10.0 - min(followers * 20.0, 100.0)
    )
    return score, {"score": round(score, 2), "n_lead": leads, "n_follow": followers}


def _voice(industry: str, scan: DragonScanInput) -> tuple[float, dict]:
    members = scan.industry_members.get(industry, ())
    values = [scan.stock_change_pct[symbol] for symbol in members if symbol in scan.stock_change_pct]
    if not values:
        return 0.0, {"score": 0.0, "degraded": True, "member_count": 0}
    limit_count = sum(value >= 0.099 for value in values)
    strong_count = sum(value >= 0.03 for value in values)
    limit_ratio = limit_count / len(values)
    strong_ratio = strong_count / len(values)
    score = _clip(limit_ratio / 0.10 * 100.0) * 0.6 + _clip(strong_ratio / 0.30 * 100.0) * 0.4
    return _clip(score), {
        "score": round(_clip(score), 2),
        "member_count": len(values),
        "limit_up_count": limit_count,
        "strong_count": strong_count,
        "limit_ratio": round(limit_ratio, 4),
        "strong_ratio": round(strong_ratio, 4),
    }


def _drive(candidate: DragonCandidate, peers: list[DragonCandidate], scan: DragonScanInput) -> dict:
    early, early_detail = _early_seal(candidate, peers)
    lead, lead_detail = _lead_sector(candidate.minute_curve, scan.industry_curves.get(candidate.industry, {}))
    voice, voice_detail = _voice(candidate.industry, scan)
    score = _clip(early * 0.40 + lead * 0.35 + voice * 0.25)
    return {
        "score": round(score, 2),
        "weight": DIM_WEIGHTS["drive"],
        "details": {
            "early": {
                "score": round(early, 2),
                "sealed_volume_lots": candidate.sealed_volume_lots,
                **early_detail,
            },
            "lead": lead_detail,
            "voice": voice_detail,
        },
    }


def _leadership(candidate: DragonCandidate, peers: list[DragonCandidate]) -> dict:
    max_boards = max((item.board_count for item in peers), default=candidate.board_count)
    board_score = _clip(100.0 - (max_boards - candidate.board_count) * 10.0)
    returns = [item.five_day_return_pct for item in peers]
    return_score = _desc_rank(candidate.five_day_return_pct, returns)
    rank = sum(1 for value in returns if value > candidate.five_day_return_pct) + 1
    score = _clip(board_score * 0.5 + return_score * 0.5)
    return {
        "score": round(score, 2),
        "weight": DIM_WEIGHTS["leadership"],
        "details": {
            "board_count": candidate.board_count,
            "industry_max_boards": max_boards,
            "board_score": round(board_score, 2),
            "five_day_return_pct": round(candidate.five_day_return_pct, 2),
            "five_day_rank_score": round(return_score, 2),
            "five_day_rank": rank,
            "five_day_peer_count": len(returns),
        },
    }


def _dip_segments(values: list[float | None]) -> list[tuple[int, int]]:
    falling = [False] * len(values)
    for index in range(3, len(values)):
        start, end = values[index - 3], values[index]
        if start is not None and end is not None and end - start < -0.005:
            for point in range(index - 3, index + 1):
                falling[point] = True
    result: list[tuple[int, int]] = []
    index = 0
    while index < len(values):
        if not falling[index]:
            index += 1
            continue
        end = index
        while end + 1 < len(values) and falling[end + 1]:
            end += 1
        candidates = [(point, values[point]) for point in range(index, end + 1) if values[point] is not None]
        if candidates:
            bottom = min(candidates, key=lambda pair: pair[1])[0]
            result.append((index, bottom))
        index = end + 1
    return result


def _anti_drop_against(base: MinuteCurve, stock: MinuteCurve) -> tuple[float, dict]:
    if not base or not stock:
        return 65.0, {"score": 65.0, "degraded": True}
    _axis, (base_values, stock_values) = _aligned(base, stock)
    segments = _dip_segments(base_values)
    if not segments:
        return 65.0, {"score": 65.0, "no_dip": True}
    weighted = 0.0
    denominator = 0.0
    for start, bottom in segments:
        values = (base_values[start], base_values[bottom], stock_values[start], stock_values[bottom])
        if any(value is None for value in values):
            continue
        base_drop = float(values[0]) - float(values[1])
        stock_drop = float(values[2]) - float(values[3])
        if base_drop <= 0:
            continue
        hold = _clip((1.0 - stock_drop / base_drop) * 100.0)
        weighted += hold * base_drop
        denominator += base_drop
    hold_score = weighted / denominator if denominator else 65.0
    deepest = max(segments, key=lambda pair: (base_values[pair[0]] or 0) - (base_values[pair[1]] or 0))
    bottom = deepest[1]
    left, right = max(0, bottom - 3), min(len(stock_values) - 1, bottom + 3)
    valid = [(index, stock_values[index]) for index in range(left, right + 1) if stock_values[index] is not None]
    stock_bottom = min(valid, key=lambda pair: pair[1])[0] if valid else bottom
    lead = _clip(bottom - stock_bottom, 0, 3) / 3.0 * 100.0
    base_start = base_values[bottom]
    stock_start = stock_values[bottom]
    base_after = [v for v in base_values[bottom:right + 1] if v is not None]
    stock_after = [v for v in stock_values[bottom:right + 1] if v is not None]
    base_rise = max(base_after) - base_start if base_after and base_start is not None else 0.0
    stock_rise = max(stock_after) - stock_start if stock_after and stock_start is not None else 0.0
    amplitude = _clip(stock_rise / max(base_rise, 1e-9), 0, 2) / 2.0 * 100.0
    rebound = lead * 0.6 + amplitude * 0.4
    score = _clip(hold_score * 0.6 + rebound * 0.4)
    return score, {
        "score": round(score, 2),
        "dip_segments": len(segments),
        "hold_score": round(hold_score, 2),
        "rebound_score": round(rebound, 2),
    }


def _anti_drop(candidate: DragonCandidate, scan: DragonScanInput) -> dict:
    market_score, market_detail = _anti_drop_against(scan.market_curve, candidate.minute_curve)
    industry_score, industry_detail = _anti_drop_against(
        scan.industry_curves.get(candidate.industry, {}), candidate.minute_curve,
    )
    score = _clip(market_score * 0.6 + industry_score * 0.4)
    return {
        "score": round(score, 2),
        "weight": DIM_WEIGHTS["anti_drop"],
        "details": {"market": market_detail, "industry": industry_detail},
    }


def _liquidity(candidate: DragonCandidate, peers: list[DragonCandidate]) -> dict:
    absolute = _clip(candidate.turnover_rate_pct / 15.0 * 100.0)
    relative = _desc_rank(candidate.turnover_rate_pct, (item.turnover_rate_pct for item in peers))
    turnover_score = absolute * 0.5 + relative * 0.5
    degraded = candidate.sealed_volume_lots is None
    if candidate.sealed_volume_lots is not None and candidate.volume_lots > 0:
        strength = candidate.sealed_volume_lots / candidate.volume_lots
        strength_score = _clip(strength / 0.3 * 100.0)
    else:
        strength = None
        strength_score = 60.0
    if candidate.open_count is None:
        stable_score = 60.0
        degraded = True
    elif candidate.open_count == 0:
        stable_score = 100.0
    elif candidate.open_count <= 2:
        stable_score = 60.0
    else:
        stable_score = 20.0
    seal_score = strength_score * 0.5 + stable_score * 0.5
    score = _clip(turnover_score * 0.5 + seal_score * 0.5)
    return {
        "score": round(score, 2),
        "weight": DIM_WEIGHTS["liquidity"],
        "details": {
            "turnover_rate_pct": round(candidate.turnover_rate_pct, 2),
            "sealed_volume_lots": candidate.sealed_volume_lots,
            "volume_lots": candidate.volume_lots,
            "seal_strength": round(strength, 4) if strength is not None else None,
            "open_count": candidate.open_count,
            "degraded": degraded,
            "absolute_score": round(absolute, 2),
            "relative_score": round(relative, 2),
            "turnover_score": round(turnover_score, 2),
            "strength_score": round(strength_score, 2),
            "stable_score": round(stable_score, 2),
            "seal_score": round(seal_score, 2),
        },
    }


def _absorption_events(industry: str, scan: DragonScanInput) -> list[dict]:
    """回看期内符合口径的承接窗口 (5 分钟桶), 带时间戳与同期回落板块。

    只负责「发现与标注」: 事件打分公式与降级口径保持原样, 最终维度分不变。
    """
    events: list[dict] = []
    for day, curves in scan.industry_history.items():
        target = curves.get(industry, [])
        if len(target) < 6:
            continue
        gains = [value for _point, value in target]
        for end in range(5, len(gains)):
            start = end - 5
            target_return = gains[end] - gains[start]
            positives = sum(gains[index] > gains[index - 1] for index in range(start + 1, end + 1))
            if target_return <= 0.003 or positives < 4:
                continue
            falling: list[tuple[str, float]] = []
            for other, values in curves.items():
                if other == industry or len(values) <= end:
                    continue
                other_gains = [value for _point, value in values]
                change = other_gains[end] - other_gains[start]
                if change < -0.003:
                    falling.append((other, change))
            if len(falling) < 2:
                continue
            peak = max(gains[start:end + 1])
            drawdown = max(0.0, peak - gains[end]) / max(peak - gains[start], 1e-9)
            if drawdown > 0.3:
                continue
            target_score = min(target_return / 0.02, 1.0) * 100.0
            flight_scale = abs(sum(change for _name, change in falling) / len(falling) * 100.0) * len(falling)
            flight_score = min(flight_scale / 5.0, 1.0) * 100.0
            intensity = target_score * 0.5 + flight_score * 0.5
            breadth = min(len(falling) / max(len(curves) - 1, 10), 1.0) * 100.0
            sustain = (1.0 - drawdown) * 100.0
            events.append({
                "day": day,
                "start_index": start,
                "end_index": end,
                "window_start": target[start][0],
                "window_end": target[end][0],
                "score": intensity * 0.4 + breadth * 0.2 + sustain * 0.4,
                "target_return": target_return,
                "falling": falling,
            })
    return events


def _merge_absorption_runs(events: list[dict]) -> list[list[dict]]:
    """把相邻窗口合并为「同一次独立承接」(同一交易日、窗口终点逐桶连续)。"""
    runs: list[list[dict]] = []
    for event in sorted(events, key=lambda item: (item["day"], item["end_index"])):
        if (
            runs
            and runs[-1][-1]["day"] == event["day"]
            and runs[-1][-1]["end_index"] + 1 == event["end_index"]
        ):
            runs[-1].append(event)
        else:
            runs.append([event])
    return runs


def _absorption(industry: str, scan: DragonScanInput) -> dict:
    events = _absorption_events(industry, scan)
    if not events:
        return {
            "score": 50.0,
            "weight": DIM_WEIGHTS["absorption"],
            "details": {
                "event_count": 0,
                "merged_event_count": 0,
                "fallback": True,
            },
        }
    event_scores = [event["score"] for event in events]
    best = max(event_scores)
    score = min(best + min((len(event_scores) - 1) * 5.0, 15.0), 100.0)
    runs = _merge_absorption_runs(events)
    ranked = sorted(events, key=lambda item: item["score"], reverse=True)[:3]
    return {
        "score": round(score, 2),
        "weight": DIM_WEIGHTS["absorption"],
        "details": {
            "event_count": len(event_scores),
            "merged_event_count": len(runs),
            "best_event_score": round(best, 2),
            "top_events": [
                {
                    "day": event["day"].isoformat(),
                    "window_start": event["window_start"].strftime("%H:%M"),
                    "window_end": event["window_end"].strftime("%H:%M"),
                    "score": round(event["score"], 2),
                    "target_return_pct": round(event["target_return"] * 100.0, 2),
                    "falling_count": len(event["falling"]),
                    "falling": [
                        {"industry": name, "change_pct": round(change * 100.0, 2)}
                        for name, change in sorted(event["falling"], key=lambda pair: pair[1])[:5]
                    ],
                }
                for event in ranked
            ],
        },
    }


def score_scan(scan: DragonScanInput) -> list[dict]:
    output: list[dict] = []
    for candidate in scan.candidates:
        peers = [item for item in scan.candidates if item.industry == candidate.industry]
        dimensions = {
            "drive": _drive(candidate, peers, scan),
            "leadership": _leadership(candidate, peers),
            "anti_drop": _anti_drop(candidate, scan),
            "liquidity": _liquidity(candidate, peers),
            "absorption": _absorption(candidate.industry, scan),
        }
        reject_reason = next(
            (
                f"{name}={dimensions[name]['score']:.1f} < {floor:.0f}"
                for name, floor in DIM_FLOORS.items()
                if dimensions[name]["score"] < floor
            ),
            None,
        )
        composite = sum(
            float(dimensions[name]["score"]) * weight
            for name, weight in DIM_WEIGHTS.items()
        )
        output.append({
            "symbol": candidate.symbol,
            "name": candidate.name,
            "industry": candidate.industry,
            "industries": list(candidate.industries),
            "board_count": candidate.board_count,
            "five_day_return_pct": round(candidate.five_day_return_pct, 2),
            "turnover_rate_pct": round(candidate.turnover_rate_pct, 2),
            "amount_yuan": round(candidate.amount_yuan, 2),
            "composite_score": round(composite, 2) if math.isfinite(composite) else 0.0,
            "is_true_dragon": reject_reason is None,
            "reject_reason": reject_reason,
            "dimensions": dimensions,
            "rank": None,
        })
    output.sort(key=lambda row: row["composite_score"], reverse=True)
    rank = 0
    for row in output:
        if row["is_true_dragon"]:
            rank += 1
            row["rank"] = rank
    return output
