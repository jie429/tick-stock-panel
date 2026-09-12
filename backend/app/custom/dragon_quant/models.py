from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

MinuteCurve = dict[datetime, float]


@dataclass(frozen=True)
class DragonCandidate:
    symbol: str
    name: str
    industry: str
    industries: tuple[str, ...]
    board_count: int
    five_day_return_pct: float
    change_pct: float
    turnover_rate_pct: float
    volume_lots: float
    amount_yuan: float
    sealed_volume_lots: float | None
    prev_raw_close: float
    limit_up_price: float
    minute_curve: MinuteCurve
    open_count: int | None


@dataclass(frozen=True)
class DragonScanInput:
    as_of: date
    candidates: tuple[DragonCandidate, ...]
    industry_members: dict[str, tuple[str, ...]]
    industry_change_pct: dict[str, float]
    stock_change_pct: dict[str, float]
    industry_curves: dict[str, MinuteCurve]
    market_curve: MinuteCurve
    industry_history: dict[date, dict[str, list[float]]] = field(default_factory=dict)
    data_quality: dict = field(default_factory=dict)
