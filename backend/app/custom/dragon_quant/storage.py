from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import ClassVar

from app.market_time import cn_now


class DragonRecordStore:
    _lock_guard: ClassVar[threading.Lock] = threading.Lock()
    _path_locks: ClassVar[dict[str, threading.RLock]] = {}

    def __init__(self, data_dir: Path, filename: str, prefix: str, limit: int = 100) -> None:
        self.path = data_dir / "user_data" / "dragon_quant" / filename
        self.prefix = prefix
        self.limit = limit
        key = str(self.path.resolve())
        with self._lock_guard:
            self._lock = self._path_locks.setdefault(key, threading.RLock())

    def list(self) -> list[dict]:
        with self._lock:
            if not self.path.exists():
                return []
            try:
                value = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                return []
            if not isinstance(value, list):
                return []
            return sorted(
                (item for item in value if isinstance(item, dict)),
                key=lambda item: str(item.get("created_at") or ""),
                reverse=True,
            )

    def get(self, record_id: str) -> dict | None:
        return next((item for item in self.list() if item.get("id") == record_id), None)

    def latest_for_date(self, as_of: str) -> dict | None:
        return next((item for item in self.list() if item.get("as_of") == as_of), None)

    def save(self, value: dict) -> dict:
        with self._lock:
            items = self.list()
            record = dict(value)
            record.setdefault("id", f"{self.prefix}_{int(time.time() * 1000)}")
            record.setdefault(
                "created_at",
                cn_now().replace(tzinfo=None).isoformat(timespec="seconds"),
            )
            items = [item for item in items if item.get("id") != record["id"]]
            items.insert(0, record)
            self._write(items[: self.limit])
            return record

    def delete(self, record_id: str) -> bool:
        with self._lock:
            items = self.list()
            remaining = [item for item in items if item.get("id") != record_id]
            if len(remaining) == len(items):
                return False
            self._write(remaining)
            return True

    def _write(self, items: list[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(items, ensure_ascii=False, indent=2, allow_nan=False),
            encoding="utf-8",
        )
        os.replace(temporary, self.path)
