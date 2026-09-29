from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.custom.dragon_quant import service
from app.custom.dragon_quant.data import DragonDataError, DragonScanOptions
from app.extensions import BACKEND_EXTENSION_API_VERSION, BackendExtensionRegistrar

EXTENSION_ID = "dragon.quant"
EXTENSION_API_VERSION = BACKEND_EXTENSION_API_VERSION

router = APIRouter(prefix="/api/custom/dragon-quant", tags=["dragon-quant"])


class ScanRequest(BaseModel):
    as_of: date
    top_industries: int = Field(default=5, ge=1, le=20)
    lagging_industries: int = Field(default=20, ge=2, le=50)
    industry_level: int = Field(default=2, ge=1, le=5)
    result_limit: int = Field(default=25, ge=1, le=100)
    absorption_days: int = Field(default=10, ge=3, le=30)


def _dependencies(request: Request):
    repo = getattr(request.app.state, "repo", None)
    if repo is None:
        raise HTTPException(status_code=503, detail="行情仓库尚未就绪")
    depth_service = getattr(request.app.state, "depth_service", None)
    data_dir = repo.store.data_dir
    return repo, depth_service, data_dir


@router.get("/status")
def status(request: Request) -> dict:
    repo, depth_service, data_dir = _dependencies(request)
    return service.extension_status(repo, depth_service, data_dir)


@router.post("/scans")
def create_scan(payload: ScanRequest, request: Request) -> dict:
    repo, depth_service, data_dir = _dependencies(request)
    options = DragonScanOptions(
        top_industries=payload.top_industries,
        lagging_industries=payload.lagging_industries,
        industry_level=payload.industry_level,
        result_limit=payload.result_limit,
        absorption_days=payload.absorption_days,
    )
    try:
        return service.run_scan(
            repo, depth_service, data_dir, as_of=payload.as_of, options=options,
        )
    except DragonDataError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/scans")
def scans(request: Request) -> list[dict]:
    _repo, _depth_service, data_dir = _dependencies(request)
    return service.list_scans(data_dir)


@router.get("/scans/{record_id}")
def scan_detail(record_id: str, request: Request) -> dict:
    _repo, _depth_service, data_dir = _dependencies(request)
    record = service.get_scan(data_dir, record_id)
    if record is None:
        raise HTTPException(status_code=404, detail="扫描记录不存在")
    return record


@router.delete("/scans/{record_id}")
def remove_scan(record_id: str, request: Request) -> dict:
    _repo, _depth_service, data_dir = _dependencies(request)
    if not service.delete_scan(data_dir, record_id):
        raise HTTPException(status_code=404, detail="扫描记录不存在")
    return {"ok": True}


def setup(registrar: BackendExtensionRegistrar) -> None:
    registrar.include_router(router)
