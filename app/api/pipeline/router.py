from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import Any, Dict
import asyncio, json
from datetime import datetime
from sse_starlette.sse import EventSourceResponse
from collections import defaultdict
from app.api.deps import DbDep
from .service import get_pipeline_status, update_pipeline_stage
from .models import PipelineUpdate, ProjectPipeline
import logging

pipeline_router = APIRouter(prefix="/pipeline", tags=["Pipeline"])
logger = logging.getLogger(__name__)


@pipeline_router.get("/{project_id}/status", summary="파이프라인 상태 조회")
async def get_project_pipeline_status(project_id: str, db: DbDep) -> ProjectPipeline:
    """프로젝트의 현재 파이프라인 상태를 조회합니다."""
    return await get_pipeline_status(db, project_id)


@pipeline_router.post("/{project_id}/update", summary="파이프라인 단계 업데이트")
async def update_project_pipeline_stage(
    project_id: str, payload: PipelineUpdate, db: DbDep
) -> Dict[str, Any]:
    """파이프라인 단계의 상태를 업데이트합니다."""
    logger.info(f"pipline update: {project_id}")
    # payload의 project_id를 URL의 project_id로 덮어쓰기
    payload.project_id = project_id
    result = await update_pipeline_stage(db, payload)
    return {"success": result["success"]}


@pipeline_router.get("/{project_id}/stream", summary="파이프라인 상태 실시간 스트림")
async def stream_pipeline_status(project_id: str, db: DbDep):
    """SSE를 통해 파이프라인 상태를 실시간으로 스트리밍합니다."""

    async def event_stream():
        try:
            while True:
                # 현재 파이프라인 상태 조회
                pipeline = await get_pipeline_status(db, project_id)

                # SSE 형식으로 데이터 전송
                data = pipeline.model_dump(mode="json")
                # datetime 객체를 문자열로 변환
                data = _serialize_datetime(data)
                logger.info(f"info data: {data}")

                yield f"data: {json.dumps(data)}\n\n"

                # (폴링)3초마다 업데이트 (실제로는 파이프라인 상태 변경 시에만 전송하도록 최적화 가능)
                await asyncio.sleep(3)

        except Exception as e:
            # 에러 발생 시 클라이언트에 에러 메시지 전송
            error_data = {"error": str(e), "timestamp": datetime.now().isoformat()}
            yield f"data: {json.dumps(error_data)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control",
        },
    )


def _serialize_datetime(obj: Any) -> Any:
    """datetime 객체를 JSON 직렬화 가능한 문자열로 변환"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: _serialize_datetime(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_serialize_datetime(item) for item in obj]
    return obj


project_channels = defaultdict(set)  # project_id -> events queue


@pipeline_router.get("/{project_id}/events")
async def pipeline_events(project_id: str):
    queue = asyncio.Queue()
    project_channels[project_id].add(queue)

    async def event_generator():
        try:
            while True:
                data = await queue.get()
                yield {"event": "stage", "data": json.dumps(data)}
        finally:
            project_channels[project_id].discard(queue)

    return EventSourceResponse(event_generator())
