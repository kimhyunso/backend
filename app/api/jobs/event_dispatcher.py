"""
Jobs API SSE 이벤트 발송 로직
"""
from datetime import datetime
from typing import Optional
from ..deps import DbDep
from ..pipeline.service import update_pipeline_stage
from ..pipeline.models import PipelineUpdate, PipelineStatus
from ..project.models import ProjectTargetStatus


async def dispatch_pipeline(project_id: str, update_payload):
    """파이프라인 상태 변경을 SSE로 브로드캐스트"""
    from ..pipeline.router import project_channels

    listeners = project_channels.get(project_id, set())
    event = {
        "project_id": project_id,
        "stage": update_payload.get("stage_id"),
        "status": update_payload.get("status", PipelineStatus.PROCESSING).value,
        "progress": update_payload.get("progress"),
        "timestamp": datetime.now().isoformat() + "Z",
    }
    for queue in list(listeners):
        await queue.put(event)


async def dispatch_target_update(
    project_id: str,
    language_code: str,
    target_status: ProjectTargetStatus,
    progress: int,
):
    """project_target 업데이트를 SSE로 브로드캐스트"""
    from ..pipeline.router import project_channels

    listeners = project_channels.get(project_id, set())
    event = {
        "project_id": project_id,
        "type": "target_update",
        "language_code": language_code,
        "status": target_status.value,
        "progress": progress,
        "timestamp": datetime.now().isoformat() + "Z",
    }
    for queue in list(listeners):
        await queue.put(event)


async def dispatch_audio_completed(
    project_id: str,
    language_code: str,
    segment_id: str,
    audio_s3_key: Optional[str] = None,
    audio_duration: Optional[float] = None,
    status: str = "completed",
    error_message: Optional[str] = None,
):
    """오디오 생성 완료/실패 이벤트를 SSE로 브로드캐스트"""
    from ..audio.router import audio_channels

    channel_key = f"{project_id}:{language_code}"
    listeners = audio_channels.get(channel_key, set())

    event_type = "audio-completed" if status == "completed" else "audio-failed"
    event_data = {
        "segmentId": segment_id,
        "projectId": project_id,
        "languageCode": language_code,
        "status": status,
    }

    if audio_s3_key:
        event_data["audioS3Key"] = audio_s3_key
    if audio_duration is not None:
        event_data["audioDuration"] = audio_duration
    if error_message:
        event_data["error"] = error_message

    event = {
        "event": event_type,
        "data": event_data,
    }

    for queue in list(listeners):
        await queue.put(event)


async def update_pipeline(db: DbDep, project_id: str, payload: dict):
    """파이프라인 디비 수정 및 SSE 이벤트 발송"""
    # 파이프라인 디비 수정
    await update_pipeline_stage(db, PipelineUpdate(**payload))
    # 파이프라인 SSE 큐에 추가
    await dispatch_pipeline(project_id, payload)
