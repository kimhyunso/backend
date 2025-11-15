from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, HTTPException, status, Depends, Query
from typing import List, Any, Optional
from pymongo.errors import PyMongoError
from app.api.deps import DbDep
from .models import ProjectCreate, ProjectCreateResponse, ProjectOut
from .service import ProjectService
from ..segment.segment_service import SegmentService
from app.api.auth.model import UserOut
from app.api.auth.service import get_current_user_from_cookie
from .models import (
    ProjectCreate,
    ProjectCreateResponse,
    ProjectOut,
    EditorStateResponse,
    EditorPlaybackState,
    ProjectSegmentCreate,
    SegmentTranslationCreate,
    SegmentTTSRegenerateRequest,
    SegmentTTSRegenerateResponse,
)
from ..jobs.service import start_segments_tts_job

# from app.api.auth.service import get_current_user_from_cookie


def _serialize(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


project_router = APIRouter(prefix="/projects", tags=["Projects"])


@project_router.post(
    "",
    response_model=ProjectCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="프로젝트 생성",
)
async def create_project_endpoint(
    payload: ProjectCreate,
    current_user: UserOut = Depends(get_current_user_from_cookie),
    project_service: ProjectService = Depends(ProjectService),
) -> ProjectCreateResponse:
    # 인증된 사용자의 ID를 사용 (payload의 owner_id 무시)
    payload.owner_id = str(current_user.id)
    result = await project_service.create_project(payload)
    return ProjectCreateResponse.model_validate(result)


@project_router.get(
    "/me",
    response_model=List[ProjectOut],
    summary="현재 사용자 프로젝트 목록",
)
async def list_my_projects(
    current_user: UserOut = Depends(get_current_user_from_cookie),
    sort: Optional[str] = Query(default="created_at", description="정렬 필드"),
    page: int = Query(1, ge=1),
    limit: int = Query(6, ge=1, le=100),
    project_service: ProjectService = Depends(ProjectService),
) -> List[ProjectOut]:
    try:
        return await project_service.get_project_paging(
            sort=sort, page=page, limit=limit, user_id=str(current_user.id)
        )
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid project_id",
        ) from exc
    except PyMongoError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve projects",
        ) from exc


@project_router.get("", summary="프로젝트 전체 목록")
async def list_projects(
    project_service: ProjectService = Depends(ProjectService),
) -> dict:
    projects = await project_service.list_projects_with_targets()
    return {"items": projects}


@project_router.get("/{project_id}", summary="프로젝트 상세 조회")
async def get_project(
    project_id: str,
    db: DbDep,
    # project_service: ProjectService = Depends(ProjectService),
):
    try:
        project_oid = ObjectId(project_id)
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid project_id",
        ) from exc

    project = await db["projects"].find_one({"_id": project_oid})
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found",
        )

    project_id_str = str(project_oid)
    project["targets"] = (
        await db["project_targets"].find({"project_id": project_id_str}).to_list(None)
    )

    # segments = (
    #     await db["segments"]
    #     .find({"project_id": project_oid})
    #     .sort("segment_index", 1)
    #     .to_list(length=None)
    # )
    # segment_ids = [seg["_id"] for seg in segments]

    # issues = (
    #     await db["issues"]
    #     .find({"segment_id": {"$in": segment_ids}})
    #     .to_list(length=None)
    # )

    # issues_by_segment: dict[ObjectId, list[dict[str, Any]]] = {}
    # for issue in issues:
    #     issues_by_segment.setdefault(issue["segment_id"], []).append(issue)

    # for segment in segments:
    #     seg_id = segment["_id"]
    #     segment["issues"] = issues_by_segment.get(seg_id, [])
    # project["segments"] = segments
    # serialized = _serialize(project)
    # return ProjectOut.model_validate(project)
    return ProjectOut.model_validate(project)


@project_router.delete("/{project_id}", response_model=int, summary="프로젝트 삭제")
async def delete_project(
    project_id: str,
    project_service: ProjectService = Depends(ProjectService),
    segment_service: SegmentService = Depends(SegmentService),
) -> None:
    try:
        project_oid = ObjectId(project_id)
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid project_id",
        ) from exc

    result = await project_service.delete_project(project_oid)
    if result == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found",
        )

    result = await segment_service.delete_segments_by_project(project_oid)
    return result


@project_router.get("/{project_id}/languages/{language_code}", summary="에디터 조회")
async def get_project_editor(
    project_id: str,
    language_code: str,
    project_service: ProjectService = Depends(ProjectService),
    segment_service: SegmentService = Depends(SegmentService),
) -> EditorStateResponse:
    project = await project_service.get_project_by_id(project_id)  # 기본 정보
    segments = await segment_service.get_project_segment_translations(
        project_id, language_code
    )

    # voices = []  # TODO: project_id + language_code 기반 조회
    # glossaries = []  # TODO: project_id 기반 조회
    playback = EditorPlaybackState(
        duration=project.duration_seconds or 0,
        active_language=language_code,
        playback_rate=1.0,
        video_source=project.video_source,
        audio_source=project.audio_source,
    )

    return EditorStateResponse(
        project_id=str(project_id),
        segments=segments,
        # voices=voices,
        # glossaries=glossaries,
        playback=playback,
    )


@project_router.post(
    "/{project_id}/segments",
    status_code=status.HTTP_201_CREATED,
    summary="(시스템) 프로젝트 세그먼트 생성",
    # include_in_schema=False,
)
async def create_project_segment(
    project_id: str,
    payload: ProjectSegmentCreate,
    segment_service: SegmentService = Depends(SegmentService),
):
    segment_id = await segment_service.create_project_segment(project_id, payload)
    return {"segment_id": segment_id}


@project_router.post(
    "/{project_id}/segments/{segment_id}/translations",
    status_code=status.HTTP_201_CREATED,
    summary="(시스템) 세그먼트 번역 생성",
    # include_in_schema=False,
)
async def create_segment_translation(
    project_id: str,
    segment_id: str,
    payload: SegmentTranslationCreate,
    segment_service: SegmentService = Depends(SegmentService),
):
    translation_id = await segment_service.create_segment_translation(
        project_id, segment_id, payload
    )
    return {"translation_id": translation_id}


@project_router.post(
    "/{project_id}/segments/regenerate-tts",
    response_model=SegmentTTSRegenerateResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="세그먼트 TTS 재생성",
)
async def regenerate_segment_tts(
    project_id: str,
    payload: SegmentTTSRegenerateRequest,
    db: DbDep,
) -> SegmentTTSRegenerateResponse:
    """
    단일 세그먼트에 대해 TTS를 재생성합니다.

    - **project_id**: 프로젝트 ID
    - **segment_id**: 세그먼트 ID (project_segments 컬렉션의 _id)
    - **translated_text**: 번역된 텍스트 (TTS 생성에 사용)
    - **start**: 세그먼트 시작 시간 (초)
    - **end**: 세그먼트 종료 시간 (초)
    - **target_lang**: 타겟 언어 코드
    - **mod**: "fixed" (고정 길이) 또는 "dynamic" (동적 길이)
    - **voice_sample_id**: voice_sample ID (선택사항, 있으면 해당 voice_sample 사용, 없으면 프로젝트의 default_speaker_voices 사용)
    """
    import logging

    logger = logging.getLogger(__name__)

    logger.info(
        f"🔍 [regenerate_segment_tts] Received request: project_id={project_id}, payload={payload.model_dump()}"
    )

    # 단일 세그먼트를 배열로 변환 (worker는 배열을 기대함)
    segments_data = [
        {
            "segment_id": payload.segment_id,
            "translated_text": payload.translated_text,
            "start": payload.start,
            "end": payload.end,
        }
    ]

    job = await start_segments_tts_job(
        db,
        project_id=project_id,
        target_lang=payload.target_lang,
        mod=payload.mod,
        segments=segments_data,
        voice_sample_id=payload.voice_sample_id,
        segment_id=payload.segment_id,  # segment_id 전달
    )

    return SegmentTTSRegenerateResponse(
        job_id=job.job_id,
        project_id=project_id,
        target_lang=payload.target_lang,
        mod=payload.mod,
    )
