from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    status,
    UploadFile,
    File,
    Form,
)
from fastapi.responses import StreamingResponse
from typing import Optional, Any
import os
import asyncio
import tempfile
import json
from pathlib import Path
from uuid import uuid4
from bson import ObjectId
from datetime import datetime

from ..deps import DbDep
from ..auth.service import get_current_user_from_cookie
from ..auth.model import UserOut
from ..jobs.service import (
    create_job,
    enqueue_job,
    mark_job_failed,
    _resolve_callback_base,
)
from ..jobs.models import JobCreate
from .service import VoiceSampleService
from .models import (
    VoiceSampleCreate,
    VoiceSampleUpdate,
    VoiceSampleOut,
    VoiceSampleListResponse,
    TestSynthesisResponse,
    VoiceSamplePrepareUpload,
    VoiceSampleFinishUpload,
    VoiceSampleAvatarPrepareUpload,
    VoiceSampleAvatarUpdate,
)
from .utils import (
    validate_audio_file_info,
    validate_audio_file_from_s3,
    ffprobe_duration,
    MAX_DURATION,
)
from app.config.s3 import s3
import logging

logger = logging.getLogger(__name__)
voice_samples_router = APIRouter(prefix="/voice-samples", tags=["Voice Samples"])

AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")

SERVICE_INTRO_SCRIPT = "안녕하세요. AI 음성 합성 서비스를 소개합니다. 이 서비스를 통해 여러분의 목소리로 다양한 콘텐츠를 제작할 수 있습니다."


@voice_samples_router.post(
    "/test-synthesis",
    response_model=TestSynthesisResponse,
    status_code=status.HTTP_200_OK,
)
async def test_synthesis(
    db: DbDep,
    file: UploadFile = File(..., description="오디오 파일 (mp3, wav)"),
    text: str = Form(..., description="합성할 텍스트"),
    target_lang: str = Form(default="ko", description="대상 언어 (ko, en, ja)"),
    current_user: UserOut = Depends(get_current_user_from_cookie),
):
    """
    테스트 합성: 음성 파일과 텍스트를 받아 TTS를 합성합니다.
    SQS를 통해 워커에 작업을 전달하고 job_id를 반환합니다.
    """
    if not AWS_S3_BUCKET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AWS_S3_BUCKET not configured",
        )

    # 파일 검증 (크기, 형식만 - 변환은 워커에서 처리)
    validate_audio_file_info(
        file.filename or "audio",
        file.content_type or "audio/mpeg",
        file.size,
    )

    # 임시 파일로 저장
    suffix = Path(file.filename or "audio").suffix or ".mp3"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
        tmp_path = Path(tmp_file.name)
        content = await file.read()
        tmp_file.write(content)

    try:
        # 파일 길이 검증
        duration = ffprobe_duration(str(tmp_path))
        if duration > MAX_DURATION:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="60초 이내의 파일만 업로드할 수 있습니다.",
            )

        # Job 생성 (job_id 먼저 생성)
        job_oid = ObjectId()
        job_id_str = str(job_oid)
        callback_base = _resolve_callback_base()
        callback_url = f"{callback_base.rstrip('/')}/api/jobs/{job_id_str}/status"

        # S3에 임시 저장 (원본 파일 그대로 - mp3 또는 wav)
        s3_key = f"voice-samples/temp/{job_id_str}{suffix}"

        await asyncio.to_thread(
            s3.upload_file,
            str(tmp_path),
            AWS_S3_BUCKET,
            s3_key,
        )

        job_payload = JobCreate(
            project_id=f"voice-sample-test-{current_user.id}",
            input_key=s3_key,
            callback_url=callback_url,
            task="test_synthesis",
            task_payload={
                "file_path": s3_key,  # mp3 또는 wav (워커에서 변환)
                "text": text,
                "target_lang": target_lang,
            },
        )

        job = await create_job(db, job_payload, job_oid=job_oid)

        # SQS에 큐잉
        try:
            await enqueue_job(job)
        except Exception as exc:
            await mark_job_failed(
                db,
                job.job_id,
                error="sqs_publish_failed",
                message=str(exc),
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to enqueue job",
            ) from exc

        return TestSynthesisResponse(job_id=job.job_id, status="queued")

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process test synthesis: {exc}",
        ) from exc
    finally:
        # 임시 파일 정리
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


@voice_samples_router.post("/prepare-upload")
async def prepare_voice_sample_upload(
    payload: VoiceSamplePrepareUpload,
    current_user: UserOut = Depends(get_current_user_from_cookie),
) -> dict:
    """음성 샘플 업로드 presigned URL 생성"""
    if not AWS_S3_BUCKET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AWS_S3_BUCKET not configured",
        )

    # 파일 정보 검증
    validate_audio_file_info(payload.filename, payload.content_type)

    # S3 키 생성: voice-samples/{user_id}/{uuid}_{filename}
    user_id = current_user.id
    sample_id = str(uuid4())
    object_key = f"voice-samples/{user_id}/{sample_id}_{payload.filename}"

    try:
        presigned = s3.generate_presigned_post(
            Bucket=AWS_S3_BUCKET,
            Key=object_key,
            Fields={"Content-Type": payload.content_type},
            Conditions=[
                ["starts-with", "$Content-Type", payload.content_type.split("/")[0]]
            ],
            ExpiresIn=300,  # 5분
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"presign 실패: {exc}",
        ) from exc

    return {
        "upload_url": presigned["url"],
        "fields": presigned["fields"],
        "object_key": object_key,
    }


@voice_samples_router.post(
    "/finish-upload",
    response_model=VoiceSampleOut,
    status_code=status.HTTP_201_CREATED,
)
async def finish_voice_sample_upload(
    db: DbDep,
    payload: VoiceSampleFinishUpload,
    current_user: UserOut = Depends(get_current_user_from_cookie),
):
    """음성 샘플 업로드 완료 후 DB 저장"""
    if not AWS_S3_BUCKET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AWS_S3_BUCKET not configured",
        )

    # S3에 업로드된 파일 검증 (존재 및 크기만 - 길이는 프론트엔드에서 검증)
    try:
        await validate_audio_file_from_s3(payload.object_key)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"파일 검증 실패: {exc}",
        ) from exc

    # DB에 저장
    service = VoiceSampleService(db)
    data = VoiceSampleCreate(
        name=payload.name,
        description=payload.description,
        is_public=payload.is_public,
        file_path_wav=payload.object_key,  # mp3 또는 wav 모두 가능
        audio_sample_url=None,  # Optional: 미리듣기용 저용량 mp3 URL (없으면 file_path_wav를 storage API로 사용)
        country=payload.country,
        gender=payload.gender,
        avatar_image_path=payload.avatar_image_path,
    )

    voice_sample = await service.create_voice_sample(data, current_user)

    # TTS 작업 시작
    try:
        job_oid = ObjectId()
        job_id_str = str(job_oid)
        callback_base = _resolve_callback_base()
        callback_url = f"{callback_base.rstrip('/')}/api/jobs/{job_id_str}/status"

        job_payload = JobCreate(
            project_id=f"voice-sample-{voice_sample.sample_id}",
            input_key=payload.object_key,
            callback_url=callback_url,
            task="test_synthesis",
            task_payload={
                "file_path": payload.object_key,
                "text": SERVICE_INTRO_SCRIPT,
                "target_lang": "ko",
                "voice_sample_id": str(voice_sample.sample_id),
            },
        )

        # Job 생성
        job = await create_job(db, job_payload, job_oid=job_oid)

        # Job 큐잉
        await enqueue_job(job)

    except Exception as exc:
        logger.error(
            f"Failed to start TTS job for voice sample {voice_sample.sample_id}: {exc}"
        )
        logger.exception("Exception details:")
    return voice_sample


@voice_samples_router.post(
    "/{sample_id}/avatar/prepare-upload",
    status_code=status.HTTP_200_OK,
)
async def prepare_voice_sample_avatar_upload(
    sample_id: str,
    payload: VoiceSampleAvatarPrepareUpload,
    db: DbDep,
    current_user: UserOut = Depends(get_current_user_from_cookie),
):
    """보이스 샘플 아바타 업로드용 presigned URL"""
    if not AWS_S3_BUCKET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AWS_S3_BUCKET not configured",
        )

    service = VoiceSampleService(db)
    sample = await service.get_voice_sample(sample_id, current_user)
    if str(sample.owner_id) != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only upload avatar for your own samples",
        )

    suffix = Path(payload.filename).suffix or ".png"
    object_key = f"voice-samples/images/{sample_id}/{uuid4()}{suffix}"
    try:
        presigned = s3.generate_presigned_post(
            Bucket=AWS_S3_BUCKET,
            Key=object_key,
            Fields={"Content-Type": payload.content_type},
            Conditions=[["starts-with", "$Content-Type", payload.content_type.split("/")[0]]],
            ExpiresIn=300,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"avatar presign 실패: {exc}",
        ) from exc

    return {
        "upload_url": presigned["url"],
        "fields": presigned["fields"],
        "object_key": object_key,
    }


@voice_samples_router.post(
    "/{sample_id}/avatar",
    response_model=VoiceSampleOut,
    status_code=status.HTTP_200_OK,
)
async def finalize_voice_sample_avatar(
    sample_id: str,
    payload: VoiceSampleAvatarUpdate,
    db: DbDep,
    current_user: UserOut = Depends(get_current_user_from_cookie),
):
    """보이스 샘플 아바타 업로드 완료 처리"""
    service = VoiceSampleService(db)
    sample = await service.get_voice_sample(sample_id, current_user)
    if str(sample.owner_id) != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You can only update avatar for your own samples",
        )

    updated = await service.update_voice_sample(
        sample_id,
        VoiceSampleUpdate(avatar_image_path=payload.object_key),
        current_user,
    )
    return updated


@voice_samples_router.get(
    "",
    response_model=VoiceSampleListResponse,
)
async def list_voice_samples(
    db: DbDep,
    q: Optional[str] = Query(None, description="검색어 (이름 또는 설명)"),
    favorites_only: bool = Query(False, description="즐겨찾기만 조회"),
    my_samples_only: bool = Query(False, description="내 샘플만 조회"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    limit: int = Query(20, ge=1, le=100, description="페이지당 개수"),
    current_user: Optional[UserOut] = Depends(get_current_user_from_cookie),
):
    """음성 샘플 목록 조회"""
    service = VoiceSampleService(db)
    samples, total = await service.list_voice_samples(
        current_user=current_user,
        q=q,
        favorites_only=favorites_only,
        my_samples_only=my_samples_only,
        page=page,
        limit=limit,
    )
    return VoiceSampleListResponse(samples=samples, total=total)


@voice_samples_router.get(
    "/{sample_id}",
    response_model=VoiceSampleOut,
)
async def get_voice_sample(
    db: DbDep,
    sample_id: str,
    current_user: Optional[UserOut] = Depends(get_current_user_from_cookie),
):
    """음성 샘플 상세 조회"""
    service = VoiceSampleService(db)
    return await service.get_voice_sample(sample_id, current_user)


@voice_samples_router.put(
    "/{sample_id}",
    response_model=VoiceSampleOut,
)
async def update_voice_sample(
    db: DbDep,
    sample_id: str,
    data: VoiceSampleUpdate,
    current_user: UserOut = Depends(get_current_user_from_cookie),
):
    """음성 샘플 업데이트"""
    service = VoiceSampleService(db)
    return await service.update_voice_sample(sample_id, data, current_user)


@voice_samples_router.delete(
    "/{sample_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_voice_sample(
    db: DbDep,
    sample_id: str,
    current_user: UserOut = Depends(get_current_user_from_cookie),
):
    """음성 샘플 삭제"""
    service = VoiceSampleService(db)
    await service.delete_voice_sample(sample_id, current_user)
    return None


def _serialize_datetime(obj: Any) -> Any:
    """datetime 객체를 JSON 직렬화 가능한 문자열로 변환"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: _serialize_datetime(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_serialize_datetime(item) for item in obj]
    return obj


@voice_samples_router.get("/{sample_id}/stream", summary="음성 샘플 상태 실시간 스트림")
async def stream_voice_sample_status(sample_id: str, db: DbDep):
    """SSE를 통해 음성 샘플의 audio_sample_url 업데이트를 실시간으로 스트리밍합니다."""

    async def event_stream():
        service = VoiceSampleService(db)
        try:
            while True:
                # voice_sample 조회
                try:
                    sample = await service.get_voice_sample(sample_id, None)
                    data = {
                        "sample_id": str(sample.sample_id),
                        "audio_sample_url": sample.audio_sample_url,
                        "has_audio_sample": sample.audio_sample_url is not None,
                    }
                    # datetime 객체를 문자열로 변환
                    data = _serialize_datetime(data)

                    yield f"data: {json.dumps(data)}\n\n"

                    # audio_sample_url이 채워지면 종료
                    if sample.audio_sample_url:
                        break

                except HTTPException as e:
                    if e.status_code == 404:
                        error_data = {"error": "Voice sample not found"}
                        yield f"data: {json.dumps(error_data)}\n\n"
                        break
                    raise

                # 2초마다 폴링
                await asyncio.sleep(2)

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
