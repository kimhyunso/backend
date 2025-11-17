# app/api/routes/upload.py
import os
import asyncio
import logging
import tempfile
from uuid import uuid4
from hashlib import sha256
from sse_starlette.sse import EventSourceResponse

from fastapi import APIRouter, HTTPException, Request, status, Depends
from fastapi.responses import RedirectResponse
from redis.exceptions import RedisError
from rq import Queue
from pymongo.errors import PyMongoError
from botocore.exceptions import ClientError

from app.api.jobs.service import start_job, start_jobs_for_targets
from app.api.project.service import ProjectService
from app.config.s3 import s3
from app.utils.job_utils import process_project_jobs
from ..deps import DbDep
from bson.errors import InvalidId
from ..project.models import ProjectUpdate
from ..pipeline.service import update_pipeline_stage, get_pipeline_status
from ..pipeline.models import PipelineUpdate, PipelineStatus
from .models import PresignRequest, UploadFinalize
from app.api.auth.service import get_current_user_from_cookie
from app.api.auth.model import UserOut
from .models import PresignRequest, RegisterRequest, UploadFinalize
from app.api.project.models import ProjectThumbnail
from app.config.redis import get_redis
from app.workers.jobs.video_ingest import run_ingest
from app.utils.thumbnail import extract_and_upload_thumbnail, ThumbnailError
from pathlib import Path
from moviepy.editor import VideoFileClip

logger = logging.getLogger(__name__)
upload_router = APIRouter(prefix="/storage", tags=["storage"])


def _make_idem_key(req: RegisterRequest, header_key: str | None) -> str:
    return (
        header_key
        or sha256(f"{req.project_id}|{str(req.youtube_url)}".encode()).hexdigest()
    )


r = get_redis()
UPLOAD_QUEUE = Queue("uploads", connection=r)
IDEMPOTENCY_HEADER_CANDIDATES = (
    "Idempotency-Key",
    "X-Idempotency-Key",
    "Dupilot-Idempotency-Key",
)


@upload_router.post(
    "/register-source",
    status_code=status.HTTP_202_ACCEPTED,
    summary="YouTube 소스 등록(큐잉)",
)
async def register_source(payload: RegisterRequest, request: Request, db: DbDep):
    # 1) 멱등키 확보
    header_key = None
    for header_name in IDEMPOTENCY_HEADER_CANDIDATES:
        value = request.headers.get(header_name)
        if value:
            header_key = value
            break
    job_id = _make_idem_key(payload, header_key)

    # 2) 기존 jobId가 있으면 그대로 반환
    try:
        existing_job = UPLOAD_QUEUE.fetch_job(job_id)
    except RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="업로드 작업 상태를 확인할 수 없습니다.",
        ) from exc

    if existing_job:
        existing_job.refresh()
        return {
            "job_id": existing_job.id,
            "queue": existing_job.origin,
            "status": existing_job.get_status(),
            "stage": existing_job.meta.get("stage"),
        }

    # 3) 큐에 넣기
    job_payload = {
        "project_id": payload.project_id,
        "source_url": payload.youtube_url,
    }

    try:
        job = UPLOAD_QUEUE.enqueue(
            run_ingest,
            job_payload,
            job_id=job_id,
            description=f"YouTube ingest for project {payload.project_id}",
            meta={
                "stage": "queued",
                "project_id": payload.project_id,
                "source_url": payload.youtube_url,
            },
            job_timeout="10m",
        )
    except RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="업로드 작업을 예약하지 못했습니다.",
        ) from exc

    return {
        "job_id": job.id,
        "queue": job.origin,
        "status": job.get_status(),
        "stage": job.meta.get("stage"),
    }


@upload_router.post("/prepare-upload")
async def prepare_file_upload(
    payload: PresignRequest,
    # _current_user: UserOut = Depends(get_current_user_from_cookie),  # 인증 추가
    project_service: ProjectService = Depends(ProjectService),
):
    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket:
        raise HTTPException(status_code=500, detail="AWS_S3_BUCKET env not set")

    # 프로젝트 생성 여기서 안함
    # project = await create_project(db, payload)
    # project_id = project["project_id"]

    object_key = (
        f"projects/{payload.project_id}/inputs/videos/{uuid4()}_{payload.filename}"
    )
    try:
        presigned = s3.generate_presigned_post(
            Bucket=bucket,
            Key=object_key,
            Fields={"Content-Type": payload.content_type},
            Conditions=[
                ["starts-with", "$Content-Type", payload.content_type.split("/")[0]]
            ],
            ExpiresIn=300,  # 5분
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"presign 실패: {exc}")

    return {
        "project_id": payload.project_id,
        "upload_url": presigned["url"],
        "fields": presigned["fields"],
        "object_key": object_key,
    }


@upload_router.post("/finish-upload", status_code=status.HTTP_202_ACCEPTED)
async def finish_upload(
    db: DbDep,
    payload: UploadFinalize,
    # _current_user: UserOut = Depends(get_current_user_from_cookie),  # 인증 추가
    project_service: ProjectService = Depends(ProjectService),
):
    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket:
        raise HTTPException(status_code=500, detail="AWS_S3_BUCKET env not set")

    thumbnail_payload: ProjectThumbnail | None = None
    suffix = Path(payload.object_key).suffix or ".mp4"
    duration_seconds = None
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    try:
        try:
            s3.download_file(bucket, payload.object_key, str(tmp_path))
        except ClientError:
            thumbnail_payload = None
        else:
            try:
                thumbnail_key = extract_and_upload_thumbnail(
                    tmp_path, payload.project_id
                )
                thumbnail_payload = ProjectThumbnail(
                    kind="s3", key=thumbnail_key, url=None
                )
                clip = VideoFileClip(str(tmp_path))
                duration_seconds = int(round(clip.duration or 0))
                clip.close()
            except ThumbnailError:
                thumbnail_payload = None
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass

    update_payload = ProjectUpdate(
        project_id=payload.project_id,
        status="uploaded",
        video_source=payload.object_key,
        thumbnail=thumbnail_payload,
        duration_seconds=duration_seconds,
    )
    try:
        get_pipeline_status(db, update_payload.project_id)
        result = await project_service.update_project(update_payload)
    except InvalidId as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid project_id",
        ) from exc
    except PyMongoError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update project",
        ) from exc

    # 공통 job 처리 로직 사용
    await process_project_jobs(
        project=result,
        project_id=payload.project_id,
        project_service=project_service,
        start_job=start_job,
        start_jobs_for_targets=start_jobs_for_targets,
        db=db,
        context="finish_upload",
    )

    return result


@upload_router.get("/media/{key:path}")
def media_redirect(key: str):
    bucket = os.getenv("AWS_S3_BUCKET")
    if not bucket:
        raise HTTPException(status_code=500, detail="AWS_S3_BUCKET env not set")

    url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600
    )

    # resp = RedirectResponse(url, status_code=302)
    # resp.headers["Cache-Control"] = "private, max-age=300"
    return {"url": url}


@upload_router.get("/{project_id}/events")
async def stream_events(project_id: str):
    redis = get_redis()
    pubsub = redis.pubsub()
    channel = f"uploads:{project_id}"
    pubsub.subscribe(channel)

    async def event_stream():
        loop = asyncio.get_running_loop()
        try:
            while True:
                message = await loop.run_in_executor(None, pubsub.get_message, 1.0)
                if not message or message["type"] != "message":
                    await asyncio.sleep(0.1)
                    continue
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()

                logger.info(f"event stream: {data}")
                yield {"event": "progress", "data": data}
        finally:
            pubsub.unsubscribe(channel)
            pubsub.close()

    return EventSourceResponse(event_stream())
