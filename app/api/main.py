from fastapi import APIRouter

from .storage.routes import upload_router
from .jobs.routes import router as job_router
from .preview.router import editor_preview_router, preview_router
from .project.router import project_router
from .segment.router import segment_router, editor_segment_router
from .segment.routes import segments_router  # 새로운 세그먼트 조회 라우터
from .pipeline.router import pipeline_router
from .auth.router import auth_router
from .voice_samples.router import voice_samples_router
from .me.router import me_router
from .translate.routes import trans_router
from .language.router import router as language_router
from .project.target_routes import target_router
from .assets.router import assets_router
from .user.routes import user_router
from .suggesion.router import suggestion_router
from .youtube.router import youtube_router

api_router = APIRouter(prefix="/api")

api_router.include_router(upload_router)
api_router.include_router(preview_router)
api_router.include_router(editor_preview_router)
api_router.include_router(project_router)
api_router.include_router(segment_router)
api_router.include_router(editor_segment_router)
api_router.include_router(segments_router)  # 새로운 세그먼트 조회 라우터 추가
api_router.include_router(job_router)
api_router.include_router(pipeline_router)
api_router.include_router(auth_router)
api_router.include_router(voice_samples_router)
api_router.include_router(me_router)
api_router.include_router(trans_router)
api_router.include_router(language_router)
api_router.include_router(user_router)
api_router.include_router(target_router)
api_router.include_router(assets_router)
api_router.include_router(suggestion_router)
api_router.include_router(youtube_router)
