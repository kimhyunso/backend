from datetime import datetime
from typing import Any, Dict, List, Optional, Annotated
from enum import Enum
from bson import ObjectId
from pydantic import BaseModel, BeforeValidator, Field

PyObjectId = Annotated[
    str, BeforeValidator(lambda v: str(v) if isinstance(v, ObjectId) else v)
]


class ProjectCreate(BaseModel):
    title: str
    owner_id: str
    sourceType: str  # 'file' | 'youtube'
    youtubeUrl: Optional[str] = None
    fileName: Optional[str] = None
    fileSize: Optional[int] = None
    speakerCount: int
    detectAutomatically: bool
    sourceLanguage: Optional[str] = None
    targetLanguages: List[str]


class ProjectThumbnail(BaseModel):
    kind: str  # "s3" or "external"
    key: str | None = None
    url: str | None = None


class ProjectBase(BaseModel):
    owner_id: str
    title: str
    status: str  # uploading | uploaded | processing | completed | failed
    source_type: str  # 'file' | 'youtube'
    video_source: str | None = None
    source_language: Optional[str] = None
    target_languages: List[str] = []
    created_at: datetime
    speaker_count: Optional[int] = None


class ProjectPublic(ProjectBase):
    project_id: str
    thumbnail: ProjectThumbnail | None = None
    glosary_id: Optional[str] = None
    duration_seconds: Optional[int] = None
    audio_source: str | None = None  # 원본 영상에서 추출한 전체 오디오 (mp4->wav)
    vocal_source: str | None = None  # 분리한 발화 음성 (vocals.wav)
    background_audio_source: str | None = None


class ProjectCreateResponse(BaseModel):
    project_id: str


class ProjectUpdate(BaseModel):
    project_id: str
    status: str | None = None
    video_source: str | None = None
    audio_source: str | None = None  # 원본 영상에서 추출한 전체 오디오 (mp4->wav)
    vocal_source: str | None = None  # 분리한 발화 음성 (vocals.wav)
    background_audio_source: str | None = None
    thumbnail: ProjectThumbnail | None = None
    segment_assets_prefix: Optional[str] = None
    segments: Optional[List[Dict[str, Any]]] = None
    owner_id: str | None = None
    source_language: Optional[str] = None
    title: Optional[str] = None
    duration_seconds: Optional[int] = None
    default_speaker_voices: Optional[Dict[str, Dict[str, Dict[str, str]]]] = (
        None  # {target_lang: {speaker: {ref_wav_key, prompt_text}}}
    )


class ProjectTargetStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ProjectTargetCreate(BaseModel):
    project_id: str
    language_code: str
    status: ProjectTargetStatus = ProjectTargetStatus.PENDING
    progress: int = 0


class ProjectTarget(BaseModel):
    target_id: PyObjectId = Field(validation_alias="_id")
    project_id: str
    language_code: str
    status: ProjectTargetStatus
    progress: int


class ProjectTargetUpdate(BaseModel):
    status: Optional[ProjectTargetStatus] = None
    progress: Optional[int] = None


class ProjectOut(BaseModel):
    id: PyObjectId = Field(validation_alias="_id")
    title: str
    status: str
    video_source: str | None = None
    thumbnail: ProjectThumbnail | None = None
    duration_seconds: Optional[int] | None = None
    issue_count: int = 0  # 새로 집계한 값을 넣기 위한 필드
    targets: list[ProjectTarget] = Field(default_factory=list)
    source_language: Optional[str] = None
    created_at: datetime
    speaker_count: Optional[int] = None


class EditorPlaybackState(BaseModel):
    duration: float
    active_language: str
    playback_rate: float = 1.0
    video_source: str | None
    audio_source: str | None


class SegmentTranslationResponse(BaseModel):
    id: PyObjectId
    project_id: PyObjectId
    language_code: str
    speaker_tag: str
    start: float = Field(..., ge=0)
    end: float = Field(..., ge=0)
    source_text: str
    target_text: str | None = None
    segment_audio_url: str | None = None


class EditorStateResponse(BaseModel):
    project_id: str
    segments: list[SegmentTranslationResponse] = []
    # voices: list[VoiceSampleOut] = []
    playback: EditorPlaybackState


class ProjectSegmentCreate(BaseModel):
    speaker_tag: str | None = None
    start: float = Field(..., ge=0)
    end: float = Field(..., ge=0)
    source_text: str
    is_verified: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SegmentTranslationCreate(BaseModel):
    language_code: str
    target_text: str | None = None
    segment_audio_url: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SegmentTTSRegenerateRequest(BaseModel):
    """단일 세그먼트에 대한 TTS 재생성 요청"""

    segment_id: str  # segment의 _id (project_segments 컬렉션의 _id)
    translated_text: str
    start: float
    end: float
    target_lang: str
    mod: str = "fixed"  # "fixed" or "dynamic"
    voice_sample_id: Optional[str] = (
        None  # voice_sample의 ID (있으면 해당 voice_sample 사용, 없으면 default_speaker_voices 사용)
    )


class SegmentTTSRegenerateResponse(BaseModel):
    """세그먼트 TTS 재생성 응답"""

    job_id: str
    project_id: str
    target_lang: str
    mod: str
