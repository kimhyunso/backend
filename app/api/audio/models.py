from pydantic import BaseModel
from typing import Optional


class AudioGenerationEvent(BaseModel):
    """오디오 생성 완료 이벤트 데이터"""

    segment_id: str
    audio_s3_key: str
    audio_duration: float  # 초 단위
    project_id: str
    language_code: str
