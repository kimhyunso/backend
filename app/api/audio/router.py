from fastapi import APIRouter, Query
from sse_starlette.sse import EventSourceResponse
import asyncio
import json
from collections import defaultdict
from typing import Dict, Set

audio_router = APIRouter(prefix="/audio", tags=["Audio"])

# 프로젝트별 + 언어별 이벤트 채널
# key: f"{project_id}:{language_code}", value: Set[Queue]
audio_channels: Dict[str, Set[asyncio.Queue]] = defaultdict(set)


@audio_router.get("/events")
async def audio_events(
    projectId: str = Query(..., description="프로젝트 ID"),
    language: str = Query(..., description="언어 코드"),
):
    """
    오디오 생성 이벤트를 SSE로 스트리밍합니다.

    이벤트 타입:
    - audio-completed: 세그먼트 TTS 생성 완료
      데이터: { segmentId, audioS3Key, audioDuration, projectId, languageCode }
    """
    channel_key = f"{projectId}:{language}"
    queue = asyncio.Queue()
    audio_channels[channel_key].add(queue)

    async def event_generator():
        try:
            while True:
                data = await queue.get()
                event_type = data.get("event", "audio-completed")
                event_data = data.get("data", {})
                yield {"event": event_type, "data": json.dumps(event_data)}
        finally:
            audio_channels[channel_key].discard(queue)
            # 채널에 리스너가 없으면 삭제
            if not audio_channels[channel_key]:
                del audio_channels[channel_key]

    return EventSourceResponse(event_generator())
