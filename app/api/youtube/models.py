from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class YoutubeOAuthStartResponse(BaseModel):
    auth_url: str
    state: str
    expires_at: datetime


class YoutubeOAuthCallbackRequest(BaseModel):
    code: str = Field(..., min_length=1)
    state: str = Field(..., min_length=1)


class YoutubeConnectionInfo(BaseModel):
    connected: bool
    channel_title: Optional[str] = None
    channel_id: Optional[str] = None
    channel_thumbnail: Optional[str] = None
    updated_at: Optional[datetime] = None


class YoutubePrivacyStatus(str, Enum):
    PRIVATE = "private"
    UNLISTED = "unlisted"
    PUBLIC = "public"


class YoutubePublishRequest(BaseModel):
    project_id: str
    asset_id: str
    language_code: str
    title: str
    description: Optional[str] = None
    privacy_status: YoutubePrivacyStatus = YoutubePrivacyStatus.UNLISTED
    tags: Optional[List[str]] = None


class YoutubePublishResponse(BaseModel):
    video_id: str
    channel_id: str
    published_at: datetime
    title: str
