# models.py
from pydantic import ConfigDict, BaseModel, Field, BeforeValidator, EmailStr
from typing import Optional, List, Any, Annotated
from bson import ObjectId
from datetime import datetime

PyObjectId = Annotated[
    str,  # 👈 최종 변환될 타입은 'str'입니다.
    BeforeValidator(lambda v: str(v) if isinstance(v, ObjectId) else v),
]


class UserCreate(BaseModel):
    username: str = Field(..., min_length=3)
    email: EmailStr  # 👈 Pydantic이 이메일 형식을 자동으로 검증
    hashed_password: str = Field(..., min_length=6, description="6자 이상")
    role: str


class User(BaseModel):
    email: str
    username: str
    hashed_password: str
    role: str
    current_session: Optional[str] = None  # refresh token


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class GoogleLogin(BaseModel):
    id_token: str = Field(..., description="Google ID token received from client")


class UserOut(BaseModel):
    id: PyObjectId = Field(alias="_id")
    username: str
    role: str
    hashed_password: str
    email: EmailStr
    createdAt: datetime
    google_sub: Optional[str] = None
    youtube_channel: Optional["YoutubeChannel"] = None
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class TokenData(BaseModel):
    sub: Optional[str] = None


# RefreshToken 요청 모델 추가
class RefreshTokenRequest(BaseModel):
    refresh_token: str


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=6)
    new_password: str = Field(..., min_length=6)


class YoutubeChannel(BaseModel):
    channel_id: str
    title: str
    thumbnail_url: str | None = None
    connected_at: datetime | None = None
    updated_at: datetime | None = None
