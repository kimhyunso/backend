import asyncio
import logging
import os
import secrets
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from bson import ObjectId
from fastapi import HTTPException, status
from google.auth.transport import requests as google_requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from jose import JWTError, jwt

from app.api.auth.model import UserOut
from app.api.deps import DbDep
from app.config.env import SECRET_KEY, ALGORITHM
from app.config.s3 import s3
from app.utils.crypto import decrypt_text, encrypt_text, EncryptionError

from .models import (
    YoutubeConnectionInfo,
    YoutubeOAuthCallbackRequest,
    YoutubeOAuthStartResponse,
    YoutubePublishRequest,
    YoutubePublishResponse,
)

logger = logging.getLogger(__name__)


class YoutubeIntegrationService:
    AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
    TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
    CHANNELS_ENDPOINT = "https://www.googleapis.com/youtube/v3/channels"
    DEFAULT_SCOPES = (
        "https://www.googleapis.com/auth/youtube.upload",
        "https://www.googleapis.com/auth/youtube.readonly",
    )

    def __init__(self, db: DbDep):
        self.db = db
        self.users = db.get_collection("users")
        self.assets = db.get_collection("assets")
        self.projects = db.get_collection("projects")
        self.client_id = os.getenv("GOOGLE_YT_CLIENT_ID")
        self.client_secret = os.getenv("GOOGLE_YT_CLIENT_SECRET")
        self.redirect_uri = os.getenv("GOOGLE_YT_REDIRECT_URI")
        self.bucket = os.getenv("AWS_S3_BUCKET")
        scope_env = os.getenv("GOOGLE_YT_SCOPES")
        if scope_env:
            self.scopes = tuple(scope_env.split())
        else:
            self.scopes = self.DEFAULT_SCOPES

    async def get_status(self, current_user: UserOut) -> YoutubeConnectionInfo:
        user = await self._get_user_document(current_user.id)
        channel = user.get("youtube_channel") or {}
        return YoutubeConnectionInfo(
            connected=bool(channel),
            channel_title=channel.get("title"),
            channel_id=channel.get("channel_id"),
            channel_thumbnail=channel.get("thumbnail_url"),
            updated_at=channel.get("updated_at"),
        )

    async def start_oauth_flow(
        self, current_user: UserOut
    ) -> YoutubeOAuthStartResponse:
        self._ensure_google_env()
        now = datetime.now(timezone.utc)
        expires_delta = timedelta(minutes=10)
        state_payload = {
            "sub": str(current_user.id),
            "nonce": secrets.token_urlsafe(16),
            "iat": now,
            "exp": now + expires_delta,
        }
        state_token = jwt.encode(state_payload, SECRET_KEY, algorithm=ALGORITHM)
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.scopes),
            "access_type": "offline",
            "include_granted_scopes": "true",
            "state": state_token,
            "prompt": "consent",
        }
        auth_url = f"{self.AUTH_ENDPOINT}?{urlencode(params)}"
        return YoutubeOAuthStartResponse(
            auth_url=auth_url,
            state=state_token,
            expires_at=now + expires_delta,
        )

    async def complete_oauth_flow(
        self,
        current_user: UserOut,
        payload: YoutubeOAuthCallbackRequest,
    ) -> YoutubeConnectionInfo:
        self._ensure_google_env()
        decoded = self._decode_state(payload.state)
        if decoded.get("sub") != str(current_user.id):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="OAuth state does not match the current user.",
            )
        token_data = await self._exchange_code_for_tokens(payload.code)
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        if not access_token or not refresh_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Google did not return access/refresh tokens.",
            )

        channel_info = await self._fetch_channel_profile(access_token)
        user_oid = self._as_object_id(current_user.id)
        await self._persist_tokens(user_oid, token_data, channel_info)
        return YoutubeConnectionInfo(
            connected=True,
            channel_title=channel_info["title"],
            channel_id=channel_info["channel_id"],
            channel_thumbnail=channel_info.get("thumbnail_url"),
            updated_at=datetime.now(timezone.utc),
        )

    async def disconnect(self, current_user: UserOut) -> None:
        user_oid = self._as_object_id(current_user.id)
        await self.users.update_one(
            {"_id": user_oid},
            {
                "$unset": {
                    "youtube_auth": "",
                    "youtube_channel": "",
                }
            },
        )

    async def publish_video(
        self, current_user: UserOut, payload: YoutubePublishRequest
    ) -> YoutubePublishResponse:
        if not self.bucket:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="AWS_S3_BUCKET env not set",
            )
        user = await self._get_user_document(current_user.id)
        creds = await self._build_credentials(user)
        asset = await self._get_asset(payload.asset_id)
        if asset.get("project_id") != payload.project_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Asset does not belong to the specified project.",
            )
        if asset.get("asset_type") != "preview_video":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only rendered videos can be published to YouTube.",
            )
        project = await self._get_project(payload.project_id)
        project_title = project.get("title", "Dupilot Project")

        file_key = asset.get("file_path")
        if not file_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Asset is missing file path.",
            )

        tmp_path = await asyncio.to_thread(self._download_asset_file, file_key)
        try:
            upload_response = await asyncio.to_thread(
                self._upload_video_to_youtube,
                creds,
                payload,
                project_title,
                tmp_path,
            )
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        video_id = upload_response.get("id")
        if not video_id:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="YouTube did not return a video id.",
            )

        channel_id = upload_response.get("snippet", {}).get("channelId")
        published_at = datetime.now(timezone.utc)

        await self.assets.update_one(
            {"_id": asset["_id"]},
            {
                "$set": {
                    "youtube_video_id": video_id,
                    "youtube_channel_id": channel_id,
                    "youtube_published_at": published_at,
                }
            },
        )

        return YoutubePublishResponse(
            video_id=video_id,
            channel_id=channel_id or "",
            published_at=published_at,
            title=payload.title,
        )

    def _ensure_google_env(self) -> None:
        if not all([self.client_id, self.client_secret, self.redirect_uri]):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Google YouTube OAuth env vars are not configured.",
            )

    def _decode_state(self, state: str) -> dict[str, Any]:
        try:
            return jwt.decode(state, SECRET_KEY, algorithms=[ALGORITHM])
        except JWTError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid OAuth state parameter.",
            ) from exc

    async def _exchange_code_for_tokens(self, code: str) -> dict[str, Any]:
        data = {
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code",
        }
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.post(
                    self.TOKEN_ENDPOINT,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
        except httpx.HTTPError as exc:
            logger.error("HTTP error during token exchange: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Could not reach Google OAuth servers.",
            ) from exc
        if response.status_code != 200:
            logger.error("Failed to exchange YouTube code: %s", response.text)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to exchange authorization code with Google.",
            )
        return response.json()

    async def _fetch_channel_profile(self, access_token: str) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        params = {"part": "snippet", "mine": "true"}
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    self.CHANNELS_ENDPOINT, headers=headers, params=params
                )
        except httpx.HTTPError as exc:
            logger.error("HTTP error while fetching YouTube channel: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Could not reach YouTube API.",
            ) from exc
        if response.status_code != 200:
            logger.error("Failed to fetch YouTube channel profile: %s", response.text)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to fetch YouTube channel profile.",
            )
        data = response.json()
        items = data.get("items") or []
        if not items:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No YouTube channel found for this Google account.",
            )
        channel = items[0]
        snippet = channel.get("snippet") or {}
        thumbnails = snippet.get("thumbnails") or {}
        thumb_url = (
            thumbnails.get("high", {}).get("url")
            or thumbnails.get("medium", {}).get("url")
            or thumbnails.get("default", {}).get("url")
        )
        return {
            "channel_id": channel.get("id"),
            "title": snippet.get("title"),
            "thumbnail_url": thumb_url,
        }

    async def _persist_tokens(
        self,
        user_oid: ObjectId,
        token_data: dict[str, Any],
        channel_info: dict[str, str] | None = None,
    ) -> None:
        expires_in = token_data.get("expires_in") or 3600
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
        try:
            auth_payload = {
                "access_token": encrypt_text(token_data["access_token"]),
                "refresh_token": encrypt_text(token_data["refresh_token"]),
                "expires_at": expires_at,
                "scope": token_data.get("scope"),
                "token_type": token_data.get("token_type"),
                "updated_at": datetime.now(timezone.utc),
            }
        except EncryptionError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to secure YouTube credentials.",
            ) from exc

        update_payload: dict[str, Any] = {
            "youtube_auth": auth_payload,
        }
        if channel_info:
            update_payload["youtube_channel"] = {
                "channel_id": channel_info.get("channel_id"),
                "title": channel_info.get("title"),
                "thumbnail_url": channel_info.get("thumbnail_url"),
                "connected_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }

        await self.users.update_one({"_id": user_oid}, {"$set": update_payload})

    def _as_utc_datetime(self, value: Any) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        return None

    def _as_object_id(self, value: str) -> ObjectId:
        try:
            return ObjectId(value)
        except Exception as exc:  # pragma: no cover - defensive
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid ObjectId value.",
            ) from exc

    async def _get_user_document(self, user_id: str) -> dict[str, Any]:
        user = await self.users.find_one({"_id": self._as_object_id(user_id)})
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found."
            )
        return user

    async def _get_asset(self, asset_id: str) -> dict[str, Any]:
        try:
            asset_oid = ObjectId(asset_id)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid asset_id"
            ) from exc
        asset = await self.assets.find_one({"_id": asset_oid})
        if not asset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found."
            )
        return asset

    async def _get_project(self, project_id: str) -> dict[str, Any]:
        try:
            project_oid = ObjectId(project_id)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid project_id"
            ) from exc
        project = await self.projects.find_one({"_id": project_oid})
        if not project:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Project not found."
            )
        return project

    def _download_asset_file(self, key: str) -> str:
        suffix = Path(key).suffix or ".mp4"
        fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        try:
            s3.download_file(self.bucket, key, tmp_path)
        except Exception as exc:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            logger.exception("Failed to download asset from S3: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to download project asset.",
            ) from exc
        return tmp_path

    def _upload_video_to_youtube(
        self,
        creds: Credentials,
        payload: YoutubePublishRequest,
        project_title: str,
        file_path: str,
    ) -> dict[str, Any]:
        youtube = build("youtube", "v3", credentials=creds)
        description = payload.description or (
            f"Dupilot generated dubbing for '{project_title}' "
            f"in language {payload.language_code}."
        )
        tags = payload.tags or ["Dupilot", payload.language_code]
        media = MediaFileUpload(file_path, chunksize=8 * 1024 * 1024, resumable=True)
        body = {
            "snippet": {
                "title": payload.title,
                "description": description,
                "tags": tags,
            },
            "status": {"privacyStatus": payload.privacy_status.value},
        }

        try:
            request = youtube.videos().insert(
                part="snippet,status",
                body=body,
                media_body=media,
            )
            response = None
            while response is None:
                _, response = request.next_chunk()
            return response
        except HttpError as exc:
            logger.error("YouTube upload failed: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Uploading video to YouTube failed.",
            ) from exc

    async def _build_credentials(self, user: dict[str, Any]) -> Credentials:
        auth = user.get("youtube_auth")
        if not auth:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You need to connect a YouTube channel first.",
            )
        try:
            access_token = decrypt_text(auth["access_token"])
            refresh_token = decrypt_text(auth["refresh_token"])
        except (KeyError, EncryptionError) as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Stored YouTube credentials are invalid.",
            ) from exc
        expires_at = self._as_utc_datetime(auth.get("expires_at"))
        creds = Credentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri=self.TOKEN_ENDPOINT,
            client_id=self.client_id,
            client_secret=self.client_secret,
            scopes=self.scopes,
            expiry=expires_at,
        )
        user_oid = user.get("_id")
        if not isinstance(user_oid, ObjectId):
            user_oid = self._as_object_id(str(user_oid))
        await self._ensure_fresh_credentials(creds, user_oid)
        return creds

    async def _ensure_fresh_credentials(
        self, creds: Credentials, user_oid: ObjectId
    ) -> None:
        if self._needs_refresh(creds) and creds.refresh_token:
            await asyncio.to_thread(self._refresh_credentials_sync, creds)
            token_data = self._token_data_from_credentials(creds)
            await self._persist_tokens(user_oid, token_data, None)

    def _refresh_credentials_sync(self, creds: Credentials) -> None:
        creds.refresh(google_requests.Request())

    def _token_data_from_credentials(self, creds: Credentials) -> dict[str, Any]:
        expires_in = 3600
        expiry = self._as_utc_datetime(creds.expiry)
        if expiry:
            expires_in = max(int((expiry - datetime.now(timezone.utc)).total_seconds()), 0)
        return {
            "access_token": creds.token,
            "refresh_token": creds.refresh_token,
            "expires_in": expires_in,
            "scope": " ".join(creds.scopes or []),
            "token_type": "Bearer",
        }

    def _needs_refresh(self, creds: Credentials) -> bool:
        expiry = self._as_utc_datetime(creds.expiry)
        if not expiry:
            return False
        skewed_expiry = expiry - timedelta(seconds=60)
        now = datetime.now(timezone.utc)
        if now >= skewed_expiry:
            return True
        # naive/aware 비교로 인한 예외에 대비하여, google-auth가 직접 판별하게 위임
        try:
            return creds.expired
        except TypeError:
            return True
