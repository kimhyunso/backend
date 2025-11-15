from fastapi import APIRouter, Depends, status

from app.api.auth.model import UserOut
from app.api.auth.service import get_current_user_from_cookie

from .models import (
    YoutubeConnectionInfo,
    YoutubeOAuthCallbackRequest,
    YoutubeOAuthStartResponse,
    YoutubePublishRequest,
    YoutubePublishResponse,
)
from .service import YoutubeIntegrationService


youtube_router = APIRouter(prefix="/youtube", tags=["YouTube"])


@youtube_router.get("/status", response_model=YoutubeConnectionInfo)
async def get_youtube_status(
    current_user: UserOut = Depends(get_current_user_from_cookie),
    service: YoutubeIntegrationService = Depends(YoutubeIntegrationService),
) -> YoutubeConnectionInfo:
    return await service.get_status(current_user)


@youtube_router.post(
    "/oauth/start", response_model=YoutubeOAuthStartResponse, status_code=status.HTTP_201_CREATED
)
async def start_youtube_oauth(
    current_user: UserOut = Depends(get_current_user_from_cookie),
    service: YoutubeIntegrationService = Depends(YoutubeIntegrationService),
) -> YoutubeOAuthStartResponse:
    return await service.start_oauth_flow(current_user)


@youtube_router.post(
    "/oauth/callback", response_model=YoutubeConnectionInfo, status_code=status.HTTP_200_OK
)
async def finalize_youtube_oauth(
    payload: YoutubeOAuthCallbackRequest,
    current_user: UserOut = Depends(get_current_user_from_cookie),
    service: YoutubeIntegrationService = Depends(YoutubeIntegrationService),
) -> YoutubeConnectionInfo:
    return await service.complete_oauth_flow(current_user, payload)


@youtube_router.delete("/connection", status_code=status.HTTP_204_NO_CONTENT)
async def disconnect_youtube(
    current_user: UserOut = Depends(get_current_user_from_cookie),
    service: YoutubeIntegrationService = Depends(YoutubeIntegrationService),
) -> None:
    await service.disconnect(current_user)
    return None


@youtube_router.post(
    "/publish", response_model=YoutubePublishResponse, status_code=status.HTTP_202_ACCEPTED
)
async def publish_to_youtube(
    payload: YoutubePublishRequest,
    current_user: UserOut = Depends(get_current_user_from_cookie),
    service: YoutubeIntegrationService = Depends(YoutubeIntegrationService),
) -> YoutubePublishResponse:
    return await service.publish_video(current_user, payload)
