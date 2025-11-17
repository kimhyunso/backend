"""
Jobs API 공통 유틸리티 함수
"""
import logging
from typing import Optional
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)


async def find_segment_id_from_metadata(
    db: AsyncIOMotorDatabase,
    project_id: str,
    metadata: dict,
) -> Optional[str]:
    """
    metadata에서 segment_id를 찾습니다.

    1. metadata.segment_id 우선
    2. metadata.segments[0].index로 project_segments에서 조회

    Args:
        db: Database connection
        project_id: 프로젝트 ID
        metadata: 콜백 metadata

    Returns:
        segment_id (str) 또는 None
    """
    # metadata에서 segment_id 직접 가져오기
    segment_id = metadata.get("segment_id")

    if segment_id:
        return segment_id

    # segments에서 index로 찾기
    segments_result = metadata.get("segments", [])
    if not segments_result:
        return None

    seg_result = segments_result[0]
    segment_index = seg_result.get("index")

    if segment_index is None:
        return None

    try:
        project_oid = (
            ObjectId(project_id)
            if isinstance(project_id, str)
            else project_id
        )
        segment_doc = await db["project_segments"].find_one(
            {
                "project_id": project_oid,
                "segment_index": segment_index,
            }
        )

        if segment_doc:
            return str(segment_doc["_id"])

    except Exception as exc:
        logger.error(
            f"Error finding segment by index {segment_index}: {exc}"
        )

    return None


async def validate_segment_exists(
    db: AsyncIOMotorDatabase,
    segment_id: str,
) -> Optional[dict]:
    """
    segment_id가 유효하고 존재하는지 확인합니다.

    Args:
        db: Database connection
        segment_id: 세그먼트 ID

    Returns:
        segment document 또는 None
    """
    try:
        segment_oid = ObjectId(segment_id)
    except Exception as exc:
        logger.error(
            f"Invalid segment_id format: {segment_id}, error: {exc}"
        )
        return None

    segment_doc = await db["project_segments"].find_one({"_id": segment_oid})

    if not segment_doc:
        logger.warning(f"Segment not found: {segment_id}")
        return None

    return segment_doc


def extract_error_message(metadata: dict, default: str = "Operation failed") -> str:
    """
    metadata에서 에러 메시지를 추출합니다.

    Args:
        metadata: 콜백 metadata
        default: 기본 에러 메시지

    Returns:
        에러 메시지 문자열
    """
    return metadata.get("error") or default


def convert_to_object_id(id_value: str | ObjectId) -> ObjectId:
    """
    문자열 또는 ObjectId를 ObjectId로 변환합니다.

    Args:
        id_value: 변환할 ID

    Returns:
        ObjectId
    """
    if isinstance(id_value, str):
        return ObjectId(id_value)
    return id_value
