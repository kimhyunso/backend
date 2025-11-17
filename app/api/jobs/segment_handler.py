"""
Jobs API 세그먼트 처리 로직
"""

import logging
from datetime import datetime
from typing import Optional

from ..deps import DbDep
from ..segment.segment_service import SegmentService
from ..segment.service import SegmentService as SegmentTranslationService
from ..assets.service import AssetService
from ..assets.models import AssetCreate, AssetType
from app.utils.s3 import download_metadata_from_s3, parse_segments_from_metadata
from app.utils.audio import get_audio_duration_from_s3
from .job_utils import (
    find_segment_id_from_metadata,
    validate_segment_exists,
    extract_error_message,
)
from .event_dispatcher import dispatch_audio_completed

logger = logging.getLogger(__name__)


async def create_asset_from_result(
    db: DbDep,
    project_id: str,
    target_lang: str,
    result_key: str,
) -> None:
    """완료된 비디오에 대한 asset 생성"""
    try:
        asset_service = AssetService(db)
        asset_payload = AssetCreate(
            project_id=project_id,
            language_code=target_lang,
            asset_type=AssetType.PREVIEW,
            file_path=result_key,
        )
        await asset_service.create_asset(asset_payload)
    except Exception as exc:
        logger.error(f"Failed to create asset: {exc}")


async def check_and_create_segments(
    db: DbDep,
    project_id: str,
    segments: list,
    target_lang: str,
    translated_texts: Optional[list[str]] = None,
) -> bool:
    """
    세그먼트 생성 - 첫 번째 타겟 언어일 때만 project_segments 생성, 번역은 항상 생성

    Args:
        db: Database connection
        project_id: 프로젝트 ID
        segments: 세그먼트 리스트 (기존 포맷 또는 새 포맷)
        target_lang: 타겟 언어 코드
        translated_texts: 번역된 텍스트 리스트 (새 포맷용, segments와 같은 순서)
    """
    segment_service = SegmentService(db)

    # 이미 세그먼트가 있는지 확인
    try:
        existing_segments = await segment_service.get_segments_by_project(project_id)
    except Exception:
        existing_segments = None

    now = datetime.now()
    segments_created = False
    segment_ids_map = {}  # segment_index -> _id 매핑

    # 기존 세그먼트가 없으면 생성
    if not existing_segments:
        segments_to_create = []

        for i, seg in enumerate(segments):
            # 새 포맷 vs 기존 포맷 구분
            # 새 포맷: {"segment_index": 0, "speaker_tag": "SPEAKER_00", "start": 0.217, "end": 13.426, "source_text": "..."}
            # 기존 포맷: {"segment_id": ..., "seg_idx": ..., "speaker": ..., "start": ..., "end": ..., "prompt_text": ...}

            if "speaker_tag" in seg:
                # 새 포맷 (parse_segments_from_metadata에서 생성된 포맷)
                segment_data = {
                    "project_id": project_id,
                    "speaker_tag": seg.get("speaker_tag", ""),
                    "start": float(seg.get("start", 0)),
                    "end": float(seg.get("end", 0)),
                    "source_text": seg.get("source_text", ""),
                    "segment_index": seg.get("segment_index", i),
                    "is_verified": False,
                    "created_at": now,
                    "updated_at": now,
                }
            else:
                # 기존 포맷 (워커에서 오는 데이터)
                segment_data = {
                    "project_id": project_id,
                    "speaker_tag": seg.get("speaker", ""),
                    "start": float(seg.get("start", 0)),
                    "end": float(seg.get("end", 0)),
                    "source_text": seg.get("source_text", ""),
                    "is_verified": False,
                    "created_at": now,
                    "updated_at": now,
                }

                # segment_index 추가 (순서 보장)
                if "seg_idx" in seg:
                    segment_data["segment_index"] = int(seg["seg_idx"])
                elif "segment_id" in seg:
                    try:
                        segment_data["segment_index"] = int(seg["segment_id"])
                    except (ValueError, TypeError):
                        segment_data["segment_index"] = i
                else:
                    segment_data["segment_index"] = i

            segments_to_create.append(segment_data)

        if segments_to_create:
            try:
                result = await db["project_segments"].insert_many(segments_to_create)
                # 생성된 segment ID 저장
                for idx, seg_id in enumerate(result.inserted_ids):
                    segment_ids_map[segments_to_create[idx]["segment_index"]] = seg_id

                segments_created = True
            except Exception as exc:
                logger.error(f"Failed to create segments: {exc}")
                return False
    else:
        # 기존 세그먼트가 있으면 ID 매핑만 생성
        for seg in existing_segments:
            segment_ids_map[seg.get("segment_index", 0)] = seg["_id"]

    # 번역 세그먼트 생성 (타겟 언어별로 생성)
    if segments and target_lang:
        translations_to_create = []

        for i, seg in enumerate(segments):
            # segment_index 결정
            if "segment_index" in seg:
                # 새 포맷
                seg_index = seg["segment_index"]
            elif "seg_idx" in seg:
                # 기존 포맷
                seg_index = int(seg["seg_idx"])
            elif "segment_id" in seg:
                try:
                    seg_index = int(seg["segment_id"])
                except (ValueError, TypeError):
                    seg_index = i
            else:
                seg_index = i

            # 해당 segment의 _id 찾기
            segment_obj_id = segment_ids_map.get(seg_index)
            if not segment_obj_id:
                logger.warning(
                    f"Cannot find segment_id for index {seg_index}, skipping translation"
                )
                continue

            # 번역된 텍스트 추출
            # 새 포맷: translated_texts 리스트에서 가져옴
            # 기존 포맷: prompt_text가 번역된 텍스트임
            if translated_texts and i < len(translated_texts):
                # 새 포맷 사용
                translated_text = translated_texts[i]
                # 새 포맷에서도 audio_file이 segments에 포함될 수 있음
                audio_url = seg.get("audio_file")
            else:
                # 기존 포맷 사용
                translated_text = seg.get("prompt_text", "")
                audio_url = seg.get("audio_file")  # TTS 오디오 파일 경로

            translation_data = {
                "segment_id": str(segment_obj_id),
                "language_code": target_lang,
                "target_text": translated_text,
                "segment_audio_url": audio_url,
                "created_at": now,
                "updated_at": now,
            }
            translations_to_create.append(translation_data)

        if translations_to_create:
            try:
                # 기존 번역이 있는지 확인하고 업데이트 또는 생성
                for trans in translations_to_create:
                    await db["segment_translations"].update_one(
                        {
                            "segment_id": trans["segment_id"],
                            "language_code": trans["language_code"],
                        },
                        {"$set": trans},
                        upsert=True,
                    )

            except Exception as exc:
                logger.error(f"Failed to create segment translations: {exc}")

    return segments_created or len(existing_segments) > 0


async def process_md_completion(
    db: DbDep,
    project_id: str,
    metadata: dict,
    result_key: str,
    defaultTarget: Optional[str] = None,
) -> None:
    """
    Done 시 처리: asset 생성, 세그먼트 생성, 번역 저장

    metadata 포맷:
    1. 기존 포맷: {"target_lang": "en", "segments": [{...}]}
    2. 새 포맷: {"target_lang": "en", "metadata_key": "s3://path/to/metadata.json"}
    """
    target_lang = metadata.get("target_lang") or defaultTarget
    if not target_lang:
        logger.warning(
            f"No target_lang in metadata or defaultTarget for project {project_id}"
        )
        return

    # 1. Asset 생성 (완성된 더빙 비디오)
    if result_key:
        await create_asset_from_result(db, project_id, target_lang, result_key)

    # 2. 세그먼트 및 번역 생성
    # metadata_key가 있으면 S3에서 metadata를 다운로드
    metadata_key = metadata.get("metadata_key")

    if metadata_key:
        # 새 포맷: S3에서 metadata 다운로드
        try:
            s3_metadata = await download_metadata_from_s3(metadata_key)

            # metadata 파싱하여 segments와 translations 추출
            segments, parsed_translations = parse_segments_from_metadata(s3_metadata)

            # 번역된 텍스트: S3 메타데이터에서 파싱된 것 우선, 없으면 콜백 metadata에서
            translated_texts = (
                parsed_translations
                or metadata.get("translations")
                or metadata.get("translated_texts")
            )

            if segments:
                await check_and_create_segments(
                    db,
                    project_id,
                    segments,
                    target_lang,
                    translated_texts=translated_texts,
                )
            else:
                logger.warning(
                    f"No segments found in S3 metadata for project {project_id}"
                )
        except Exception as exc:
            logger.error(f"Failed to process S3 metadata: {exc}")
            # S3 메타데이터 처리 실패 시 기존 방식으로 fallback
            segments = metadata.get("segments", [])
            if segments:
                await check_and_create_segments(db, project_id, segments, target_lang)
    else:
        # 기존 포맷: metadata에 직접 segments가 포함됨
        segments = metadata.get("segments", [])
        if segments:
            await check_and_create_segments(db, project_id, segments, target_lang)
        else:
            logger.warning(
                f"No segments in metadata for project {project_id}, language {target_lang}"
            )


async def tts_complete_processing(db: DbDep, project_id: str, segments: list):
    """기존 호환성 유지를 위한 함수"""
    # 세그먼트 Insert_many
    segment_service = SegmentService(db)
    await segment_service.insert_segments_from_metadata(project_id, segments)


async def process_segment_tts_completed(
    db: DbDep,
    project_id: str,
    language_code: str,
    metadata: dict,
) -> None:
    """세그먼트 TTS 재생성 완료 처리 - 리팩토링된 버전"""
    if not metadata.get("segments"):
        logger.warning(
            f"⚠️ [segment_tts_completed] No segments in metadata for project {project_id}"
        )
        return

    try:
        segment_translation_service = SegmentTranslationService(db)
        segments_result = metadata.get("segments", [])

        # 유틸리티 함수로 segment_id 찾기
        segment_id = await find_segment_id_from_metadata(db, project_id, metadata)

        if not segment_id:
            logger.error(
                f"❌ [segment_tts_completed] Cannot find segment_id from metadata or segments"
            )
            return

        # segment 유효성 검사
        segment_doc = await validate_segment_exists(db, segment_id)
        if not segment_doc:
            return

        logger.info(
            f"✅ [segment_tts_completed] Found segment: segment_id={segment_id}, "
            f"segment_index={segment_doc.get('segment_index')}"
        )

        # segments_result에서 audio_key 가져오기 및 업데이트
        for seg_result in segments_result:
            audio_key = seg_result.get("audio_key")

            if not audio_key:
                logger.warning(
                    f"⚠️ [segment_tts_completed] No audio_key in segment result: {seg_result}"
                )
                continue

            logger.info(
                f"🔍 [segment_tts_completed] Processing segment: segment_id={segment_id}, audio_key={audio_key}"
            )

            # segment_translations에서 해당 segment_id와 language_code로 번역 찾기
            translation_doc = await db["segment_translations"].find_one(
                {"segment_id": segment_id, "language_code": language_code}
            )

            if translation_doc:
                # segment_audio_url 업데이트
                translation_id = str(translation_doc["_id"])
                # audio_key를 URL 형식으로 변환 (필요시)
                audio_url = (
                    f"{audio_key}"
                    if not audio_key.startswith("/")
                    and not audio_key.startswith("http")
                    else audio_key
                )

                logger.info(
                    f"🔄 [segment_tts_completed] Updating translation {translation_id} with audio_url: {audio_url}"
                )

                await segment_translation_service.update_translation(
                    translation_id=translation_id,
                    segment_audio_url=audio_url,
                )

                # 오디오 duration 구하고 SSE 이벤트 발송
                try:
                    audio_duration = await get_audio_duration_from_s3(audio_key)
                    if audio_duration is not None:
                        logger.info(
                            f"✅ [segment_tts_completed] Got audio duration: {audio_duration}s for {audio_key}"
                        )
                        # SSE 이벤트 발송 (성공)
                        await dispatch_audio_completed(
                            project_id=project_id,
                            language_code=language_code,
                            segment_id=segment_id,
                            audio_s3_key=audio_key,
                            audio_duration=audio_duration,
                            status="completed",
                        )
                    else:
                        logger.warning(
                            f"⚠️ [segment_tts_completed] Failed to get audio duration for {audio_key}"
                        )
                except Exception as duration_exc:
                    logger.error(
                        f"❌ [segment_tts_completed] Error getting audio duration: {duration_exc}",
                        exc_info=True,
                    )

            else:
                logger.warning(
                    f"⚠️ [segment_tts_completed] Translation not found for segment {segment_id}, language {language_code}"
                )
                # 디버깅: 해당 segment_id로 모든 번역 조회
                all_translations = (
                    await db["segment_translations"]
                    .find({"segment_id": segment_id})
                    .to_list(None)
                )
                logger.info(
                    f"🔍 [segment_tts_completed] All translations for segment {segment_id}: {all_translations}"
                )

            # 첫 번째 audio_key만 처리 (단일 세그먼트이므로)
            break

    except Exception as exc:
        logger.error(
            f"❌ [segment_tts_completed] Failed to update segment_audio_url for project {project_id}: {exc}",
            exc_info=True,
        )


async def process_segment_tts_failed(
    db: DbDep,
    project_id: str,
    language_code: str,
    metadata: dict,
) -> None:
    """세그먼트 TTS 재생성 실패 처리 - 리팩토링된 버전"""
    try:
        # 유틸리티 함수로 segment_id 찾기
        segment_id = await find_segment_id_from_metadata(db, project_id, metadata)

        if not segment_id:
            logger.error(
                f"❌ [segment_tts_failed] Cannot find segment_id from metadata"
            )
            return

        # 에러 메시지 추출 (유틸리티 함수 사용)
        error_message = extract_error_message(metadata, "TTS generation failed")

        logger.error(
            f"❌ [segment_tts_failed] Segment TTS failed: project_id={project_id}, "
            f"segment_id={segment_id}, language_code={language_code}, error={error_message}"
        )

        # SSE 이벤트 발송 (실패)
        await dispatch_audio_completed(
            project_id=project_id,
            language_code=language_code,
            segment_id=segment_id,
            status="failed",
            error_message=error_message,
        )

    except Exception as exc:
        logger.error(
            f"❌ [segment_tts_failed] Failed to process segment_tts_failed: {exc}",
            exc_info=True,
        )
