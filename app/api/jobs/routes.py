from fastapi import APIRouter
from datetime import datetime

import logging
from ..deps import DbDep
from .models import JobRead, JobUpdateStatus
from .service import get_job, update_job_status
from ..pipeline.service import update_pipeline_stage
from ..pipeline.models import PipelineUpdate, PipelineStatus
from ..translate.service import suggestion_by_project
from app.api.pipeline.router import project_channels
from ..segment.segment_service import SegmentService
from ..segment.service import SegmentService as SegmentTranslationService
from ..auth.service import AuthService
from ..auth.model import UserOut
from ..voice_samples.service import VoiceSampleService
from ..voice_samples.models import VoiceSampleUpdate
from ..project.models import ProjectTargetUpdate, ProjectTargetStatus, ProjectUpdate
from ..project.service import ProjectService
from ..assets.service import AssetService
from ..assets.models import AssetCreate, AssetType
from app.utils.project_utils import extract_language_code
from app.utils.s3 import download_metadata_from_s3, parse_segments_from_metadata
from app.utils.audio import get_audio_duration_from_s3

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/project/{project_id}")
async def get_jobs_by_project(project_id: str, db: DbDep):
    jobs = []
    cursor = db["jobs"].find({"project_id": project_id})
    async for job in cursor:
        # MongoDB의 _id를 id로 변환
        job["id"] = str(job.pop("_id"))
        jobs.append(JobRead(**job))
    return jobs


@router.get("/{job_id}", response_model=JobRead)
async def read_job(job_id: str, db: DbDep) -> JobRead:
    return await get_job(db, job_id)


async def dispatch_pipeline(project_id: str, update_payload):
    listeners = project_channels.get(project_id, set())
    event = {
        "project_id": project_id,
        "stage": update_payload.get("stage_id"),
        "status": update_payload.get("status", PipelineStatus.PROCESSING).value,
        "progress": update_payload.get("progress"),
        "timestamp": datetime.now().isoformat() + "Z",
    }
    for queue in list(listeners):
        await queue.put(event)


async def dispatch_target_update(
    project_id: str,
    language_code: str,
    target_status: ProjectTargetStatus,
    progress: int,
):
    """project_target 업데이트를 SSE로 브로드캐스트"""
    listeners = project_channels.get(project_id, set())
    event = {
        "project_id": project_id,
        "type": "target_update",
        "language_code": language_code,
        "status": target_status.value,
        "progress": progress,
        "timestamp": datetime.now().isoformat() + "Z",
    }
    for queue in list(listeners):
        await queue.put(event)


async def dispatch_audio_completed(
    project_id: str,
    language_code: str,
    segment_id: str,
    audio_s3_key: str,
    audio_duration: float,
):
    """오디오 생성 완료 이벤트를 SSE로 브로드캐스트"""
    from ..audio.router import audio_channels

    channel_key = f"{project_id}:{language_code}"
    listeners = audio_channels.get(channel_key, set())

    event = {
        "event": "audio-completed",
        "data": {
            "segmentId": segment_id,
            "audioS3Key": audio_s3_key,
            "audioDuration": audio_duration,
            "projectId": project_id,
            "languageCode": language_code,
        },
    }

    for queue in list(listeners):
        await queue.put(event)


async def update_pipeline(db, project_id, payload):
    # 파이프라인 디비 수정
    await update_pipeline_stage(db, PipelineUpdate(**payload))
    # 파이프라인 SSE 큐에 추가
    await dispatch_pipeline(project_id, payload)


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
    translated_texts: list[str] | None = None,
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
    defaultTarget: str = None,
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


async def tts_complete_processing(db, project_id, segments):
    """기존 호환성 유지를 위한 함수"""
    # 세그먼트 Insert_many
    segment_service = SegmentService(db)
    await segment_service.insert_segments_from_metadata(project_id, segments)


@router.post("/{job_id}/status", response_model=JobRead)
async def set_job_status(job_id: str, payload: JobUpdateStatus, db: DbDep) -> JobRead:
    # job 상태 업데이트
    result = await update_job_status(db, job_id, payload)

    metadata = None
    if payload.metadata is not None:
        if payload.metadata is not None:
            metadata = (
                payload.metadata.model_dump()
                if hasattr(payload.metadata, "model_dump")
                else payload.metadata
            )
    # voice_sample_id가 있으면 audio_sample_url 업데이트
    if metadata and "voice_sample_id" in metadata:
        if result.status == "done":
            voice_sample_id = metadata["voice_sample_id"]
            try:
                service = VoiceSampleService(db)

                # 샘플을 직접 DB에서 조회 (owner_id만 필요)
                from bson import ObjectId

                try:
                    sample_oid = ObjectId(voice_sample_id)
                    sample_doc = await service.collection.find_one({"_id": sample_oid})
                    if sample_doc:
                        # owner_id로 사용자 조회
                        auth_service = AuthService(db)
                        owner_oid = sample_doc["owner_id"]
                        user_doc = await auth_service.collection.find_one(
                            {"_id": owner_oid}
                        )
                        if user_doc:
                            owner = UserOut(**user_doc)
                            # 업데이트할 데이터 구성
                            update_data = {}

                            # audio_sample_url 업데이트 (워커에서 보낸 값 우선, 없으면 result_key로 생성)
                            audio_sample_url = metadata.get("audio_sample_url")
                            if not audio_sample_url and result.result_key:
                                audio_sample_url = (
                                    f"/api/storage/media/{result.result_key}"
                                )

                            if audio_sample_url:
                                update_data["audio_sample_url"] = audio_sample_url

                            # prompt_text 업데이트
                            prompt_text = metadata.get("prompt_text")
                            if prompt_text:
                                update_data["prompt_text"] = prompt_text

                            if update_data:
                                await service.update_voice_sample(
                                    voice_sample_id,
                                    VoiceSampleUpdate(**update_data),
                                    owner,
                                )

                except Exception as owner_exc:
                    logger.error(
                        f"Failed to get owner for voice sample {voice_sample_id}: {owner_exc}"
                    )
            except Exception as exc:
                logger.error(
                    f"Failed to update audio_sample_url for voice sample {voice_sample_id}: {exc}"
                )

    # state 없을 때 리턴
    if not metadata or "stage" not in metadata:
        return result

    stage = metadata["stage"]
    project_id = result.project_id

    print(f"metadata for job {job_id}, stage {stage}: {metadata}")

    # metadata에서 language_code 추출 (target_lang)
    language_code = metadata.get("target_lang") or metadata.get("language_code")

    # 특정 stage에서는 language_code가 필요하지 않을 수 있음
    language_independent_stages = ["downloaded", "stt_completed"]

    if not language_code and stage not in language_independent_stages:
        logger.warning(f"No target_lang in metadata for job {job_id}, stage {stage}")
        # language_code가 없는 경우, project의 첫 번째 target language 사용 시도
        try:
            project_service = ProjectService(db)
            targets = await project_service.get_targets_by_project(project_id)
            if targets and len(targets) > 0:
                # 유틸 함수로 첫 번째 타겟의 언어 코드 추출
                language_code = extract_language_code(targets[0])

        except Exception as exc:
            logger.error(f"Failed to get project targets: {exc}")

    if not language_code and stage not in language_independent_stages:
        logger.error(f"Cannot determine language_code for job {job_id}, stage {stage}")
        return result

    # ProjectService 인스턴스 생성
    project_service = ProjectService(db)

    # stage별 project_target 업데이트를 위한 payload
    target_update = None

    # stage별, project target 업데이트
    if stage == "starting":  # s3에서 불러오기 완료 (stt 시작)
        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.PROCESSING, progress=1
        )
    elif stage == "asr_started":  # stt 시작
        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.PROCESSING, progress=10  # STT 시작 시 10%
        )
    elif stage == "asr_completed":  # stt 완료
        # 원본 오디오, 발화 음성, 배경음, 오디오 제거 비디오 경로를 프로젝트에 저장
        if metadata:
            audio_key = metadata.get("audio_key")  # 원본 오디오 (mp4->wav)
            vocals_key = metadata.get("vocals_key")  # 발화 음성 (vocals.wav)
            background_key = metadata.get("background_key")  # 배경음

            if audio_key or vocals_key or background_key:
                update_data = {}

                if audio_key:
                    update_data["audio_source"] = audio_key  # 원본 오디오

                if vocals_key:
                    update_data["vocal_source"] = vocals_key  # 발화 음성

                if background_key:
                    update_data["background_audio_source"] = background_key

                if update_data:
                    try:
                        await project_service.update_project(
                            ProjectUpdate(project_id=project_id, **update_data)
                        )
                        logger.info(
                            f"Updated project {project_id} with audio/video files: "
                            f"audio_source={update_data.get('audio_source', 'N/A')}, "
                            f"vocal_source={update_data.get('vocal_source', 'N/A')}, "
                        )
                    except Exception as exc:
                        logger.error(
                            f"Failed to update project audio/video files: {exc}"
                        )

        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.PROCESSING, progress=20  # STT 완료 시 20%
        )
    elif stage == "translation_started":
        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.PROCESSING, progress=21  # MT 시작 시 25%
        )
    elif stage == "translation_completed":  # mt 완료
        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.PROCESSING, progress=35  # MT 완료 시 50%
        )
    elif stage == "tts_started":  # TTS 시작
        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.PROCESSING, progress=36  # TTS 시작 시 55%
        )
    elif stage == "tts_completed":  # TTS 완료
        # speaker_voices를 default_speaker_voices 형식으로 변환하여 프로젝트에 저장
        if metadata and metadata.get("speaker_voices") and language_code:
            try:
                speaker_voices = metadata.get("speaker_voices", {})
                # 형식 변환: {speaker: {ref_wav_key, prompt_text}} -> {target_lang: {speaker: {ref_wav_key, prompt_text}}}
                default_speaker_voices = {language_code: speaker_voices}

                await project_service.update_project(
                    ProjectUpdate(
                        project_id=project_id,
                        default_speaker_voices=default_speaker_voices,
                    )
                )
                logger.info(
                    f"Updated project {project_id} with default_speaker_voices for language {language_code}"
                )
            except Exception as exc:
                logger.error(
                    f"Failed to update default_speaker_voices for project {project_id}: {exc}"
                )

        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.COMPLETED, progress=70  # TTS 완료
        )
    elif stage == "segment_tts_completed":  # 세그먼트 TTS 재생성 완료
        # TTS된 음성의 key를 segment_translations의 segment_audio_url에 업데이트

        if metadata and metadata.get("segments") and language_code:
            try:
                from bson import ObjectId

                segment_translation_service = SegmentTranslationService(db)

                segments_result = metadata.get("segments", [])

                # metadata에서 segment_id 가져오기 (task_payload에서 전달됨)
                segment_id = metadata.get("segment_id")

                if not segment_id:

                    # 폴백: segments의 첫 번째 항목에서 index로 찾기
                    if segments_result:
                        seg_result = segments_result[0]
                        segment_index = seg_result.get("index")

                        if segment_index is not None:
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
                                segment_id = str(segment_doc["_id"])

                if not segment_id:
                    logger.error(
                        f"❌ [segment_tts_completed] Cannot find segment_id from metadata or segments"
                    )
                else:
                    # segment_id로 segment 확인
                    try:
                        segment_oid = ObjectId(segment_id)
                    except Exception as exc:
                        logger.error(
                            f"❌ [segment_tts_completed] Invalid segment_id format: {segment_id}, error: {exc}"
                        )
                        segment_id = None

                    if segment_id:
                        segment_doc = await db["project_segments"].find_one(
                            {"_id": segment_oid}
                        )
                        if not segment_doc:
                            logger.warning(
                                f"⚠️ [segment_tts_completed] Segment not found: {segment_id}"
                            )
                            segment_id = None
                        else:
                            logger.info(
                                f"✅ [segment_tts_completed] Found segment: segment_id={segment_id}, segment_index={segment_doc.get('segment_index')}"
                            )

                # segments_result에서 audio_key 가져오기
                if segment_id:
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
                                audio_duration = await get_audio_duration_from_s3(
                                    audio_key
                                )
                                if audio_duration is not None:
                                    logger.info(
                                        f"✅ [segment_tts_completed] Got audio duration: {audio_duration}s for {audio_key}"
                                    )
                                    # SSE 이벤트 발송
                                    await dispatch_audio_completed(
                                        project_id=project_id,
                                        language_code=language_code,
                                        segment_id=segment_id,
                                        audio_s3_key=audio_key,
                                        audio_duration=audio_duration,
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

        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.COMPLETED, progress=70  # 세그먼트 TTS 완료
        )
    elif stage == "mux_started":  # 비디오 처리 시작
        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.PROCESSING,
            progress=71,  # 비디오 처리 시작 시 70%
        )
    elif stage == "done":  # 비디오 처리 완료
        # speaker_refs 또는 speaker_voices가 있으면 저장 (tts_completed를 건너뛴 경우 대비)
        if metadata and language_code:
            speaker_voices = metadata.get("speaker_voices") or metadata.get(
                "speaker_refs"
            )
            if speaker_voices:
                try:
                    from bson import ObjectId

                    # 기존 default_speaker_voices를 가져와서 병합 (다른 언어 데이터 보존)
                    project_oid = ObjectId(project_id)
                    project_doc = await db["projects"].find_one({"_id": project_oid})
                    existing_default_speaker_voices = (
                        project_doc.get("default_speaker_voices", {})
                        if project_doc
                        else {}
                    )

                    # 새로운 언어 데이터 추가 (기존 데이터 유지)
                    updated_default_speaker_voices = {
                        **existing_default_speaker_voices,
                        language_code: speaker_voices,
                    }

                    await project_service.update_project(
                        ProjectUpdate(
                            project_id=project_id,
                            default_speaker_voices=updated_default_speaker_voices,
                        )
                    )

                    # 저장 확인: 업데이트 후 다시 조회하여 확인
                    verify_doc = await db["projects"].find_one({"_id": project_oid})
                    saved_voices = (
                        verify_doc.get("default_speaker_voices", {})
                        if verify_doc
                        else {}
                    )

                except Exception as exc:
                    logger.error(
                        f"❌ [done] Failed to update default_speaker_voices for project {project_id}: {exc}",
                        exc_info=True,
                    )

        # 새로운 처리 함수 호출: asset 생성 및 세그먼트 생성
        # result_key는 metadata 또는 result에서 가져옴
        final_result_key = metadata.get("result_key") or result.result_key

        await process_md_completion(
            db, project_id, metadata, final_result_key, defaultTarget=language_code
        )

        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.COMPLETED,
            progress=100,  # 비디오 처리 완료 시 100%
        )
    elif stage == "failed":  # 실패
        target_update = ProjectTargetUpdate(
            status=ProjectTargetStatus.FAILED, progress=0
        )

    print(f"target_lang for job {job_id}, stage {stage}: {language_code}")

    # project_target 업데이트 실행
    if target_update:
        try:
            # language_code가 있으면 해당 언어만 업데이트
            if language_code:
                await project_service.update_targets_by_project_and_language(
                    project_id, language_code, target_update
                )

                # SSE 이벤트 브로드캐스트
                await dispatch_target_update(
                    project_id,
                    language_code,
                    target_update.status or ProjectTargetStatus.PROCESSING,
                    target_update.progress or 0,
                )

        except Exception as exc:
            logger.error(f"Failed to update project_target: {exc}")

    return result
