import os
import asyncio
import tempfile
import logging
from pathlib import Path
from typing import Optional

from app.config.s3 import s3 as s3_client

logger = logging.getLogger(__name__)
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "dupilot-dev-media")


def ffprobe_duration_sync(file_path: str) -> float:
    """오디오 파일의 길이(초)를 반환 (동기)"""
    import subprocess

    try:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            file_path,
        ]
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if result.returncode == 0:
            return float(result.stdout.strip())
        raise RuntimeError(f"ffprobe failed: {result.stderr}")
    except Exception as exc:
        logger.error(f"Failed to get audio duration: {exc}")
        raise


async def get_audio_duration_from_s3(s3_key: str) -> Optional[float]:
    """
    S3에서 오디오 파일을 다운로드하여 duration을 구합니다.

    Args:
        s3_key: S3 객체 키

    Returns:
        오디오 길이(초), 실패 시 None
    """
    if not s3_key:
        logger.warning("s3_key is empty")
        return None

    tmp_path = None
    try:
        # S3에서 파일 존재 확인
        try:
            await asyncio.to_thread(
                s3_client.head_object,
                Bucket=AWS_S3_BUCKET,
                Key=s3_key,
            )
        except Exception as exc:
            logger.error(f"S3 file not found: {s3_key}, error: {exc}")
            return None

        # 임시 파일 생성
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp_file:
            tmp_path = Path(tmp_file.name)

        # S3에서 파일 다운로드
        response = await asyncio.to_thread(
            s3_client.get_object,
            Bucket=AWS_S3_BUCKET,
            Key=s3_key,
        )

        # 파일 저장
        with open(tmp_path, "wb") as f:
            for chunk in response["Body"].iter_chunks(chunk_size=8192):
                f.write(chunk)

        # ffprobe로 duration 구하기
        duration = await asyncio.to_thread(ffprobe_duration_sync, str(tmp_path))
        return duration

    except Exception as exc:
        logger.error(f"Failed to get audio duration from S3: {s3_key}, error: {exc}")
        return None

    finally:
        # 임시 파일 정리
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception as exc:
                logger.warning(f"Failed to delete temp file {tmp_path}: {exc}")
