#!/usr/bin/env python3
"""
워커 콜백 시뮬레이션 스크립트

사용법:
  python test_worker_callback.py <job_id> [--stage <stage>] [--format new|legacy]

예시:
  python test_worker_callback.py 67491d8e9f1234567890abcd --stage all
  python test_worker_callback.py 67491d8e9f1234567890abcd --stage done --format new
  python test_worker_callback.py 67491d8e9f1234567890abcd --stage done --format legacy
"""

import argparse
import json
import time
import requests
from typing import Optional

BASE_URL = "http://localhost:8000"


def send_callback(job_id: str, status: str, metadata: dict, result_key: Optional[str] = None):
    """콜백 요청 전송"""
    url = f"{BASE_URL}/api/jobs/{job_id}/status"
    payload = {
        "status": status,
        "metadata": metadata
    }
    if result_key:
        payload["result_key"] = result_key

    print(f"\n{'='*60}")
    print(f"▶ Stage: {metadata.get('stage')}")
    print(f"  Status: {status}")
    print(f"  Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
    print(f"{'='*60}")

    try:
        response = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
        response.raise_for_status()
        print(f"✅ Success: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return None


def simulate_full_pipeline(job_id: str, target_lang: str = "en", format_type: str = "new"):
    """전체 파이프라인 시뮬레이션"""
    print("\n" + "="*60)
    print("🚀 전체 파이프라인 시뮬레이션 시작")
    print(f"Job ID: {job_id}")
    print(f"Target Language: {target_lang}")
    print(f"Format: {format_type}")
    print("="*60)

    stages = [
        ("starting", "in_progress", {}),
        ("asr_started", "in_progress", {}),
        ("asr_completed", "in_progress", {}),
        ("translation_started", "in_progress", {}),
        ("translation_completed", "in_progress", {}),
        ("tts_started", "in_progress", {}),
        ("tts_completed", "in_progress", {}),
        ("mux_started", "in_progress", {}),
    ]

    for stage, status, extra_meta in stages:
        metadata = {
            "stage": stage,
            "target_lang": target_lang,
            **extra_meta
        }
        send_callback(job_id, status, metadata)
        time.sleep(1)

    # 마지막 done 단계
    if format_type == "new":
        simulate_done_new_format(job_id, target_lang)
    else:
        simulate_done_legacy_format(job_id, target_lang)


def simulate_done_new_format(job_id: str, target_lang: str = "en"):
    """Done 단계 - 새 포맷 (metadata_key 사용)"""
    metadata = {
        "stage": "done",
        "target_lang": target_lang,
        "metadata_key": "projects/test-project/metadata.json",  # S3에 미리 업로드 필요
        "translations": [
            "좋은 개발자라는 단어가 중요한 단인데 좋은 개발자라고 했을 때 정말 중요한 첫 번째는 기초, 소프트웨어 관련된, 컴퓨터가 관련된 것들이 정말 빠르게 변해요.",
            "빠르게 변하는 거를 이렇게 따라가야 되는데, 그러려면 그냥 도구만 알아서는 곤란하고 정말 기초가 탄탄해야 되는 것 같아요."
        ]
    }
    result_key = f"projects/test-project/output/dubbed_{target_lang}.mp4"
    send_callback(job_id, "done", metadata, result_key)


def simulate_done_legacy_format(job_id: str, target_lang: str = "en"):
    """Done 단계 - 기존 포맷 (인라인 segments)"""
    metadata = {
        "stage": "done",
        "target_lang": target_lang,
        "segments": [
            {
                "seg_idx": 0,
                "speaker": "SPEAKER_00",
                "start": 0.217,
                "end": 13.426,
                "prompt_text": "Good developers are important. When we talk about good developers, the first really important thing is fundamentals - software-related, computer-related things change really fast.",
                "audio_file": "projects/test/segments/seg_0_en.mp3"
            },
            {
                "seg_idx": 1,
                "speaker": "SPEAKER_00",
                "start": 13.446,
                "end": 23.187,
                "prompt_text": "We need to keep up with these rapid changes, and to do that, just knowing the tools isn't enough - we really need solid fundamentals.",
                "audio_file": "projects/test/segments/seg_1_en.mp3"
            }
        ]
    }
    result_key = f"projects/test-project/output/dubbed_{target_lang}.mp4"
    send_callback(job_id, "done", metadata, result_key)


def upload_test_metadata_to_s3(project_id: str):
    """테스트용 메타데이터를 S3에 업로드 (선택사항)"""
    import boto3
    import os

    # S3 클라이언트 생성
    session = boto3.Session(
        profile_name=os.getenv("AWS_PROFILE"),
        region_name=os.getenv("AWS_REGION", "ap-northeast-2")
    )
    s3_client = session.client("s3")
    bucket = os.getenv("AWS_S3_BUCKET", "dupilot-dev-media")

    # 테스트 메타데이터
    metadata = {
        "v": 1,
        "unit": "ms",
        "lang": "ko",
        "speakers": ["SPEAKER_00"],
        "segments": [
            {
                "s": 217,
                "e": 13426,
                "sp": 0,
                "txt": "좋은 개발자라는 단어가 중요한 단인데 좋은 개발자라고 했을 때 정말 중요한 첫 번째는 기초, 소프트웨어 관련된, 컴퓨터가 관련된 것들이 정말 빠르게 변해요.",
                "gap": [20, 20],
                "w_off": [0, 22],
                "o": 0,
                "ov": False
            },
            {
                "s": 13446,
                "e": 23187,
                "sp": 0,
                "txt": "빠르게 변하는 거를 이렇게 따라가야 되는데, 그러려면 그냥 도구만 알아서는 곤란하고 정말 기초가 탄탄해야 되는 것 같아요.",
                "gap": [None, None],
                "w_off": [22, 17],
                "o": 1,
                "ov": False
            }
        ],
        "vocab": ["좋은", "개발자라는", "단어가", "중요한"],
        "words": []
    }

    # S3에 업로드
    key = f"projects/{project_id}/metadata.json"
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(metadata, ensure_ascii=False),
            ContentType="application/json"
        )
        print(f"✅ 메타데이터 업로드 성공: s3://{bucket}/{key}")
        return key
    except Exception as e:
        print(f"❌ 메타데이터 업로드 실패: {e}")
        return None


def verify_results(job_id: str):
    """결과 확인"""
    print("\n" + "="*60)
    print("🔍 결과 확인")
    print("="*60)

    # Job 조회
    try:
        response = requests.get(f"{BASE_URL}/api/jobs/{job_id}")
        response.raise_for_status()
        job = response.json()
        print(f"\n1️⃣ Job 상태:")
        print(f"   Status: {job.get('status')}")
        print(f"   Result Key: {job.get('result_key')}")

        project_id = job.get('project_id')

        # Project Targets 조회
        if project_id:
            response = requests.get(f"{BASE_URL}/api/projects/{project_id}/targets")
            response.raise_for_status()
            targets = response.json()
            print(f"\n2️⃣ Project Targets:")
            for target in targets:
                print(f"   - {target.get('language_code')}: {target.get('status')} ({target.get('progress')}%)")

            # Segments 조회
            response = requests.get(f"{BASE_URL}/api/segments/project/{project_id}")
            response.raise_for_status()
            segments = response.json()
            print(f"\n3️⃣ Segments: {len(segments)} 개")
            for seg in segments[:3]:  # 처음 3개만 출력
                print(f"   - [{seg.get('segment_index')}] {seg.get('speaker_tag')}: {seg.get('source_text')[:50]}...")

            # Assets 조회
            response = requests.get(f"{BASE_URL}/api/assets/project/{project_id}")
            response.raise_for_status()
            assets = response.json()
            print(f"\n4️⃣ Assets: {len(assets)} 개")
            for asset in assets:
                print(f"   - {asset.get('language_code')} ({asset.get('asset_type')}): {asset.get('file_path')}")

    except requests.exceptions.RequestException as e:
        print(f"❌ 조회 실패: {e}")


def main():
    parser = argparse.ArgumentParser(description="워커 콜백 시뮬레이션")
    parser.add_argument("job_id", help="Job ID")
    parser.add_argument("--stage", default="all",
                       choices=["all", "starting", "asr_started", "asr_completed",
                               "translation_started", "translation_completed",
                               "tts_started", "tts_completed", "mux_started", "done"],
                       help="실행할 stage")
    parser.add_argument("--format", default="new", choices=["new", "legacy"],
                       help="Done 단계 포맷 (new: metadata_key 사용, legacy: inline segments)")
    parser.add_argument("--target-lang", default="en", help="타겟 언어")
    parser.add_argument("--upload-metadata", action="store_true",
                       help="S3에 테스트 메타데이터 업로드 (new 포맷 사용시)")
    parser.add_argument("--verify", action="store_true", help="실행 후 결과 확인")

    args = parser.parse_args()

    # S3에 메타데이터 업로드 (선택사항)
    if args.upload_metadata and args.format == "new":
        # Job에서 project_id 추출 필요
        try:
            response = requests.get(f"{BASE_URL}/api/jobs/{args.job_id}")
            response.raise_for_status()
            job = response.json()
            project_id = job.get("project_id")
            if project_id:
                upload_test_metadata_to_s3(project_id)
        except Exception as e:
            print(f"⚠️ 메타데이터 업로드 스킵: {e}")

    # 시뮬레이션 실행
    if args.stage == "all":
        simulate_full_pipeline(args.job_id, args.target_lang, args.format)
    elif args.stage == "done":
        if args.format == "new":
            simulate_done_new_format(args.job_id, args.target_lang)
        else:
            simulate_done_legacy_format(args.job_id, args.target_lang)
    else:
        # 단일 stage 실행
        metadata = {
            "stage": args.stage,
            "target_lang": args.target_lang
        }
        send_callback(args.job_id, "in_progress", metadata)

    # 결과 확인
    if args.verify:
        time.sleep(1)
        verify_results(args.job_id)


if __name__ == "__main__":
    main()
