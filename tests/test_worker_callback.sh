#!/bin/bash

# 워커 콜백 시뮬레이션 스크립트
# 사용법: ./test_worker_callback.sh <job_id> <stage>

JOB_ID=${1:-"your-job-id-here"}
STAGE=${2:-"done"}
BASE_URL="http://localhost:8000"

echo "=== 워커 콜백 시뮬레이션 ==="
echo "Job ID: $JOB_ID"
echo "Stage: $STAGE"
echo ""

# 1. starting 단계
if [ "$STAGE" == "starting" ] || [ "$STAGE" == "all" ]; then
  echo "▶ Starting stage..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "in_progress",
      "metadata": {
        "stage": "starting",
        "target_lang": "en"
      }
    }'
  echo -e "\n"
  sleep 2
fi

# 2. asr_started 단계
if [ "$STAGE" == "asr_started" ] || [ "$STAGE" == "all" ]; then
  echo "▶ ASR Started stage..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "in_progress",
      "metadata": {
        "stage": "asr_started",
        "target_lang": "en"
      }
    }'
  echo -e "\n"
  sleep 2
fi

# 3. asr_completed 단계
if [ "$STAGE" == "asr_completed" ] || [ "$STAGE" == "all" ]; then
  echo "▶ ASR Completed stage..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "in_progress",
      "metadata": {
        "stage": "asr_completed",
        "target_lang": "en"
      }
    }'
  echo -e "\n"
  sleep 2
fi

# 4. translation_started 단계
if [ "$STAGE" == "translation_started" ] || [ "$STAGE" == "all" ]; then
  echo "▶ Translation Started stage..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "in_progress",
      "metadata": {
        "stage": "translation_started",
        "target_lang": "en"
      }
    }'
  echo -e "\n"
  sleep 2
fi

# 5. translation_completed 단계
if [ "$STAGE" == "translation_completed" ] || [ "$STAGE" == "all" ]; then
  echo "▶ Translation Completed stage..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "in_progress",
      "metadata": {
        "stage": "translation_completed",
        "target_lang": "en"
      }
    }'
  echo -e "\n"
  sleep 2
fi

# 6. tts_started 단계
if [ "$STAGE" == "tts_started" ] || [ "$STAGE" == "all" ]; then
  echo "▶ TTS Started stage..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "in_progress",
      "metadata": {
        "stage": "tts_started",
        "target_lang": "en"
      }
    }'
  echo -e "\n"
  sleep 2
fi

# 7. tts_completed 단계
if [ "$STAGE" == "tts_completed" ] || [ "$STAGE" == "all" ]; then
  echo "▶ TTS Completed stage..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "in_progress",
      "metadata": {
        "stage": "tts_completed",
        "target_lang": "en"
      }
    }'
  echo -e "\n"
  sleep 2
fi

# 8. mux_started 단계
if [ "$STAGE" == "mux_started" ] || [ "$STAGE" == "all" ]; then
  echo "▶ Mux Started stage..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "in_progress",
      "metadata": {
        "stage": "mux_started",
        "target_lang": "en"
      }
    }'
  echo -e "\n"
  sleep 2
fi

# 9. done 단계 (새 포맷 - metadata_key 사용)
if [ "$STAGE" == "done" ] || [ "$STAGE" == "all" ]; then
  echo "▶ Done stage (with metadata_key)..."

  # 먼저 S3에 테스트 메타데이터 업로드 (수동으로 해야 함)
  # 또는 metadata에 직접 segments 포함

  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "done",
      "result_key": "projects/test-project/output/dubbed_en.mp4",
      "metadata": {
        "stage": "done",
        "target_lang": "en",
        "metadata_key": "projects/test-project/metadata.json",
        "translations": [
          "This is the first translated segment",
          "This is the second translated segment"
        ]
      }
    }'
  echo -e "\n"
fi

# 10. done 단계 (기존 포맷 - 인라인 segments)
if [ "$STAGE" == "done-legacy" ]; then
  echo "▶ Done stage (legacy format with inline segments)..."
  curl -X POST "$BASE_URL/api/jobs/$JOB_ID/status" \
    -H "Content-Type: application/json" \
    -d '{
      "status": "done",
      "result_key": "projects/test-project/output/dubbed_en.mp4",
      "metadata": {
        "stage": "done",
        "target_lang": "en",
        "segments": [
          {
            "seg_idx": 0,
            "speaker": "SPEAKER_00",
            "start": 0.217,
            "end": 13.426,
            "prompt_text": "This is the first translated segment",
            "audio_file": "projects/test/segments/0.mp3"
          },
          {
            "seg_idx": 1,
            "speaker": "SPEAKER_00",
            "start": 13.446,
            "end": 23.187,
            "prompt_text": "This is the second translated segment",
            "audio_file": "projects/test/segments/1.mp3"
          }
        ]
      }
    }'
  echo -e "\n"
fi

echo "=== 완료 ==="
