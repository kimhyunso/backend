# 워커 콜백 테스트 가이드

워커가 로컬에서 실행되지 않을 때 콜백을 시뮬레이션하여 API 동작을 테스트하는 방법입니다.

## 사전 준비

1. **Job ID 얻기**
   ```bash
   # 프로젝트 생성 후 job을 시작하면 job_id를 얻을 수 있습니다
   # 또는 기존 job 조회
   curl http://localhost:8000/api/jobs/project/{project_id}
   ```

2. **API 서버 실행 확인**
   ```bash
   docker compose logs -f api
   ```

## 방법 1: Python 스크립트 사용 (추천)

### 설치
```bash
pip install requests  # 필요한 경우
```

### 기본 사용법

#### 전체 파이프라인 테스트 (새 포맷)
```bash
python test_worker_callback.py <job_id> --stage all --format new --verify
```

#### 전체 파이프라인 테스트 (기존 포맷)
```bash
python test_worker_callback.py <job_id> --stage all --format legacy --verify
```

#### Done 단계만 테스트 (새 포맷 - metadata_key 사용)
```bash
# 1. 먼저 S3에 테스트 메타데이터 업로드
python test_worker_callback.py <job_id> --stage done --format new --upload-metadata --verify

# 또는 수동으로 S3에 업로드 후
python test_worker_callback.py <job_id> --stage done --format new --verify
```

#### Done 단계만 테스트 (기존 포맷 - 인라인 segments)
```bash
python test_worker_callback.py <job_id> --stage done --format legacy --verify
```

#### 특정 stage만 테스트
```bash
python test_worker_callback.py <job_id> --stage asr_completed --target-lang en
```

### 옵션 설명
- `--stage`: 실행할 stage (`all`, `starting`, `asr_started`, `done` 등)
- `--format`: Done 단계 포맷 (`new` 또는 `legacy`)
  - `new`: S3에서 metadata_key로 JSON 다운로드
  - `legacy`: metadata에 직접 segments 포함
- `--target-lang`: 타겟 언어 (기본값: `en`)
- `--upload-metadata`: S3에 테스트 메타데이터 업로드
- `--verify`: 실행 후 결과 확인 (job, targets, segments, assets)

## 방법 2: Bash 스크립트 사용

### 기본 사용법
```bash
./test_worker_callback.sh <job_id> <stage>
```

### 예시
```bash
# 전체 파이프라인 실행
./test_worker_callback.sh 67491d8e9f1234567890abcd all

# Done 단계만 (새 포맷)
./test_worker_callback.sh 67491d8e9f1234567890abcd done

# Done 단계만 (기존 포맷)
./test_worker_callback.sh 67491d8e9f1234567890abcd done-legacy
```

## 방법 3: cURL 직접 사용

### Starting
```bash
curl -X POST "http://localhost:8000/api/jobs/<job_id>/status" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "in_progress",
    "metadata": {
      "stage": "starting",
      "target_lang": "en"
    }
  }'
```

### Done (새 포맷 - metadata_key)
```bash
curl -X POST "http://localhost:8000/api/jobs/<job_id>/status" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "done",
    "result_key": "projects/test-project/output/dubbed_en.mp4",
    "metadata": {
      "stage": "done",
      "target_lang": "en",
      "metadata_key": "projects/test-project/metadata.json",
      "translations": [
        "Translated text 1",
        "Translated text 2"
      ]
    }
  }'
```

### Done (기존 포맷 - 인라인 segments)
```bash
curl -X POST "http://localhost:8000/api/jobs/<job_id>/status" \
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
          "prompt_text": "Translated segment 1",
          "audio_file": "projects/test/segments/0.mp3"
        }
      ]
    }
  }'
```

## 결과 확인

### 1. API 로그 확인
```bash
docker compose logs -f api | grep -E "(Processing|Created|Updated|segments|asset)"
```

### 2. Database 직접 조회
```bash
# MongoDB 접속
docker exec -it dupilot-mongo mongosh -u root -p example --authenticationDatabase admin dupilot

# Segments 확인
db.project_segments.find({project_id: "your-project-id"}).pretty()

# Translations 확인
db.segment_translations.find({}).pretty()

# Assets 확인
db.assets.find({project_id: "your-project-id"}).pretty()

# Project Targets 확인
db.project_targets.find({project_id: "your-project-id"}).pretty()
```

### 3. REST API로 확인
```bash
# Job 상태
curl http://localhost:8000/api/jobs/<job_id> | jq

# Project Targets
curl http://localhost:8000/api/projects/<project_id>/targets | jq

# Segments
curl http://localhost:8000/api/segments/project/<project_id> | jq

# Assets
curl http://localhost:8000/api/assets/project/<project_id> | jq
```

## 테스트 시나리오

### 시나리오 1: 전체 파이프라인 (새 포맷)

1. **S3에 메타데이터 준비**
   ```bash
   # S3에 테스트 metadata.json 업로드 (AWS CLI 또는 콘솔 사용)
   aws s3 cp test_metadata.json s3://dupilot-dev-media/projects/test-project/metadata.json
   ```

2. **전체 파이프라인 실행**
   ```bash
   python test_worker_callback.py 67491d8e9f1234567890abcd --stage all --format new --verify
   ```

3. **확인 사항**
   - ✅ Job 상태가 `done`으로 변경
   - ✅ Project Target 진행도 100% 및 `COMPLETED` 상태
   - ✅ Project Segments 생성 (원본 텍스트 포함)
   - ✅ Segment Translations 생성 (번역된 텍스트 포함)
   - ✅ Asset 생성 (DUBBED_VIDEO 타입)

### 시나리오 2: Done만 테스트 (기존 포맷)

```bash
python test_worker_callback.py 67491d8e9f1234567890abcd --stage done --format legacy --verify
```

### 시나리오 3: 다중 언어 처리

```bash
# 영어
python test_worker_callback.py <job_id_en> --stage done --format new --target-lang en

# 일본어
python test_worker_callback.py <job_id_ja> --stage done --format new --target-lang ja

# 각 언어별 Segment Translations가 별도로 생성되는지 확인
```

## 새 포맷 vs 기존 포맷 차이

### 새 포맷 (metadata_key)
**장점:**
- 큰 세그먼트 데이터를 콜백 페이로드에 포함하지 않음
- S3에서 재사용 가능
- 네트워크 부하 감소

**단점:**
- S3 다운로드 추가 단계 필요
- S3 접근 권한 필요

**워커 콜백 예시:**
```json
{
  "status": "done",
  "result_key": "projects/xxx/output.mp4",
  "metadata": {
    "stage": "done",
    "target_lang": "en",
    "metadata_key": "projects/xxx/metadata.json",
    "translations": ["text1", "text2"]
  }
}
```

### 기존 포맷 (inline segments)
**장점:**
- S3 불필요
- 단순한 구조

**단점:**
- 큰 페이로드 크기
- 콜백 요청 제한 가능성

**워커 콜백 예시:**
```json
{
  "status": "done",
  "result_key": "projects/xxx/output.mp4",
  "metadata": {
    "stage": "done",
    "target_lang": "en",
    "segments": [
      {
        "seg_idx": 0,
        "speaker": "SPEAKER_00",
        "start": 0.217,
        "end": 13.426,
        "prompt_text": "Translated text",
        "audio_file": "path/to/audio.mp3"
      }
    ]
  }
}
```

## 트러블슈팅

### S3 다운로드 실패
```
❌ Failed to download metadata from S3
```
**해결:**
- S3에 metadata.json이 존재하는지 확인
- AWS 자격 증명 확인
- S3 버킷 이름 확인 (`.env`의 `AWS_S3_BUCKET`)

### Segment 생성 실패
```
❌ Failed to create segments
```
**해결:**
- MongoDB 연결 확인
- 로그에서 상세 에러 확인: `docker compose logs api`

### Asset 생성 안됨
**원인:**
- `result_key`가 누락됨

**해결:**
- 콜백에 `result_key` 포함 확인

## 참고 자료

- [routes.py](app/api/jobs/routes.py) - 콜백 처리 로직
- [s3.py](app/utils/s3.py) - S3 유틸리티 함수
- [CLAUDE.md](CLAUDE.md) - 프로젝트 전체 가이드
