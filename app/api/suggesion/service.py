import vertexai
from vertexai.generative_models import GenerativeModel
from google.oauth2 import service_account
from bson import ObjectId
import asyncio
import logging
from app.config.env import (
    VERTEX_PROJECT_ID,
    VERTEX_LOCATION,
    GEMINI_MODEL_VERSION,
    GOOGLE_APPLICATION_CREDENTIALS,
)
from ..deps import DbDep
from .models import SuggestionRequest, SuggestionResponse
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Model:
    def __init__(self, db: DbDep):
        self.suggesion_prompt_collection = db.get_collection("suggesion_prompt")
        self.project_segemnts_collection = db.get_collection("project_segments")
        self.segment_translations_collection = db.get_collection("segment_translations")
        self.languages_collection = db.get_collection("languages")

        sa_path = GOOGLE_APPLICATION_CREDENTIALS
        try:
            # 서비스 계정 키 파일 경로
            sa_path = GOOGLE_APPLICATION_CREDENTIALS

            if not all(
                [VERTEX_PROJECT_ID, VERTEX_LOCATION, GEMINI_MODEL_VERSION, sa_path]
            ):
                raise ValueError(
                    "필수 환경 변수(PROJECT_ID, LOCATION, MODEL, CREDENTIALS)가 설정되지 않았습니다."
                )

            # 2. 자격 증명(Credentials) 생성
            credentials = service_account.Credentials.from_service_account_file(
                sa_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )

            vertexai.init(
                project=VERTEX_PROJECT_ID,
                location=VERTEX_LOCATION,
                credentials=credentials,
            )

            self.model = GenerativeModel(GEMINI_MODEL_VERSION)

            if not all([self.project_id, self.location, self.model_name, sa_path]):
                raise ValueError(
                    "필수 환경 변수(PROJECT_ID, LOCATION, MODEL, CREDENTIALS)가 설정되지 않았습니다."
                )

            # 2. 자격 증명(Credentials) 생성
            credentials = service_account.Credentials.from_service_account_file(
                sa_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )

            vertexai.init(
                project=self.project_id, location=self.location, credentials=credentials
            )

            self.model = GenerativeModel(GEMINI_MODEL_VERSION)

        except Exception as e:
            logger.error(f"오류 발생: {e}")

    async def prompt_text(self, segment_id: str, request_context: str) -> str:
        if not self.model:
            logger.error("Gemini 모델이 초기화되지 않았습니다.")
            return ""

        try:
            # --- 1. DB 조회 병렬 처리 ---
            # 3개의 비동기 작업을 리스트로 준비
            tasks = [
                self.project_segemnts_collection.find_one(
                    {"_id": ObjectId(segment_id)}
                ),
                self.segment_translations_collection.find_one(
                    {"segment_id": segment_id}
                ),
                # 이 언어 코드는 trans_segment가 완료되어야 알 수 있으므로,
                # 이 방식은 적합하지 않습니다. (아래 2안 참고)
            ]

            # --- (수정) DB 호출 2단계로 병렬화 ---

            # 1단계: 필수 정보 2개를 동시에 가져오기
            tasks_step1 = [
                self.project_segemnts_collection.find_one(
                    {"_id": ObjectId(segment_id)}
                ),
                self.segment_translations_collection.find_one(
                    {"segment_id": segment_id}
                ),
            ]

            project_segment, trans_segment = await asyncio.gather(*tasks_step1)

            if not project_segment or not trans_segment:
                logger.error("세그먼트 정보를 찾을 수 없습니다: %s", segment_id)
                return ""

            # 2단계: 1단계 정보를 바탕으로 3번째 정보 가져오기
            language_code = trans_segment.get("language_code")
            language = await self.languages_collection.find_one(
                {"language_code": language_code}
            )

            if not language:
                logger.error("언어 정보를 찾을 수 없습니다: %s", language_code)
                return ""
            # --- 병렬 처리 완료 ---

            language_name = language.get("name_ko", "")
            origin_context = project_segment.get("source_text", "")
            translate_context = trans_segment.get("target_text", "")

            prompt = f"""
            [Role]: You are a professional dubbing script editor.
            [Original Text]: {origin_context}
            [Translated Text]: {translate_context}
            [Request]: {request_context}
            [Rules]:
            1. Do not provide any explanations, apologies, or extra text.
            2. Respond with only the single, final, revised {language_name} script.
            3. Do not add any text before or after the revised script.
            4. **CRITICAL:** Your output must be the raw text of the script *only*. Do not wrap your response in quotation marks ("), apostrophes ('), asterisks (*), hyphens (-), or any other formatting characters.
            """

            # --- 2. 비-스트리밍 API 사용 ---
            # (이것이 '완성된 텍스트'를 얻는 가장 빠르고 올바른 방법입니다.)
            response = await self.model.generate_content_async(prompt)

            if not response:
                return ""

            cleaned_text = response.text.strip().strip('"')
            return cleaned_text

        except Exception as exc:
            logger.error(f"Gemini API 또는 DB 호출 오류: {exc}", exc_info=True)
            return ""

    async def get_suggession_by_id(self, segment_id: str):
        doc = await self.suggesion_prompt_collection.find_one(
            {"$or": [{"_id": ObjectId(segment_id)}, {"segment_id": segment_id}]}
        )
        if doc:
            return SuggestionResponse(**doc)
        return None

    async def delete_suggession_by_id(self, segment_id: str):
        return await self.suggesion_prompt_collection.delete_one(
            {"segment_id": segment_id}
        )

    async def update_suggession_by_id(self, request: SuggestionRequest):
        update_data = request.model_dump(exclude_unset=True)

        await self.suggesion_prompt_collection.update_one(
            {"segment_id": request.segment_id},
            {"$set": update_data},
        )
        return str(request.segment_id)

    async def get_suggession_list(self):
        docs = await self.suggesion_prompt_collection.find({}).to_list(length=None)
        return [SuggestionResponse(**doc) for doc in docs]
