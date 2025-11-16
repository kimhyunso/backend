import vertexai
from vertexai.generative_models import GenerativeModel
from google.oauth2 import service_account
from bson import ObjectId
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

        except Exception as e:
            logger.error(f"오류 발생: {e}")

    async def prompt_text(self, segment_id: str, request_context: str) -> str:
        if not self.model:
            logger.error("Gemini 모델이 초기화되지 않았습니다.")
            return ""

        project_segment = await self.project_segemnts_collection.find_one(
            {"_id": ObjectId(segment_id)}
        )
        trans_segment = await self.segment_translations_collection.find_one(
            {"segment_id": segment_id}
        )
        language_code = trans_segment.get("language_code")
        language = await self.languages_collection.find_one(
            {"language_code": language_code}
        )

        if not project_segment or not trans_segment or not language:
            logger.error("세그먼트 정보를 찾을 수 없습니다: %s", segment_id)
            return ""

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

        try:
            response = await self.model.generate_content_async(prompt)
        except Exception as exc:
            logger.error("Gemini API 호출 오류: %s", exc)
            return ""

        if not response:
            return ""
        return response.text.strip()

    async def save_prompt_text(self, segment_id: str) -> str:
        project_segment = await self.project_segemnts_collection.find_one(
            {"_id": ObjectId(segment_id)}
        )
        trans_segment = await self.segment_translations_collection.find_one(
            {"segment_id": segment_id}
        )
        if not project_segment or not trans_segment:
            raise ValueError("세그먼트 정보를 찾을 수 없습니다.")

        document_to_save = {
            "segment_id": segment_id,
            "original_text": project_segment.get("source_text", ""),
            "translate_text": trans_segment.get("target_text", ""),
            "sugession_text": None,
            "created_at": datetime.utcnow(),
        }

        result = await self.suggesion_prompt_collection.insert_one(document_to_save)
        return str(result.inserted_id)

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
