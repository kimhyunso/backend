"""
세그먼트 번역 서비스
worker의 translate_single_segment 로직을 참고하여 구현
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional
from app.config.env import GOOGLE_APPLICATION_CREDENTIALS

logger = logging.getLogger(__name__)

#  AI 라이브러리 선택적 임포트
_VERTEX_AVAILABLE = True
try:
    import vertexai
    from vertexai.generative_models import GenerativeModel, GenerationConfig
    from google.oauth2 import service_account
except Exception:
    _VERTEX_AVAILABLE = False


def _env_str(key: str, default: str | None = None) -> str | None:
    """환경변수 문자열 읽기"""
    v = os.getenv(key)
    return v if v is not None and v != "" else default


def _env_bool(key: str, default: bool = False) -> bool:
    """환경변수 불린 읽기"""
    v = os.getenv(key)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _fallback_translate_batch(
    items: List[Dict[str, Any]], target_lang: str, src_lang: str | None = None
) -> List[Dict[str, Any]]:
    """최소한의 폴백 번역기.

    - 1순위: googletrans 사용(설치되어 있고 네트워크 가능할 때)
    - 실패 시: 입력 텍스트를 그대로 반환(아이덴티티)
    """
    try:
        from googletrans import Translator

        tr = Translator()
        src = src_lang if src_lang else "auto"
        texts = [str(o["text"]) for o in items]
        res = tr.translate(texts, dest=target_lang, src=src)
        outputs: List[Dict[str, Any]] = []
        for i, o in enumerate(items):
            translated = res[i].text if i < len(res) else str(o["text"])
            outputs.append({"seg_idx": int(o["seg_idx"]), "translation": translated})
        return outputs
    except Exception as e:
        logger.warning(f"Fallback translation failed: {e}, returning original text")
        # 네트워크/설치 문제 시 아이덴티티 폴백
        return [
            {"seg_idx": int(o["seg_idx"]), "translation": str(o["text"])} for o in items
        ]


class GeminiTranslator:
    """Vertex AI Gemini 기반 번역기 (worker의 GeminiTranslator와 동일한 로직)"""

    def __init__(self) -> None:
        if not _VERTEX_AVAILABLE:
            raise RuntimeError(
                "Vertex AI libraries not installed. Add google-cloud-aiplatform to requirements."
            )

        # 모델/리전
        self.model_name = _env_str("GEMINI_MODEL_VERSION", "gemini-2.5-flash")
        self.location = _env_str("VERTEX_LOCATION", "us-central1")
        self.project_id = _env_str("VERTEX_PROJECT_ID")

        # 서비스 계정 JSON (여러 키명 지원)
        sa_path = (
            _env_str("VERTEX_SERVICE_ACCOUNT_JSON")
            or _env_str("VERTEX_SA_PATH")
            or GOOGLE_APPLICATION_CREDENTIALS
        )

        creds = None
        if sa_path and os.path.isfile(sa_path):
            # 프로젝트 ID가 없으면 JSON에서 복구 시도
            if not self.project_id:
                try:
                    with open(sa_path, "r", encoding="utf-8") as f:
                        j = json.load(f)
                        self.project_id = j.get("project_id")
                except Exception:
                    pass
            try:
                creds = service_account.Credentials.from_service_account_file(
                    sa_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to load service account JSON at {sa_path}: {e}"
                )

        if not self.project_id:
            raise RuntimeError(
                "VERTEX_PROJECT_ID is required (or set in service account JSON)."
            )

        vertexai.init(
            project=self.project_id, location=self.location, credentials=creds
        )
        self._model = GenerativeModel(self.model_name)

    def translate_batch(
        self,
        items: List[Dict[str, Any]],
        target_lang: str,
        src_lang: str | None = None,
    ) -> List[Dict[str, Any]]:
        """배치 번역 수행 (심플 버전).

        items: [{"seg_idx": int, "text": str}, ...]
        반환: [{"seg_idx": int, "translation": str}, ...] (seg_idx 기준으로 N개 복원)
        """
        n = len(items)
        if n == 0:
            return []

        src_texts = [str(o["text"]) for o in items]
        seg_idxs = [int(o["seg_idx"]) for o in items]

        sys = (
            "You are a professional subtitle translator.\n"
            "- Translate with dubbing in mind.\n"
            "- Idioms can be paraphrased.\n"
            "- Translate short things briefly.\n"
            "- Translate each item from source language to the target language.\n"
            "- Do NOT merge or split items.\n"
            "- Do NOT add explanations, numbering, or any extra text.\n"
            "- Return ONLY one JSON array of length N.\n"
            "- Each element must be an object with exactly two fields: "
            "seg_idx (int) and translation (string).\n"
        )
        src_lang_txt = (
            f"Source language: {src_lang}"
            if src_lang
            else "Source language: auto-detect"
        )
        user = (
            f"N={n}\n"
            f"{src_lang_txt}\nTarget language: {target_lang}\n\n"
            "Inputs (keep order and seg_idx):\n"
            + "\n".join(
                f"[{i}] seg_idx={seg_idxs[i]} text={src_texts[i]}" for i in range(n)
            )
            + "\n\nReturn JSON ONLY like:\n"
            '[{"seg_idx": 0, "translation": "..."}, ...]'
        )

        gen_cfg = GenerationConfig(
            temperature=0.1,
            max_output_tokens=8192,
        )

        resp = self._model.generate_content(
            contents=[sys, user],
            generation_config=gen_cfg,
        )
        logger.info(f"Gemini raw response: {resp}")
        text = self._extract_text(resp)
        data = self._parse_json_array(text)

        # seg_idx → 번역 매핑
        mapping: Dict[int, str] = {}
        for obj in data or []:
            try:
                si = int(obj.get("seg_idx"))
                tr = obj.get("translation")
                if isinstance(tr, str):
                    mapping[si] = tr
            except Exception:
                continue

        # 최종 N개 복원 (누락은 원문 폴백)
        out: List[Dict[str, Any]] = []
        for idx, src in zip(seg_idxs, src_texts):
            out.append({"seg_idx": idx, "translation": mapping.get(idx, src)})
        return out

    @staticmethod
    def _extract_text(resp: Any) -> str:
        """응답에서 텍스트 추출"""
        try:
            cands = getattr(resp, "candidates", None)
            if cands:
                content = cands[0].content
                parts = getattr(content, "parts", None)
                for p in parts or []:
                    t = getattr(p, "text", None)
                    if isinstance(t, str) and t.strip():
                        return t
        except Exception:
            pass
        try:
            if hasattr(resp, "text") and isinstance(resp.text, str):
                return resp.text
        except Exception:
            pass
        return str(resp)

    @staticmethod
    def _parse_json_array(text: str) -> List[Dict[str, Any]]:
        """텍스트에서 JSON 배열 파싱"""
        # 1) 순수 JSON 시도
        try:
            obj = json.loads(text)
            if isinstance(obj, list):
                return obj
        except Exception:
            pass
        # 2) 주변 텍스트에서 JSON 배열 부분 추출 시도
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1 and end > start:
            snippet = text[start : end + 1]
            try:
                obj = json.loads(snippet)
                if isinstance(obj, list):
                    return obj
            except Exception:
                pass
        # 3) 실패 시 빈 배열
        return []


def translate_single_segment(
    source_text: str,
    segment_index: int,
    target_lang: str,
    src_lang: str | None = None,
) -> dict[str, Any]:
    """단일 세그먼트를 번역합니다.

    Args:
        source_text: 원문 텍스트
        segment_index: 세그먼트 인덱스
        target_lang: 타겟 언어 코드
        src_lang: 소스 언어 코드 (선택)

    Returns:
        {"seg_idx": int, "translation": str}
    """
    items = [{"seg_idx": segment_index, "text": source_text}]

    # 백엔드 선택: 기본 vertex, 환경변수로 강제 가능
    backend = (os.getenv("MT_BACKEND") or "vertex").strip().lower()
    strict = _env_bool("MT_STRICT", True)
    translator: Any | None = None
    use_vertex = backend in {"vertex", "gemini", "gemini-vertex"}

    if use_vertex:
        try:
            translator = GeminiTranslator()
        except Exception as exc:
            if strict:
                raise RuntimeError(
                    f"Vertex translator initialization failed under MT_STRICT: {exc}"
                )
            use_vertex = False

    if not use_vertex or translator is None:
        # 폴백 번역 사용
        result = _fallback_translate_batch(items, target_lang, src_lang=src_lang)
    else:
        # Vertex 사용
        result = translator.translate_batch(items, target_lang, src_lang=src_lang)

    if result and len(result) > 0:
        return result[0]
    else:
        # 실패 시 원문 반환
        return {"seg_idx": segment_index, "translation": source_text}
