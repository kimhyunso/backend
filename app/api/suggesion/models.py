from datetime import datetime
from typing import Optional, Annotated
from pydantic import BaseModel, Field, ConfigDict, BeforeValidator
from bson import ObjectId

PyObjectId = Annotated[
    str,  # <--- str에서 ObjectId로 변경하세요.
    BeforeValidator(lambda v: ObjectId(v) if not isinstance(v, ObjectId) else v),
]


class SuggestionResponse(BaseModel):
    id: PyObjectId = Field(validation_alias="_id")
    segment_id: str
    original_text: Optional[str] = None
    translate_text: Optional[str] = None
    sugession_text: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class SuggestDelete(BaseModel):
    segment_id: str


class SuggestSave(BaseModel):
    segment_id: PyObjectId


class SuggestionRequest(BaseModel):
    segment_id: str
    original_text: Optional[str] = None
    translate_text: Optional[str] = None
    sugession_text: Optional[str] = None
    created_at: datetime

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True  # PyObjectId 같은 커스텀 타입 허용
        json_encoders = {ObjectId: str}  # ObjectId를 str으로 변환
