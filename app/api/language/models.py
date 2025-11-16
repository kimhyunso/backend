from typing import Optional
from pydantic import BaseModel, Field


class LanguageBase(BaseModel):
    name_ko: str = Field(..., min_length=1)
    name_en: str = Field(..., min_length=1)


class LanguageCreate(LanguageBase):
    language_code: str = Field(..., min_length=2, max_length=8)
    sort: int = Field(..., ge=0, description="언어 정렬 순서 (오름차순)")


class LanguageUpdate(BaseModel):
    name_ko: Optional[str] = Field(default=None, min_length=1)
    name_en: Optional[str] = Field(default=None, min_length=1)
    sort: Optional[int] = Field(default=None, ge=0)


class Language(LanguageBase):
    language_code: str = Field(..., min_length=2, max_length=8)
    sort: int = Field(default=-1, ge=0, description="언어 정렬 순서 (오름차순)")

    class Config:
        orm_mode = True
