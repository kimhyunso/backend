from typing import List
from fastapi import HTTPException
from .models import LanguageCreate, LanguageUpdate, Language
from ..deps import DbDep

class LanguageService:
    def __init__(self, db: DbDep):
        self.collection = db.get_collection("languages")

    async def list_languages(self) -> List[Language]:
        docs = (
            await self.collection.find({}, {"_id": 0})
            .sort("sort", 1)
            .to_list(None)
        )
        return [Language(**doc) for doc in docs]

    async def get_language(self, code: str) -> Language:
        doc = await self.collection.find_one({"language_code": code}, {"_id": 0})
        if not doc:
            raise HTTPException(status_code=404, detail="language not found")
        return Language(**doc)

    async def create_language(self, payload: LanguageCreate) -> Language:
        exists = await self.collection.find_one({"language_code": payload.language_code})
        if exists:
            raise HTTPException(status_code=409, detail="language already exists")
        await self.collection.insert_one(payload.model_dump())
        return await self.get_language(payload.language_code)

    async def update_language(self, code: str, payload: LanguageUpdate) -> Language:
        result = await self.collection.update_one(
            {"language_code": code},
            {"$set": payload.model_dump(exclude_unset=True)},
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="language not found")
        return await self.get_language(code)

    async def delete_language(self, code: str) -> None:
        result = await self.collection.delete_one({"language_code": code})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="language not found")

    async def ensure_defaults(self, defaults: List[LanguageCreate]) -> List[Language]:
        for language in defaults:
            await self.collection.update_one(
                {"language_code": language.language_code},
                {"$set": language.model_dump()},
                upsert=True,
            )
        return await self.list_languages()
