from fastapi import HTTPException, status
from datetime import datetime
from bson import ObjectId
from bson.errors import InvalidId
from pymongo.errors import PyMongoError
from typing import Optional, List, Tuple

from ..deps import DbDep
from ..auth.model import UserOut
from .models import VoiceSampleCreate, VoiceSampleUpdate, VoiceSampleOut


class VoiceSampleService:
    def __init__(self, db: DbDep):
        self.collection = db.get_collection("voice_samples")
        self.favorites_collection = db.get_collection("user_favorites")

    async def create_voice_sample(
        self, data: VoiceSampleCreate, owner: UserOut
    ) -> VoiceSampleOut:
        """음성 샘플 생성"""
        try:
            owner_oid = ObjectId(owner.id)
        except InvalidId as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid owner_id"
            ) from exc

        sample_data = {
            "owner_id": owner_oid,
            "name": data.name,
            "description": data.description,
            "is_public": data.is_public,
            "file_path_wav": data.file_path_wav,
            "audio_sample_url": data.audio_sample_url,
            "prompt_text": data.prompt_text,
            "created_at": datetime.utcnow(),
            "country": data.country,
            "gender": data.gender,
            "avatar_image_path": data.avatar_image_path,
        }

        try:
            result = await self.collection.insert_one(sample_data)
            sample_data["_id"] = result.inserted_id
            return VoiceSampleOut(**sample_data, is_favorite=False)
        except PyMongoError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create voice sample: {exc}",
            ) from exc

    async def get_voice_sample(
        self, sample_id: str, current_user: Optional[UserOut] = None
    ) -> VoiceSampleOut:
        """음성 샘플 상세 조회"""
        try:
            sample_oid = ObjectId(sample_id)
        except InvalidId as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid sample_id"
            ) from exc

        sample = await self.collection.find_one({"_id": sample_oid})
        if not sample:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Voice sample not found"
            )

        # 공개 여부 확인
        if not sample.get("is_public", False):
            if not current_user or str(sample["owner_id"]) != current_user.id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have permission to access this voice sample",
                )

        # 즐겨찾기 여부 확인
        is_favorite = False
        if current_user:
            try:
                user_oid = ObjectId(current_user.id)
                favorite = await self.favorites_collection.find_one(
                    {
                        "user_id": user_oid,
                        "sample_id": sample_oid,
                        "type": "voice_sample",
                    }
                )
                is_favorite = favorite is not None
            except Exception:
                pass

        favorite_total = await self.favorites_collection.count_documents(
            {"type": "voice_sample", "sample_id": sample_oid}
        )

        return VoiceSampleOut(
            **sample,
            is_favorite=is_favorite,
            favorite_count=favorite_total,
        )

    async def list_voice_samples(
        self,
        current_user: Optional[UserOut] = None,
        q: Optional[str] = None,
        favorites_only: bool = False,
        my_samples_only: bool = False,
        page: int = 1,
        limit: int = 20,
    ) -> Tuple[List[VoiceSampleOut], int]:
        """음성 샘플 목록 조회"""
        # 필터 구성
        filter_query = {}

        # 검색어 필터
        search_or = None
        if q:
            search_or = [
                {"name": {"$regex": q, "$options": "i"}},
                {"description": {"$regex": q, "$options": "i"}},
            ]

        # 내 샘플만 필터
        if my_samples_only:
            if not current_user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Authentication required",
                )
            try:
                owner_oid = ObjectId(current_user.id)
                filter_query["owner_id"] = owner_oid
            except InvalidId:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user_id"
                )

        # 즐겨찾기만 필터
        if favorites_only:
            if not current_user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Authentication required",
                )
            try:
                user_oid = ObjectId(current_user.id)
                favorites = await self.favorites_collection.find(
                    {"user_id": user_oid, "type": "voice_sample"}
                ).to_list(length=None)
                favorite_sample_ids = [
                    fav["sample_id"] for fav in favorites if "sample_id" in fav
                ]
                if not favorite_sample_ids:
                    return [], 0
                filter_query["_id"] = {"$in": favorite_sample_ids}
            except InvalidId:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user_id"
                )

        # 공개 샘플 또는 소유자 샘플만 조회
        visibility_or = None
        if not my_samples_only and not favorites_only:
            if current_user:
                try:
                    owner_oid = ObjectId(current_user.id)
                    visibility_or = [
                        {"is_public": True},
                        {"owner_id": owner_oid},
                    ]
                except InvalidId:
                    pass
            else:
                filter_query["is_public"] = True

        # 검색어와 공개/비공개 필터 병합
        if search_or and visibility_or:
            # $and를 사용하여 두 조건을 모두 만족하도록 함
            filter_query["$and"] = [
                {"$or": search_or},
                {"$or": visibility_or},
            ]
        elif search_or:
            filter_query["$or"] = search_or
        elif visibility_or:
            filter_query["$or"] = visibility_or

        # 총 개수 조회
        total = await self.collection.count_documents(filter_query)

        # 페이지네이션
        skip = (page - 1) * limit
        cursor = (
            self.collection.find(filter_query)
            .sort("created_at", -1)
            .skip(skip)
            .limit(limit)
        )

        samples = await cursor.to_list(length=limit)

        # 즐겨찾기 정보 추가
        if current_user:
            try:
                user_oid = ObjectId(current_user.id)
                favorite_docs = await self.favorites_collection.find(
                    {
                        "user_id": user_oid,
                        "type": "voice_sample",
                        "sample_id": {"$in": [s["_id"] for s in samples]},
                    }
                ).to_list(length=None)
                favorite_ids = {str(fav["sample_id"]) for fav in favorite_docs}
            except Exception:
                favorite_ids = set()
        else:
            favorite_ids = set()

        favorite_counts: dict[str, int] = {}
        if samples:
            sample_ids = [sample["_id"] for sample in samples]
            count_docs = await self.favorites_collection.aggregate(
                [
                    {"$match": {"type": "voice_sample", "sample_id": {"$in": sample_ids}}},
                    {"$group": {"_id": "$sample_id", "count": {"$sum": 1}}},
                ]
            ).to_list(length=None)
            favorite_counts = {
                str(doc["_id"]): int(doc.get("count", 0)) for doc in count_docs
            }

        result = [
            VoiceSampleOut(
                **sample,
                is_favorite=str(sample["_id"]) in favorite_ids,
                favorite_count=favorite_counts.get(str(sample["_id"]), 0),
            )
            for sample in samples
        ]

        return result, total

    async def update_voice_sample(
        self, sample_id: str, data: VoiceSampleUpdate, owner: UserOut
    ) -> VoiceSampleOut:
        """음성 샘플 업데이트"""
        try:
            sample_oid = ObjectId(sample_id)
        except InvalidId as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid sample_id"
            ) from exc

        sample = await self.collection.find_one({"_id": sample_oid})
        if not sample:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Voice sample not found"
            )

        # 소유자 확인
        if str(sample["owner_id"]) != owner.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only update your own voice samples",
            )

        # 업데이트 데이터 구성
        update_data = {}
        if data.name is not None:
            update_data["name"] = data.name
        if data.description is not None:
            update_data["description"] = data.description
        if data.is_public is not None:
            update_data["is_public"] = data.is_public
        if data.audio_sample_url is not None:
            update_data["audio_sample_url"] = data.audio_sample_url
        if data.prompt_text is not None:
            update_data["prompt_text"] = data.prompt_text
        if data.country is not None:
            update_data["country"] = data.country
        if data.gender is not None:
            update_data["gender"] = data.gender
        if getattr(data, "avatar_image_path", None) is not None:
            update_data["avatar_image_path"] = data.avatar_image_path

        if not update_data:
            return VoiceSampleOut(**sample, is_favorite=False)

        try:
            updated = await self.collection.find_one_and_update(
                {"_id": sample_oid},
                {"$set": update_data},
                return_document=True,
            )
            if not updated:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Voice sample not found",
                )
            return VoiceSampleOut(**updated, is_favorite=False)
        except PyMongoError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update voice sample: {exc}",
            ) from exc

    async def delete_voice_sample(self, sample_id: str, owner: UserOut) -> None:
        """음성 샘플 삭제"""
        try:
            sample_oid = ObjectId(sample_id)
        except InvalidId as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid sample_id"
            ) from exc

        sample = await self.collection.find_one({"_id": sample_oid})
        if not sample:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Voice sample not found"
            )

        # 소유자 확인
        if str(sample["owner_id"]) != owner.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only delete your own voice samples",
            )

        try:
            # 샘플 삭제
            result = await self.collection.delete_one({"_id": sample_oid})
            if result.deleted_count == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Voice sample not found",
                )

            # 관련 즐겨찾기도 삭제
            await self.favorites_collection.delete_many(
                {"sample_id": sample_oid, "type": "voice_sample"}
            )
        except PyMongoError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete voice sample: {exc}",
            ) from exc

    async def add_favorite(self, sample_id: str, user: UserOut) -> None:
        """즐겨찾기 추가"""
        try:
            sample_oid = ObjectId(sample_id)
            user_oid = ObjectId(user.id)
        except InvalidId as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ID"
            ) from exc

        # 샘플 존재 확인
        sample = await self.collection.find_one({"_id": sample_oid})
        if not sample:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Voice sample not found"
            )

        # 공개 여부 또는 소유자 확인
        if not sample.get("is_public", False) and str(sample["owner_id"]) != user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to favorite this voice sample",
            )

        # 이미 즐겨찾기에 있는지 확인
        existing = await self.favorites_collection.find_one(
            {"user_id": user_oid, "sample_id": sample_oid, "type": "voice_sample"}
        )
        if existing:
            return  # 이미 추가되어 있음

        # 즐겨찾기 추가
        try:
            await self.favorites_collection.insert_one(
                {
                    "user_id": user_oid,
                    "sample_id": sample_oid,
                    "type": "voice_sample",
                    "created_at": datetime.utcnow(),
                }
            )
        except PyMongoError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to add favorite: {exc}",
            ) from exc

    async def remove_favorite(self, sample_id: str, user: UserOut) -> None:
        """즐겨찾기 제거"""
        try:
            sample_oid = ObjectId(sample_id)
            user_oid = ObjectId(user.id)
        except InvalidId as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ID"
            ) from exc

        try:
            result = await self.favorites_collection.delete_one(
                {"user_id": user_oid, "sample_id": sample_oid, "type": "voice_sample"}
            )
            if result.deleted_count == 0:
                # 이미 제거되어 있거나 존재하지 않음 (에러 없이 처리)
                pass
        except PyMongoError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to remove favorite: {exc}",
            ) from exc
