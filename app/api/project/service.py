from fastapi import HTTPException, status
from datetime import datetime
from typing import Optional, List
from bson import ObjectId
from ..deps import DbDep
from .models import (
    ProjectCreate,
    ProjectUpdate,
    ProjectPublic,
    ProjectBase,
    ProjectOut,
    ProjectTargetStatus,
    ProjectTarget,
    ProjectTargetUpdate,
)
from app.config.s3 import drop_projects
from app.config.env import settings


class ProjectService:
    def __init__(self, db: DbDep):
        self.db = db
        self.project_collection = db.get_collection("projects")
        self.segment_collection = db.get_collection("segments")
        self.target_collection = db.get_collection("project_targets")
        self.bucket = settings.S3_BUCKET

    async def get_project_by_id(self, project_id: str) -> ProjectPublic:
        doc = await self.project_collection.find_one({"_id": ObjectId(project_id)})
        doc["project_id"] = str(doc["_id"])
        return ProjectPublic.model_validate(doc)

    async def get_project_paging(
        self,
        user_id: Optional[str] = None,
        sort: str = "created_at",
        page: int = 1,
        limit: int = 6,
    ) -> List[ProjectOut]:
        skip = (page - 1) * limit
        docs = (
            await self.project_collection.find({"owner_code": user_id})
            .sort([(sort, -1)])
            .skip(skip)
            .limit(limit)
            .to_list(length=limit)
        )

        project_ids = [doc["_id"] for doc in docs]

        pipeline = [
            {"$match": {"project_id": {"$in": project_ids}}},
            {
                "$lookup": {
                    "from": "issues",
                    "let": {"segmentId": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$segment_id", "$$segmentId"]}}},
                        {"$count": "count"},
                    ],
                    "as": "issue_docs",
                }
            },
            {
                "$addFields": {
                    "issue_count": {"$ifNull": [{"$first": "$issue_docs.count"}, 0]}
                }
            },
            {
                "$group": {
                    "_id": "$project_id",
                    "issue_count": {"$sum": "$issue_count"},
                }
            },
        ]
        issue_counts = await self.segment_collection.aggregate(pipeline).to_list(
            length=None
        )
        issue_map = {row["_id"]: row["issue_count"] for row in issue_counts}

        result = []
        for doc in docs:
            doc["issue_count"] = issue_map.get(doc["_id"], 0)
            result.append(ProjectOut.model_validate(doc))
        return result

    async def list_projects_with_targets(self) -> List[ProjectOut]:
        pipeline = [
            {"$addFields": {"project_id_str": {"$toString": "$_id"}}},
            {"$sort": {"created_at": -1}},
            {
                "$lookup": {
                    "from": "project_targets",
                    "localField": "project_id_str",
                    "foreignField": "project_id",
                    "as": "targets",
                }
            },
        ]
        docs = await self.project_collection.aggregate(pipeline).to_list(length=None)
        for doc in docs:
            doc["targets"] = doc.get("targets") or []
        return [ProjectOut.model_validate(doc) for doc in docs]

    async def delete_project(self, project_id: str) -> int:
        drop_projects(project_id=project_id)
        result = await self.project_collection.delete_one({"_id": project_id})
        return result.deleted_count

    async def create_project(self, payload: ProjectCreate) -> str:
        now = datetime.now()
        base = ProjectBase(
            owner_id=payload.owner_id,
            title=payload.title,
            source_type=payload.sourceType,
            video_source=None,
            source_language=payload.sourceLanguage,
            status="uploading",
            created_at=now,
            speaker_count=payload.speakerCount,
        )
        doc = base.model_dump(exclude_none=True)
        result = await self.project_collection.insert_one(doc)
        # 프로젝트 생성 시, 타겟(타겟 언어별 진행도) 생성
        project_id = str(result.inserted_id)
        await self._create_project_targets(project_id, payload.targetLanguages)
        return {"project_id": project_id}

    async def update_project(self, payload: ProjectUpdate) -> ProjectPublic:
        project_id = payload.project_id
        update_data = payload.model_dump(exclude={"project_id"}, exclude_none=True)
        update_data["updated_at"] = datetime.now()

        result = await self.project_collection.update_one(
            {"_id": ObjectId(project_id)},
            {"$set": update_data},
        )

        doc = await self.project_collection.find_one({"_id": ObjectId(project_id)})
        if not doc:
            raise HTTPException(status_code=404, detail="Project not found")

        doc["project_id"] = str(doc["_id"])

        if result.matched_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Project not found",
            )

        return ProjectPublic.model_validate(doc)

    async def _create_project_targets(
        self, project_id: str, target_languages: List[str] | None
    ) -> None:
        if not target_languages:
            return
        now = datetime.now()
        docs = []
        for code in target_languages:
            lang = (code or "").strip()
            if not lang:
                continue
            docs.append(
                {
                    "project_id": project_id,
                    "language_code": lang,
                    "status": ProjectTargetStatus.PENDING.value,
                    "progress": 0,
                    "created_at": now,
                }
            )
        if docs:
            await self.target_collection.insert_many(docs)

    async def get_targets_by_project(
        self, project_id: str, language_code: str | None = None
    ) -> list[ProjectTarget]:
        query = {"project_id": project_id}
        if language_code:
            query["language_code"] = language_code
        docs = await self.target_collection.find(query).to_list(length=None)

        result = []
        for doc in docs:
            doc["target_id"] = str(doc["_id"])
            result.append(ProjectTarget.model_validate(doc))
        return result

    async def update_target(
        self, target_id: str, payload: ProjectTargetUpdate
    ) -> ProjectTarget:
        doc = await self.target_collection.find_one({"_id": ObjectId(target_id)})
        if not doc:
            raise HTTPException(status_code=404, detail="Target not found")

        update_data = payload.model_dump(exclude_none=True)
        update_data["updated_at"] = datetime.now()
        await self.target_collection.update_one(
            {"_id": ObjectId(target_id)},
            {"$set": update_data},
            # {"$set": {**update_data, "project_id": doc["project_id"]}},
        )
        doc = await self.target_collection.find_one({"_id": ObjectId(target_id)})
        doc["target_id"] = str(doc["_id"])
        return doc

    async def update_targets_by_project_and_language(
        self, project_id: str, language_code: str, payload: ProjectTargetUpdate
    ) -> ProjectTarget:
        doc = await self.target_collection.find_one(
            {"project_id": project_id, "language_code": language_code}
        )
        if not doc:
            raise HTTPException(status_code=404, detail="Targets not found")

        update_data = payload.model_dump(exclude_none=True)
        update_data["updated_at"] = datetime.now()

        await self.target_collection.update_one(
            {"project_id": project_id, "language_code": language_code},
            {"$set": update_data},
        )
        doc = await self.target_collection.find_one(
            {"project_id": project_id, "language_code": language_code}
        )
        doc["target_id"] = str(doc["_id"])
        return doc
