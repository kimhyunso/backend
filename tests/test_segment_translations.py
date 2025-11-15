#!/usr/bin/env python3
"""
Test script to verify segment_translations creation during TTS completion.
This script simulates a worker callback to test the segment translations feature.
"""

import asyncio
import json
from datetime import datetime
from bson import ObjectId
from app.config.db import make_db
from app.api.jobs.models import JobUpdateStatus, JobStatus
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_segment_translations():
    """Test segment_translations creation during TTS completion"""

    # Connect to MongoDB
    db = make_db()

    try:
        # Step 1: Find a test project with targets
        project = await db["projects"].find_one({
            "project_targets": {"$exists": True, "$ne": []}
        })

        if not project:
            logger.error("No project with targets found. Please create a test project first.")
            return

        project_id = str(project["_id"])

        # Step 2: Check existing segments
        existing_segments = await db["project_segments"].count_documents({
            "project_id": project["_id"]
        })

        # Step 3: Find a job for this project
        job = await db["jobs"].find_one({
            "project_id": project_id
        })

        if not job:
            logger.error("No job found for this project")
            return

        job_id = str(job["_id"])
        target_lang = job.get("target_lang", "ko")

        # Step 4: Simulate TTS completion callback
        test_segments = [
            {
                "segment_id": "0",
                "seg_idx": 0,
                "speaker": "Speaker1",
                "start": 0.0,
                "end": 5.5,
                "prompt_text": "안녕하세요, 테스트 번역입니다.",  # Translated text
                "audio_file": f"s3://test-bucket/audio/segment_0_{target_lang}.wav"
            },
            {
                "segment_id": "1",
                "seg_idx": 1,
                "speaker": "Speaker2",
                "start": 5.5,
                "end": 10.2,
                "prompt_text": "이것은 두 번째 세그먼트입니다.",  # Translated text
                "audio_file": f"s3://test-bucket/audio/segment_1_{target_lang}.wav"
            }
        ]

        # Step 5: Create the callback payload
        metadata = {
            "stage": "tts_completed",
            "target_lang": target_lang,
            "segments": test_segments
        }

        # Step 6: Call the callback function directly
        from app.api.jobs.routes import set_job_status
        from app.api.deps import DbDep

        # Create mock payload
        payload = JobUpdateStatus(
            status=JobStatus.DONE,
            result_key=f"s3://test-bucket/videos/dubbed_{target_lang}.mp4",
            metadata=metadata
        )

        # Execute the callback
        result = await set_job_status(job_id, payload, db)

        # Step 7: Verify results
        # Check project_segments
        segments_count = await db["project_segments"].count_documents({
            "project_id": project["_id"]
        })

        # Check segment_translations
        translations = await db["segment_translations"].find({
            "language_code": target_lang
        }).to_list(None)

        # Check assets
        assets = await db["assets"].find({
            "project_id": project_id,
            "language_code": target_lang
        }).to_list(None)


    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
    finally:
        # Clean up
        pass

if __name__ == "__main__":
    asyncio.run(test_segment_translations())