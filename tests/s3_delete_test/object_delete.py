import json
from pathlib import Path
import boto3

BUCKET = "dupilot-dev-media"  # 필요하면 env에서 읽어와도 됩니다.
EXCLUDE_FILE = Path(__file__).parent / "exclude_project_id.json"


def load_excluded_ids() -> set[str]:
    data = json.loads(EXCLUDE_FILE.read_text())
    # exclude_project_id.json 을 [{"_id": "..."}] 형태의 배열로 만들어 두세요.
    return {item["_id"] for item in data}


def should_delete(key: str, excluded: set[str]) -> bool:
    # 프로젝트 오브젝트 키는 projects/<project_id>/... 형태라고 가정
    if not key.startswith("projects/"):
        return True
    project_id = key.split("/", 2)[1]
    return project_id not in excluded


def delete_objects():
    excluded = load_excluded_ids()
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET):
        contents = page.get("Contents", [])
        targets = [
            {"Key": obj["Key"]}
            for obj in contents
            if should_delete(obj["Key"], excluded)
        ]
        if not targets:
            continue
        # S3 는 한번에 1000개까지 삭제 가능
        chunks = [targets[i : i + 1000] for i in range(0, len(targets), 1000)]
        for chunk in chunks:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": chunk, "Quiet": True})
            print(f"Deleted {len(chunk)} objects")


if __name__ == "__main__":
    delete_objects()
