import os, boto3
from dotenv import load_dotenv
from .env import settings

load_dotenv()


aws_profile = os.getenv("AWS_PROFILE")
aws_region = os.getenv("AWS_REGION", "ap-northeast-2")

session_kwargs = {}
if aws_profile:
    session_kwargs["profile_name"] = aws_profile

session = boto3.Session(**session_kwargs, region_name=aws_region)
s3 = session.client("s3")


def drop_projects(project_id):
    bucket = settings.S3_BUCKET
    prefix = f"projects/{project_id}/"
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_payload = {
            "Objects": [{"Key": obj["Key"]} for obj in objects],
            "Quiet": True,
        }
        s3.delete_objects(Bucket=bucket, Delete=delete_payload)


