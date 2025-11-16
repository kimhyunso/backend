import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pathlib import Path

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL_VERSION = os.getenv("GEMINI_MODEL_VERSION", "gemini-2.5-flash")
GOOGLE_APPLICATION_CREDENTIALS_NAME = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS_NAME", "gcp-sa.json"
)
GOOGLE_APPLICATION_CREDENTIALS = f"{BASE_DIR}/{GOOGLE_APPLICATION_CREDENTIALS_NAME}"

VERTEX_PROJECT_ID = os.getenv("VERTEX_PROJECT_ID", "ivory-haven-477106-a2")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")

_raw_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173")
origins = [origin.strip() for origin in _raw_origins.split(",") if origin.strip()]
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_DEFAULT_ROLE = os.getenv("GOOGLE_DEFAULT_ROLE", "user")
REFRESH_TOKEN_EXPIRE_DAYS = int(
    os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7")
)  # refresh token 만료 기간


class Settings(BaseSettings):
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    S3_BUCKET: str = os.getenv("AWS_S3_BUCKET", "dupilot-dev-media")
    AWS_REGION: str = os.getenv("AWS_REGION", "ap-northeast-2")
    INGEST_WORKDIR: str = os.getenv("INGEST_WORKDIR", "/tmp/dupilot-ingest")


settings = Settings()
