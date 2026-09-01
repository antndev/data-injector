from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    data_dir: Path = Path("/data")
    db_path: Path = Path("/db/ingestor.db")

    app_version: str = "dev"

    @property
    def inbox_dir(self) -> Path:
        return self.data_dir / "inbox"

    @property
    def processing_dir(self) -> Path:
        return self.data_dir / "processing"

    @property
    def done_dir(self) -> Path:
        return self.data_dir / "done"

    @property
    def failed_dir(self) -> Path:
        return self.data_dir / "failed"

    @property
    def duplicates_dir(self) -> Path:
        return self.data_dir / "duplicates"

    @property
    def unsupported_dir(self) -> Path:
        return self.data_dir / "unsupported"

    @property
    def uploads_tmp_dir(self) -> Path:
        return self.data_dir / "_uploads"

    @property
    def log_dir(self) -> Path:
        return self.db_path.parent / "logs"

    openwebui_url: str = "http://openwebui:8080"
    openwebui_api_key: str
    openwebui_knowledge_id: str
    openwebui_batch_size: int = 15
    openwebui_batch_seconds: float = 5.0
    stability_wait_s: int = 0

    vision_model: str = "glm-ocr"
    vision_enabled: bool = True
    asr_enabled: bool = True
    asr_language: str = "de"
    ollama_host: str = "http://host.docker.internal:11434"
    worker_concurrency: int = 4

    delete_after_ingest: bool = True

    upload_chunk_bytes: int = 8 * 1024 * 1024
    upload_ttl_hours: int = 48
    upload_max_bytes: int = 0

    admin_password: str
    auth_log_retention_days: int = 30
    auth_log_max_total_mb: int = 100

    class Config:
        env_file = ".env"

    def create_dirs(self):
        dirs = [self.inbox_dir, self.processing_dir, self.uploads_tmp_dir, self.log_dir]
        if not self.delete_after_ingest:
            dirs += [self.done_dir, self.failed_dir,
                     self.duplicates_dir, self.unsupported_dir]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
