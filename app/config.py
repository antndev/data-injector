from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    data_dir: Path = Path("/data")
    db_path: Path = Path("/db/ingestor.db")

    # ── Derived paths (not env vars) ─────────────────────────────────────────
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
    def log_dir(self) -> Path:
        return self.db_path.parent / "logs"

    # ── Qdrant ────────────────────────────────────────────────────────────────
    qdrant_host: str = "qdrant"
    qdrant_port: int = 6333
    qdrant_api_key: str | None = None
    qdrant_collection: str = "open-webui_knowledge"
    qdrant_knowledge_base_id: str = ""
    embedding_dimensions: int = 768

    # ── Ollama ────────────────────────────────────────────────────────────────
    ollama_host: str = "http://ollama:11434"
    embedding_model: str = "nomic-embed-text"

    # ── Pipeline tuning ───────────────────────────────────────────────────────
    chunk_size: int = 512
    chunk_overlap: int = 50
    worker_concurrency: int = 64
    embedding_batch_size: int = 128
    embed_concurrency: int = 8
    upsert_concurrency: int = 4
    stability_wait_s: int = 2

    # ── OpenWebUI ────────────────────────────────────────────────────────────
    openwebui_user_id: str

    # ── Auth ─────────────────────────────────────────────────────────────────
    admin_password: str  # no default — app won't start without this set
    auth_log_retention_days: int = 30
    auth_log_max_total_mb: int = 100

    class Config:
        env_file = ".env"

    def create_dirs(self):
        for d in [
            self.inbox_dir, self.processing_dir, self.done_dir,
            self.failed_dir, self.duplicates_dir, self.unsupported_dir,
            self.log_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
