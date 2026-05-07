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
    embedding_dimensions: int = 1024

    # ── Ollama ────────────────────────────────────────────────────────────────
    ollama_host: str = "http://host.docker.internal:11434"
    embedding_model: str = "bge-m3:latest"

    # ── Pipeline tuning ───────────────────────────────────────────────────────
    # Larger chunks halve the number of embed calls, at the cost of slightly
    # coarser retrieval. 1024 is a good balance for mixed prose / docs.
    chunk_size: int = 1024
    chunk_overlap: int = 100
    worker_concurrency: int = 64
    embedding_batch_size: int = 128
    # Embed/upsert concurrency are GLOBAL caps across the whole worker pool —
    # not per-file. Setting these higher than your embedding server can keep
    # up with just queues requests internally without speedup.
    embed_concurrency: int = 16
    upsert_concurrency: int = 8
    # 0 disables the stability wait entirely. Atomic uploads (SFTP, mv on
    # same FS) need no wait. Bump to ~2 only for non-atomic copies.
    stability_wait_s: int = 0

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
