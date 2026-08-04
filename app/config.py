from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    data_dir: Path = Path("/data")
    db_path: Path = Path("/db/ingestor.db")

    # Baked into the image by CI (see Dockerfile ARG APP_VERSION). Reported on
    # /health so the running build is always identifiable.
    app_version: str = "dev"

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
    def uploads_tmp_dir(self) -> Path:
        # Durable staging for resumable web uploads. A partial upload lives
        # here as <id>.part (+ <id>.json sidecar) until it is complete, then
        # is atomically renamed into the inbox. The .part survives a server
        # restart, so an interrupted upload can resume from its current size.
        return self.data_dir / "_uploads"

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
    # Path to OpenWebUI's shared sqlite DB inside this container. Configurable
    # so the app isn't silently broken if the volume is mounted elsewhere or
    # OpenWebUI changes its layout. A startup check logs a loud error if it's
    # wrong (the app keeps running; affected files surface as 'failed').
    openwebui_db_path: Path = Path("/openwebui-data/webui.db")

    # ── Storage model ─────────────────────────────────────────────────────────
    # When True (default), a file is removed from disk as soon as its vectors
    # are in Qdrant — so customer data lives ONLY in the vector DB, never
    # retained as a copy on the server. Set False to keep the old behaviour
    # of moving finished files into <DATA_DIR>/done.
    delete_after_ingest: bool = True

    # ── Resumable web upload ──────────────────────────────────────────────────
    # Server tolerates any chunk size; this is the size the dashboard uses.
    upload_chunk_bytes: int = 8 * 1024 * 1024
    # Abandoned partial uploads are reaped after this many hours of inactivity
    # (measured against the .part's last-modified time, so a paused-but-alive
    # upload is never swept).
    upload_ttl_hours: int = 48
    # 0 = unlimited. Otherwise reject an upload whose declared size exceeds this.
    upload_max_bytes: int = 0

    # ── Auth ─────────────────────────────────────────────────────────────────
    admin_password: str  # no default — app won't start without this set
    auth_log_retention_days: int = 30
    auth_log_max_total_mb: int = 100

    class Config:
        env_file = ".env"

    def create_dirs(self):
        # Always-needed dirs. The category dirs (done/failed/duplicates/
        # unsupported) are created on demand by worker._move when a file is
        # actually moved there — under delete_after_ingest they'd otherwise sit
        # permanently empty.
        dirs = [self.inbox_dir, self.processing_dir, self.uploads_tmp_dir, self.log_dir]
        if not self.delete_after_ingest:
            dirs += [self.done_dir, self.failed_dir,
                     self.duplicates_dir, self.unsupported_dir]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
