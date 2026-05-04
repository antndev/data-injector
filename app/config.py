from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    inbox_dir: Path = Path("/data/inbox")
    processing_dir: Path = Path("/data/processing")
    done_dir: Path = Path("/data/done")
    failed_dir: Path = Path("/data/failed")
    duplicates_dir: Path = Path("/data/duplicates")
    unsupported_dir: Path = Path("/data/unsupported")
    db_path: Path = Path("/db/ingestor.db")

    qdrant_host: str = "qdrant"
    qdrant_port: int = 6333
    qdrant_api_key: str | None = None
    qdrant_collection: str = "open-webui_knowledge"
    qdrant_knowledge_base_id: str = ""
    embedding_dimensions: int = 768

    ollama_host: str = "http://ollama:11434"
    embedding_model: str = "nomic-embed-text"

    chunk_size: int = 512
    chunk_overlap: int = 50
    worker_concurrency: int = 4

    openwebui_user_id: str

    admin_pin: str = "1234"

    class Config:
        env_file = ".env"

    def create_dirs(self):
        for d in [
            self.inbox_dir, self.processing_dir, self.done_dir,
            self.failed_dir, self.duplicates_dir, self.unsupported_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
