# data-injector

Watches a folder for files, extracts their text, embeds it via Ollama, and indexes everything into a Qdrant vector store for use with OpenWebUI knowledge bases. Has a small web dashboard to see what is going on.

## How it works

Drop a file into the inbox folder (or upload it from the dashboard). The watcher picks it up, runs a brief stability check to make sure the file is not still being written, then hands it off to the worker pool. Each worker:

1. Checks for duplicates by SHA-256 hash
2. Extracts text depending on the file type
3. Splits the text into chunks
4. Sends chunks to Ollama for embedding
5. Upserts the resulting vectors into Qdrant
6. Registers the file in the OpenWebUI database

Files that finish successfully land in `<DATA_DIR>/done`. Files that fail land in `<DATA_DIR>/failed` with a `.error` sidecar explaining what went wrong. You can retry failed files from the dashboard.

Duplicates (same hash as an already-indexed file) are moved to `<DATA_DIR>/duplicates` and skipped.

## Setup

Copy `.env.example` to `.env` and fill it in.

```
# Where all data lives — inbox, done, failed, duplicates, etc. are
# created automatically as subdirectories on first start.
DATA_DIR=/data
DB_PATH=/db/ingestor.db

# Qdrant
QDRANT_HOST=qdrant
QDRANT_PORT=6333
QDRANT_COLLECTION=open-webui_knowledge
QDRANT_KNOWLEDGE_BASE_ID=        # the UUID of the knowledge base in OpenWebUI

# Ollama
OLLAMA_HOST=http://host.docker.internal:11434
EMBEDDING_MODEL=bge-m3:latest
EMBEDDING_DIMENSIONS=768

# OpenWebUI
OPENWEBUI_USER_ID=               # your user ID from the OpenWebUI database

# Dashboard login — any characters, no default (app won't start without this)
ADMIN_PASSWORD=your-strong-password
```

The dashboard runs on port 8000. Open it in a browser and enter the password.

After 5 wrong attempts the login is locked for 5 minutes. Audit logs land in `<DB_PATH parent>/logs/auth.log` and are rotated daily, kept for 30 days.

## Uploading files

Drop files directly onto the dashboard's upload zone or click it to browse. Folders are supported — the app recurses into them and uploads only the files inside. Each file is queued and starts processing as soon as it finishes uploading, without waiting for the rest. Large files (GBs) are streamed in chunks so memory stays flat.

If you close the browser tab while an upload is in progress, a confirmation dialog appears first. If you force-close anyway, the partial upload is discarded server-side and the inbox is left clean.

## Supported file types

PDF, DOCX, DOC, PPTX, PPT, XLSX, XLS, XLSB, CSV, TXT, MD, HTML, XML, MSG

DOC and PPT files are converted to their modern equivalents before processing. Everything else is parsed directly.

## Dashboard

The dashboard shows all ingested files with their status, chunk count, file size, and timestamp. It has a live activity log and a rate strip showing how many files per second are being ingested and completed.

From the dashboard you can:
- Filter files by status
- Search by filename
- Retry individual failed files or a whole selection
- Delete files from disk, the database, and the knowledge base
- Bulk-delete all failed, duplicate, or unsupported files at once
- Upload files directly via drag-and-drop or file picker

## Tuning

| Variable | Default | What it controls |
|---|---|---|
| `WORKER_CONCURRENCY` | 64 | How many files are processed at the same time |
| `EMBEDDING_BATCH_SIZE` | 128 | Chunks sent per Ollama embed request |
| `EMBED_CONCURRENCY` | 16 | Global cap on concurrent embed batches |
| `UPSERT_CONCURRENCY` | 8 | Global cap on concurrent Qdrant upserts |
| `CHUNK_SIZE` | 1024 | Characters per chunk |
| `CHUNK_OVERLAP` | 100 | Overlap between consecutive chunks |
| `STABILITY_WAIT_S` | 0 | Seconds to wait after a file appears before touching it (0 = disabled, fine for SFTP / atomic copies) |

Higher concurrency helps when embedding is the bottleneck. If Ollama is slow, raising `EMBEDDING_BATCH_SIZE` usually helps more than raising `WORKER_CONCURRENCY`.

## License

PolyForm Noncommercial License 1.0.0 — private and internal use only, no commercial use.
See [LICENSE](LICENSE).
