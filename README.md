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

By default (`DELETE_AFTER_INGEST=true`) a file is **deleted from disk** as soon as its vectors are in Qdrant — the content then lives only in the vector DB, nothing customer-supplied is retained on the server. The database still records that the file was ingested (filename, hash, chunk count), so duplicate detection keeps working. Set `DELETE_AFTER_INGEST=false` to keep the old behaviour of moving finished files to `<DATA_DIR>/done`.

Files that fail land in `<DATA_DIR>/failed` with a `.error` sidecar explaining what went wrong. You can retry failed files from the dashboard (a `done` file's bytes are gone, so re-running it means re-uploading).

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
EMBEDDING_DIMENSIONS=1024   # must match the model — bge-m3 emits 1024

# OpenWebUI
OPENWEBUI_USER_ID=               # your user ID from the OpenWebUI database

# Dashboard login — any characters, no default (app won't start without this)
ADMIN_PASSWORD=your-strong-password
```

The dashboard runs on port 8000. Open it in a browser and enter the password.

After 5 wrong attempts the login is locked for 5 minutes. Audit logs land in `<DB_PATH parent>/logs/auth.log` and are rotated daily, kept for 30 days.

## Uploading files

Drop files **or folders** onto the dashboard's upload zone, click it to pick files, or use "Select a folder…" to pick a whole directory (recursed automatically). Up to three files upload at once; each one starts processing the instant it finishes, without waiting for the rest. Files are sent in chunks (8 MiB), so memory stays flat even for multi-GB files.

**Already-indexed files are skipped before upload.** The dashboard hashes each file (SHA-256) and asks the server whether that content is already in the index; unchanged files in a re-uploaded folder are skipped entirely instead of wasting bandwidth (the summary shows how many were "already indexed"). Files above 256 MiB skip this local hash and are deduplicated server-side after upload instead.

**Uploads are resumable.** Each partial upload is held server-side as a `.part` file whose size is the resume cursor, and tracked in the browser's IndexedDB. So an interrupted upload continues from where it stopped rather than restarting:

- **Page refresh** — incomplete uploads reappear as "resume" chips in the sidebar; on Chromium they continue automatically once you confirm, elsewhere you re-pick the same file.
- **Browser restart** — the durable `.part` survives. On Chromium (File System Access API) click the resume chip and approve the one-time file-access prompt; the upload continues from the server's offset, sending only the remaining bytes. On Firefox/Safari, re-select the same file from the chip — it still resumes from the server offset, never from zero.

Abandoned partials are swept after `UPLOAD_TTL_HOURS` (default 48 h) of inactivity. Closing the tab mid-upload shows a confirmation dialog first.

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
| `DELETE_AFTER_INGEST` | true | Delete the file once its vectors are stored (content lives only in the vector DB). `false` keeps it in `<DATA_DIR>/done` |
| `UPLOAD_CHUNK_BYTES` | 8388608 | Chunk size the dashboard uploads with (8 MiB) |
| `UPLOAD_TTL_HOURS` | 48 | Reap abandoned upload partials after this many hours of inactivity |
| `UPLOAD_MAX_BYTES` | 0 | Reject uploads larger than this; 0 = unlimited |
| `OPENWEBUI_DB_PATH` | /openwebui-data/webui.db | Path to OpenWebUI's shared sqlite DB inside the container |

Higher concurrency helps when embedding is the bottleneck. If Ollama is slow, raising `EMBEDDING_BATCH_SIZE` usually helps more than raising `WORKER_CONCURRENCY`.

## License

PolyForm Noncommercial License 1.0.0 — private and internal use only, no commercial use.
See [LICENSE](LICENSE).
