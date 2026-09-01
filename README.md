# data-injector

Watches a folder for files, extracts their content as structured markdown, and uploads it into an OpenWebUI knowledge base through the public API. OpenWebUI does the splitting and embedding on its own supported path. Has a small web dashboard to see what is going on.

## How it works

Drop a file into the inbox folder (or upload it from the dashboard). The watcher picks it up, runs a brief stability check to make sure the file is not still being written, then hands it off to the worker pool. Each worker:

1. Checks for duplicates by SHA-256 hash
2. Extracts the content depending on the file type, as blocks that keep their origin (slide number, sheet name, page)
3. Runs a vision model over blocks that carry no text of their own, and speech recognition over audio and video
4. Renders the blocks as markdown
5. Uploads that markdown to OpenWebUI and adds it to the knowledge base in batches

By default (`DELETE_AFTER_INGEST=true`) a file is **deleted from disk** as soon as its markdown is in OpenWebUI, so nothing customer-supplied is retained on the ingestor side. The database still records that the file was ingested (filename, hash, chunk count), so duplicate detection keeps working. Set `DELETE_AFTER_INGEST=false` to keep the old behaviour of moving finished files to `<DATA_DIR>/done`.

With `DELETE_AFTER_INGEST=true`, **duplicates and unsupported files are also removed from disk** (their database row still records what happened), so the only customer data retained anywhere is the extracted markdown inside OpenWebUI. With it `false`, duplicates go to `<DATA_DIR>/duplicates` and unsupported files to `<DATA_DIR>/unsupported`.

Files that fail land in `<DATA_DIR>/failed` with a `.error` sidecar explaining what went wrong (failures are kept regardless, so you can diagnose and retry them). You can retry failed files from the dashboard; a `done` file's bytes are gone, so re-running it means re-uploading.

## Setup

Copy `.env.example` to `.env` and fill it in.

```
DATA_DIR=/data
DB_PATH=/db/ingestor.db

OPENWEBUI_URL=http://openwebui:8080
OPENWEBUI_API_KEY=
OPENWEBUI_KNOWLEDGE_ID=

OLLAMA_HOST=http://host.docker.internal:11434
VISION_MODEL=glm-ocr
ASR_LANGUAGE=de

ADMIN_PASSWORD=
```

The dashboard runs on port 8000. Open it in a browser and enter the password.

After 5 wrong attempts the login is locked for 5 minutes. Audit logs land in `<DB_PATH parent>/logs/auth.log` and are rotated daily, kept for 30 days.

## Uploading files

Drop files **or folders** onto the dashboard's upload zone, or use the "Choose files" / "Choose folder" buttons (a folder is recursed automatically). Up to three files upload at once; each one starts processing the instant it finishes, without waiting for the rest. Files are sent in chunks (8 MiB), so memory stays flat even for multi-GB files. An in-progress upload can be cancelled (the partials are dropped server-side).

**Already-indexed files are skipped before upload.** The dashboard hashes each file (SHA-256) and asks the server whether that content is already in the index; unchanged files in a re-uploaded folder are skipped entirely instead of wasting bandwidth (the summary shows how many were "already indexed"). Files above 256 MiB skip this local hash and are deduplicated server-side after upload instead.

**Uploads are resumable.** Each partial upload is held server-side as a `.part` file whose size is the resume cursor, and tracked in the browser's IndexedDB. So an interrupted upload continues from where it stopped rather than restarting:

- **Page refresh**: incomplete uploads reappear as "resume" chips in the sidebar; on Chromium they continue automatically once you confirm, elsewhere you re-pick the same file.
- **Browser restart**: the durable `.part` survives. On Chromium (File System Access API) click the resume chip and approve the one-time file-access prompt; the upload continues from the server's offset, sending only the remaining bytes. On Firefox/Safari, re-select the same file from the chip, it still resumes from the server offset, never from zero.

Abandoned partials are swept after `UPLOAD_TTL_HOURS` (default 48 h) of inactivity. Closing the tab mid-upload shows a confirmation dialog first.

## Supported file types

Documents: PDF, DOCX, DOCM, DOTM, DOC, PPTX, PPTM, PPSX, PPT, XLSX, XLSM, XLSB, XLS, CSV, TXT, MD, HTML, HTM, XML, MSG, EML

Images: PNG, JPG, JPEG, GIF, BMP, TIF, TIFF, WEBP

Video: MP4, MOV, M4V, AVI, MKV, WEBM

Audio: M4A, MP3, WAV, AAC, FLAC, OGG, OPUS

DOC, PPT and the macro formats go through LibreOffice first. A file whose extension lies about its content is routed by what is actually inside it. Images and text-free slides go to a vision model, audio and the sound track of a video go to whisper.cpp.

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
| `WORKER_CONCURRENCY` | 4 | How many files are processed at the same time |
| `OPENWEBUI_BATCH_SIZE` | 15 | Files added to the knowledge base per request. Batching is worth 2.3x over one by one |
| `OPENWEBUI_BATCH_SECONDS` | 5.0 | Flush a partial batch after this much quiet, so a single dropped file never waits for a batch that will not fill |
| `VISION_MODEL` | glm-ocr | Model used for blocks without text of their own |
| `ASR_LANGUAGE` | de | Language passed to whisper.cpp |
| `STABILITY_WAIT_S` | 0 | Seconds to wait after a file appears before touching it (0 = disabled, fine for SFTP / atomic copies) |
| `DELETE_AFTER_INGEST` | true | Delete the file once its markdown is in OpenWebUI. `false` keeps it in `<DATA_DIR>/done` |
| `UPLOAD_CHUNK_BYTES` | 8388608 | Chunk size the dashboard uploads with (8 MiB) |
| `UPLOAD_TTL_HOURS` | 48 | Reap abandoned upload partials after this many hours of inactivity |
| `UPLOAD_MAX_BYTES` | 0 | Reject uploads larger than this; 0 = unlimited |
| `OPENWEBUI_URL` | http://openwebui:8080 | Where OpenWebUI is reachable from this container |

Extraction is the bottleneck, not the upload. Vision and speech recognition dominate the time on a corpus with scans, images and video, so `WORKER_CONCURRENCY` above the number of usable cores buys nothing.

## License

PolyForm Noncommercial License 1.0.0, private and internal use only, no commercial use.
See [LICENSE](LICENSE).
