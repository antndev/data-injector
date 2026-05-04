from pathlib import Path
import fitz


def extract(path: Path) -> str:
    doc = fitz.open(str(path))
    pages = [page.get_text() for page in doc]
    doc.close()
    return "\n".join(pages)
