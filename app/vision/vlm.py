"""Image description through a vision model in Ollama."""

from __future__ import annotations

import base64, json, re, time, urllib.request

HOST = "http://localhost:11434"

PROMPT = "Extract all text from this image."

MAX_TOKENS = 768

PROMPT_STRUCTURE = (
    "Describe the structure of this diagram: type, hierarchy, arrow directions, "
    "and which elements connect to which."
)


def clean(text: str) -> str:
    """Strips markdown fences and collapses repeated segments.

    glm-ocr sometimes wraps its answer in ```markdown and repeats the content,
    either whole or in parts. Both would end up in the chunk otherwise."""
    text = re.sub(r"```[a-zA-Z]*\n?", "", text).strip()
    previous = None
    while previous != text:
        previous = text
        text = re.sub(r"(?s)(.{15,}?)\s*\1(?=\s|$)", r"\1", text)
    return text.strip()


def describe(
    png: bytes, model: str, prompt: str = PROMPT, host: str = HOST, timeout: int = 300
) -> dict:

    payload = {
        "model": model,
        "prompt": prompt,
        "images": [base64.b64encode(png).decode()],
        "stream": False,
        "options": {"temperature": 0, "num_predict": MAX_TOKENS},
    }
    t0 = time.time()
    req = urllib.request.Request(
        f"{host}/api/generate",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.load(r)
    return {
        "text": clean(d.get("response", "")),
        "duration_s": time.time() - t0,
        "model": model,
    }
