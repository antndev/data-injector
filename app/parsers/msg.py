from pathlib import Path


def extract(path: Path) -> str:
    import extract_msg
    with extract_msg.openMsg(str(path)) as msg:
        parts = [
            f"Subject: {msg.subject or ''}",
            f"From: {msg.sender or ''}",
            f"To: {msg.to or ''}",
            f"Date: {msg.date or ''}",
            "",
            msg.body or "",
        ]
    return "\n".join(parts)
