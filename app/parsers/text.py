from pathlib import Path


def extract_txt(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def extract_csv(path: Path) -> str:
    import csv
    rows = []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.reader(f):
            rows.append(" | ".join(row))
    return "\n".join(rows)


def extract_html(path: Path) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(path.read_bytes(), "lxml")
    return soup.get_text(separator="\n", strip=True)


def extract_xml(path: Path) -> str:
    import xml.etree.ElementTree as ET
    tree = ET.parse(str(path))
    texts = [el.text.strip() for el in tree.iter() if el.text and el.text.strip()]
    return "\n".join(texts)
