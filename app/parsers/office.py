from pathlib import Path


def extract_pptx(path: Path) -> str:
    from pptx import Presentation
    prs = Presentation(str(path))
    parts = []
    for i, slide in enumerate(prs.slides, 1):
        texts = []
        for shape in slide.shapes:
            if shape.has_text_frame:
                texts.append(shape.text_frame.text.strip())
        if slide.has_notes_slide:
            notes = slide.notes_slide.notes_text_frame.text.strip()
            if notes:
                texts.append(f"[Notes] {notes}")
        if texts:
            parts.append(f"--- Slide {i} ---\n" + "\n".join(t for t in texts if t))
    return "\n\n".join(parts)


def extract_docx(path: Path) -> str:
    from docx import Document
    from docx.oxml.ns import qn

    doc = Document(str(path))
    parts = []

    for para in doc.paragraphs:
        if para.text.strip():
            parts.append(para.text.strip())

    for table in doc.tables:
        for row in table.rows:
            row_text = " | ".join(c.text.strip() for c in row.cells if c.text.strip())
            if row_text:
                parts.append(row_text)

    return "\n".join(parts)


def extract_xlsx(path: Path) -> str:
    import openpyxl
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    parts = []
    for sheet in wb.sheetnames:
        ws = wb[sheet]
        parts.append(f"--- Sheet: {sheet} ---")
        for row in ws.iter_rows(values_only=True):
            row_text = " | ".join(str(c) for c in row if c is not None)
            if row_text.strip():
                parts.append(row_text)
    wb.close()
    return "\n".join(parts)


def extract_xls(path: Path) -> str:
    import xlrd
    wb = xlrd.open_workbook(str(path))
    parts = []
    for sheet in wb.sheets():
        parts.append(f"--- Sheet: {sheet.name} ---")
        for rx in range(sheet.nrows):
            row_text = " | ".join(str(v) for v in sheet.row_values(rx) if str(v).strip())
            if row_text.strip():
                parts.append(row_text)
    return "\n".join(parts)


def extract_xlsb(path: Path) -> str:
    from pyxlsb import open_workbook
    parts = []
    with open_workbook(str(path)) as wb:
        for name in wb.sheets:
            parts.append(f"--- Sheet: {name} ---")
            with wb.get_sheet(name) as sheet:
                for row in sheet.rows():
                    row_text = " | ".join(str(c.v) for c in row if c.v is not None)
                    if row_text.strip():
                        parts.append(row_text)
    return "\n".join(parts)
