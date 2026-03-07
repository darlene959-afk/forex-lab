# forexlab/strategy_store.py
from datetime import datetime
from io import BytesIO
import docx
from pypdf import PdfReader

def extract_text_pdf(file_bytes: bytes) -> str:
    reader = PdfReader(BytesIO(file_bytes))
    chunks = []
    for page in reader.pages:
        txt = page.extract_text() or ""
        chunks.append(txt)
    return "\n".join(chunks).strip()

def extract_text_docx(file_bytes: bytes) -> str:
    d = docx.Document(BytesIO(file_bytes))
    return "\n".join(p.text for p in d.paragraphs).strip()

def save_strategy_file(conn, name: str, filename: str, file_bytes: bytes) -> int:
    ext = filename.lower().split(".")[-1]
    if ext == "pdf":
        extracted = extract_text_pdf(file_bytes)
        ftype = "pdf"
    elif ext in ("docx",):
        extracted = extract_text_docx(file_bytes)
        ftype = "docx"
    else:
        raise ValueError("Only PDF or DOCX supported")

    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
        """
        INSERT INTO strategy_files(name,filename,file_type,content_bytes,extracted_text,uploaded_at_utc)
        VALUES (?,?,?,?,?,?)
        """,
        (name, filename, ftype, file_bytes, extracted, now_utc)
    )
    conn.commit()
    return cur.lastrowid