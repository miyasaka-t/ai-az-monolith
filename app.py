# app.py (monolith) — One service for front+back
# - Keeps: /extract_mail, /tickets/create-multipart, /api/msg-to-xlsx-ticket, /tickets/create
# - Serves: /front -> templates/onedrive.html
# - Graph (org) upload & simple folder picker
# Dependencies: Flask, requests, extract-msg, openpyxl, xlrd

import os, re, json, time, base64, mimetypes, tempfile, html as _html
from io import BytesIO
from typing import List, Dict
from uuid import uuid4
from datetime import datetime
from urllib.parse import quote

import requests
from flask import Flask, request, jsonify, Response, send_from_directory, render_template
from email.message import EmailMessage
from email.utils import formatdate

# ===== Excel readers
from openpyxl import load_workbook
import xlrd  # .xls support

# ===== Mail parsers
import extract_msg               # .msg
from email import policy
from email.parser import BytesParser

app = Flask(__name__, static_folder="static", template_folder="templates")

# --------------------------
# Config (Org Graph / App permission)
# --------------------------
TENANT_ID         = os.getenv("AZ_TENANT_ID", "")
CLIENT_ID_ORG     = os.getenv("AZ_CLIENT_ID", "")
CLIENT_SECRET_ORG = os.getenv("AZ_CLIENT_SECRET", "")
TARGET_USER_ID    = os.getenv("TARGET_USER_ID", "")  # UPN or GUID id

GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

SMALL_MAX_BYTES    = 250 * 1024 * 1024
CHUNK_SIZE         = 5 * 1024 * 1024
DEFAULT_TICKET_TTL = 600

TOKENS  = {"access_token": "", "exp": 0}

def _now() -> float: return time.time()

def refresh_if_needed():
    if TOKENS["exp"] > time.time() + 60:
        return
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    r = requests.post(url, data={
        "client_id": CLIENT_ID_ORG,
        "client_secret": CLIENT_SECRET_ORG,
        "scope": GRAPH_SCOPE,
        "grant_type": "client_credentials",
    }, timeout=30)
    r.raise_for_status()
    j = r.json()
    TOKENS["access_token"] = j["access_token"]
    TOKENS["exp"] = time.time() + int(j.get("expires_in", 3600))

def _auth_hdr():
    refresh_if_needed()
    return {"Authorization": f"Bearer {TOKENS['access_token']}"}

def _sanitize_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name)

# --------------------------
# Ticket store (memory)
# --------------------------
TICKETS: dict = {}

def save_ticket(meta: dict, ttl=DEFAULT_TICKET_TTL) -> str:
    tid = uuid4().hex
    meta["expire"] = time.time() + ttl
    TICKETS[tid] = meta
    return tid

def redeem_ticket(ticket: str, consume: bool = True) -> dict:
    if ticket not in TICKETS:
        raise KeyError("ticket_not_found_or_expired")
    meta = TICKETS[ticket]
    if meta.get("expire", 0) < time.time():
        TICKETS.pop(ticket, None)
        raise KeyError("ticket_expired")
    if consume:
        TICKETS.pop(ticket, None)
    return meta

# --------------------------
# Text -> HTML helper + EML builder
# --------------------------
def _text_to_html(s: str) -> str:
    if not s:
        return "<p></p>"
    t = (_html.escape(s or "")
         .replace("\r\n", "\n").replace("\r", "\n"))
    paras = t.split("\n\n")
    return "".join(f"<p>{p.replace('\n','<br>')}</p>" for p in paras) or "<p></p>"

def build_eml_bytes(subject, from_addr, to_addrs, body_text="", body_html=None, date_str=None) -> bytes:
    msg = EmailMessage()
    msg["Subject"] = subject or "LLM Output"
    msg["From"] = from_addr or "noreply@example.com"
    msg["To"] = ", ".join(to_addrs) if isinstance(to_addrs, list) else (to_addrs or "")
    msg["Date"] = date_str or formatdate(localtime=True)
    msg.set_content(body_text or "", subtype="plain", charset="utf-8")
    if body_html is None or body_html is False:
        body_html = _text_to_html(body_text or "")
    if body_html:
        msg.add_alternative(body_html, subtype="html", charset="utf-8")
    return msg.as_bytes()

# --------------------------
# Materialize bytes from ticket meta
# --------------------------
def materialize_bytes(meta: dict) -> tuple[str, bytes, str]:
    t = (meta.get("type") or "").lower()
    if t == "text":
        return meta.get("fileName") or "note.txt", (meta.get("data") or "").encode("utf-8"), meta.get("mime") or "text/plain"
    if t == "base64":
        return meta.get("fileName") or "download.bin", base64.b64decode(meta.get("data") or ""), meta.get("mime") or "application/octet-stream"
    if t == "url":
        u = meta.get("href")
        if not u:
            raise RuntimeError("payload.url missing")
        r = requests.get(u, timeout=120)
        r.raise_for_status()
        content = r.content
        mime = meta.get("mime") or r.headers.get("Content-Type") or "application/octet-stream"
        return meta.get("fileName") or "download.bin", content, mime
    if t == "eml":
        data_b64 = meta.get("data")
        if data_b64:
            return meta.get("fileName") or "message.eml", base64.b64decode(data_b64), meta.get("mime") or "message/rfc822"
        # build from payload
        p = meta.get("payload") or {}
        body_text = p.get("text", "") or ""
        html_from_text = bool(p.get("htmlFromText"))
        body_html = p.get("html")
        if html_from_text or body_html in (None, False, ""):
            body_html = _text_to_html(body_text)
        eml = build_eml_bytes(
            subject=p.get("subject"),
            from_addr=p.get("from"),
            to_addrs=p.get("to") or ["user@example.com"],
            body_text=body_text,
            body_html=body_html,
            date_str=p.get("date")
        )
        return meta.get("fileName") or "message.eml", eml, meta.get("mime") or "message/rfc822"
    # default
    return meta.get("fileName") or "download.bin", base64.b64decode(meta.get("data") or ""), meta.get("mime") or "application/octet-stream"

# --------------------------
# Graph helpers (Org)
# --------------------------
def graph_put_small_to_folder_org(folder_id, name, mime, data):
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{folder_id}:/{_sanitize_name(name)}:/content"
    return requests.put(url, headers={**_auth_hdr(), "Content-Type": mime or "application/octet-stream"}, data=data, timeout=300)

def graph_create_upload_session_org(folder_id, name):
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{folder_id}:/{_sanitize_name(name)}:/createUploadSession"
    r = requests.post(url, headers=_auth_hdr(), json={}, timeout=60)
    r.raise_for_status()
    return r.json()["uploadUrl"]

def graph_put_chunked_to_folder_org(folder_id, name, data):
    upload_url = graph_create_upload_session_org(folder_id, name)
    size = len(data); off = 0; last = None
    while off < size:
        chunk = data[off: off + CHUNK_SIZE]
        start = off; end = off + len(chunk) - 1
        headers = {"Content-Length": str(len(chunk)),
                   "Content-Range": f"bytes {start}-{end}/{size}",
                   "Content-Type": "application/octet-stream"}
        last = requests.put(upload_url, headers=headers, data=chunk, timeout=600)
        if last.status_code not in (200, 201, 202):
            break
        off += len(chunk)
    return last

def graph_get_item_meta(item_id):
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{item_id}"
    r = requests.get(url, headers=_auth_hdr(), timeout=30)
    r.raise_for_status()
    return r.json()

def graph_list_child_folders(parent_id: str):
    if not parent_id or parent_id == "root":
        url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/root/children?$select=id,name,folder&$top=200"
    else:
        url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{parent_id}/children?$select=id,name,folder&$top=200"
    r = requests.get(url, headers=_auth_hdr(), timeout=30)
    r.raise_for_status()
    arr = []
    for it in r.json().get("value", []):
        if isinstance(it.get("folder"), dict):
            arr.append({"id": it.get("id"), "name": it.get("name")})
    return arr

def graph_get_item_parent(item_id: str):
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{item_id}?$select=id,name,parentReference"
    r = requests.get(url, headers=_auth_hdr(), timeout=30)
    r.raise_for_status()
    j = r.json()
    parent_id = (j.get("parentReference") or {}).get("id")
    return {"id": j.get("id"), "name": j.get("name"), "parentId": parent_id}

def graph_create_folder(parent_id: str, folder_name: str):
    parent = "root" if not parent_id or parent_id == "root" else parent_id
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{parent}:/children"
    payload = {"name": _sanitize_name(folder_name), "folder": {}, "@microsoft.graph.conflictBehavior": "rename"}
    r = requests.post(url, headers={**_auth_hdr(), "Content-Type": "application/json"}, json=payload, timeout=30)
    r.raise_for_status()
    j = r.json()
    return {"id": j.get("id"), "name": j.get("name")}

# --------------------------
# Excel sparse extraction helpers (xlsx/xls)
# --------------------------
MAX_ROWS = 200
MAX_COLS = 50
MAX_NONEMPTY = 2000

def to_str(v) -> str:
    if v is None:
        return ""
    s = str(v)
    return (s.replace("_x000D_", " ")
             .replace("\t", " ")
             .replace("\r\n", " ")
             .replace("\n", " ")
             .replace("\r", " ")
             .strip())

def _html_to_text(html_s: str) -> str:
    if not html_s:
        return ""
    s = re.sub(r'(?is)<(script|style).*?>.*?</\1>', '', html_s)
    s = re.sub(r'(?is)<br\s*/?>', '\n', s)
    s = re.sub(r'(?is)</p\s*>', '\n', s)
    s = re.sub(r'(?is)<.*?>', '', s)
    s = _html.unescape(s)
    return to_str(s)

def _num_to_col(n: int) -> str:
    s = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s.append(chr(65 + rem))
    return "".join(reversed(s))

def _excel_sparse_from_xlsx_bytes(xlsx_bytes: bytes, sheet_req: str | None = None,
                                  max_rows=MAX_ROWS, max_cols=MAX_COLS, max_nonempty=MAX_NONEMPTY) -> str:
    wb = load_workbook(BytesIO(xlsx_bytes), data_only=True, read_only=True)
    ws = None
    if sheet_req:
        try:
            idx = int(sheet_req)
            names = wb.sheetnames
            if 0 <= idx < len(names):
                ws = wb[names[idx]]
            elif 1 <= idx <= len(names):
                ws = wb[names[idx - 1]]
        except ValueError:
            if sheet_req in wb.sheetnames:
                ws = wb[sheet_req]
    ws = ws or wb.active
    lines, count = [], 0
    for row in ws.iter_rows(min_row=1, max_row=max_rows, min_col=1, max_col=max_cols, values_only=False):
        for cell in row:
            v = cell.value
            if v is None: continue
            txt = to_str(v)
            if not txt: continue
            lines.append(f"{cell.coordinate}\t{txt}")
            count += 1
            if count >= max_nonempty:
                lines.append("# ...truncated...")
                break
        if count >= max_nonempty:
            break
    return "\n".join(lines)

def _excel_sparse_from_xls_bytes(xls_bytes: bytes, max_rows=MAX_ROWS, max_cols=MAX_COLS, max_nonempty=MAX_NONEMPTY) -> str:
    with tempfile.NamedTemporaryFile(suffix=".xls", delete=True) as tmp:
        tmp.write(xls_bytes); tmp.flush()
        book = xlrd.open_workbook(tmp.name, on_demand=True)
        sheet = book.sheet_by_index(0)
        lines, cnt = [], 0
        rows = min(sheet.nrows, max_rows)
        cols = min(sheet.ncols, max_cols)
        for r in range(rows):
            for c in range(cols):
                v = sheet.cell_value(r, c)
                if v in ("", None): continue
                txt = to_str(v)
                if not txt: continue
                lines.append(f"{_num_to_col(c+1)}{r+1}\t{txt}")
                cnt += 1
                if cnt >= max_nonempty:
                    lines.append("# ...truncated...")
                    break
            if cnt >= max_nonempty:
                break
    return "\n".join(lines)

def _excel_sparse_from_bytes_auto(name_lower: str, data: bytes) -> str:
    if name_lower.endswith((".xlsx", ".xlsm")):
        return _excel_sparse_from_xlsx_bytes(data)
    if name_lower.endswith(".xls"):
        return _excel_sparse_from_xls_bytes(data)
    # fallback try xlsx
    return _excel_sparse_from_xlsx_bytes(data)

# --------------------------
# .msg / .eml utilities for /extract_mail
# --------------------------
def _is_excel_filename(name: str) -> bool:
    n = (name or "").lower()
    return n.endswith((".xlsx", ".xlsm", ".xls", ".csv"))

def _is_excel_mime(mime: str) -> bool:
    return (mime or "").lower().startswith("application/vnd.openxmlformats-officedocument.spreadsheetml") or mime == "application/vnd.ms-excel" or mime == "text/csv"

def _looks_like_msg(b: bytes) -> bool:
    return len(b) >= 8 and b[:8] == b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"

def _looks_like_eml(b: bytes) -> bool:
    head = b[:512].decode("utf-8", errors="ignore")
    return ("From:" in head or "Subject:" in head) and "\n\n" in head

def _handle_msg_bytes(b: bytes) -> Dict:
    with tempfile.NamedTemporaryFile(delete=True, suffix=".msg") as tmp:
        tmp.write(b); tmp.flush()
        msg = extract_msg.Message(tmp.name)
    raw_text = to_str(getattr(msg, "body", "") or "")
    raw_html = getattr(msg, "bodyHTML", "") or ""
    body_text = raw_text or _html_to_text(raw_html)
    excel_results: List[Dict] = []
    for att in msg.attachments:
        name = getattr(att, "longFilename", "") or getattr(att, "shortFilename", "") or "attachment"
        data = getattr(att, "data", None)
        if not data: continue
        if _is_excel_filename(name):
            try:
                cells_text = _excel_sparse_from_bytes_auto(name.lower(), data)
            except Exception as e:
                cells_text = f"# ERROR: excel parse failed: {e}"
            excel_results.append({"filename": name, "cells": cells_text})
    return {"ok": True, "format": "msg", "body_text": body_text, "excel_attachments": excel_results}

def _handle_eml_bytes(b: bytes) -> Dict:
    msg = BytesParser(policy=policy.default).parsebytes(b)
    body_text = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and part.get_content_disposition() in (None, "inline"):
                body_text = to_str(part.get_content())
                if body_text: break
        if not body_text:
            for part in msg.walk():
                if part.get_content_type() == "text/html" and part.get_content_disposition() in (None, "inline"):
                    body_text = _html_to_text(part.get_content())
                    if body_text: break
    else:
        ctype = msg.get_content_type()
        if ctype == "text/plain":
            body_text = to_str(msg.get_content())
        elif ctype == "text/html":
            body_text = _html_to_text(msg.get_content())

    excel_results: List[Dict] = []
    for part in msg.walk():
        fname = part.get_filename()
        cdisp = part.get_content_disposition()
        ctype = part.get_content_type()
        if cdisp == "attachment" or fname:
            if _is_excel_filename(fname or "") or _is_excel_mime(ctype):
                data = part.get_payload(decode=True) or b""
                if not data: continue
                try:
                    cells_text = _excel_sparse_from_bytes_auto((fname or "").lower(), data)
                except Exception as e:
                    cells_text = f"# ERROR: excel parse failed: {e}"
                excel_results.append({"filename": fname or "attachment.xlsx", "cells": cells_text})
    return {"ok": True, "format": "eml", "body_text": body_text, "excel_attachments": excel_results}

# --------------------------
# Routes
# --------------------------
@app.get("/")
def root():
    return jsonify({"ok": True, "service": "ai-az monolith", "version": "2025-09-19"})

# Front (template)
@app.get("/front")
def front():
    return render_template("onedrive.html")

@app.get("/picker-redirect.html")
def picker_redirect():
    html = "<!doctype html><meta charset='utf-8'><title>close</title>OK"
    resp = app.response_class(html, mimetype="text/html")
    resp.headers.pop("Content-Security-Policy", None)
    return resp

# ----- Drive (org) helpers for picker -----
@app.get("/api/drive/folders")
def api_list_folders():
    parent_id = request.args.get("parentId", "root")
    try:
        folders = graph_list_child_folders(parent_id)
        parent = None if parent_id == "root" else graph_get_item_parent(parent_id)
        return jsonify({"ok": True, "parent": parent, "folders": folders})
    except requests.HTTPError as e:
        return jsonify({"error": "graph_http_error", "status": e.response.status_code, "detail": e.response.text}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.post("/api/drive/create-folder")
def api_create_folder():
    try:
        j = request.get_json(silent=True) or {}
        parent_id = j.get("parentId") or request.form.get("parentId") or "root"
        name = j.get("name") or request.form.get("name")
        if not name:
            return jsonify({"error": "missing name"}), 400
        info = graph_create_folder(parent_id, name)
        return jsonify({"ok": True, **info})
    except requests.HTTPError as e:
        return jsonify({"error": "graph_http_error", "status": e.response.status_code, "detail": e.response.text}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.get("/api/drive/item")
def api_drive_item():
    item_id = request.args.get("id", "")
    if not item_id:
        return jsonify({"error": "missing id"}), 400
    try:
        meta = graph_get_item_meta(item_id)
        return jsonify({"id": meta.get("id"), "name": meta.get("name"), "webUrl": meta.get("webUrl")})
    except requests.HTTPError as e:
        return jsonify({"error": "graph_http_error", "status": e.response.status_code, "detail": e.response.text}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ----- Tickets -----
@app.post("/tickets/create")
def tickets_create():
    """
    Keeps existing schema:
    {
      "type": "eml"|"text"|"base64"|"url",
      "fileName": "...",
      "mime": "...",
      "payload": {...},  # for eml
      "data": "<base64>",  # for base64/eml(optional)
      "ttlSec": 600,
      "once": true (ignored; always single-use by redeem)
    }
    """
    try:
        j = request.get_json(force=True, silent=False)
        mtype = (j.get("type") or "text").lower()
        meta = {
            "type": mtype,
            "fileName": j.get("fileName") or ("message.eml" if mtype == "eml" else "download.bin"),
            "mime": j.get("mime"),
        }
        if mtype in ("text", "base64"):
            meta["data"] = j.get("data") or ""
        elif mtype == "url":
            meta["href"] = j.get("href") or ""
        elif mtype == "eml":
            meta["payload"] = j.get("payload") or {}
            if j.get("data"): meta["data"] = j.get("data")
            if not meta.get("mime"): meta["mime"] = "message/rfc822"
        else:
            meta["data"] = j.get("data") or ""
        ttl = int(j.get("ttlSec") or DEFAULT_TICKET_TTL)
        tid = save_ticket(meta, ttl=ttl)
        return jsonify({"ticket": tid, "expiresIn": ttl})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.get("/tickets/peek")
def tickets_peek():
    t = request.args.get("ticket", "")
    try:
        meta = redeem_ticket(t, consume=False)
        return jsonify({"fileName": meta.get("fileName"), "type": meta.get("type"), "mime": meta.get("mime")})
    except KeyError:
        return jsonify({"error": "ticket_not_found_or_expired"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.post("/tickets/create-multipart")
def tickets_create_multipart():
    """
    form-data:
      - file: (optional) any file (.msg etc)
      - metadata: (optional) JSON string
        options: type/fileName/mime/data/payload/ttlSec/extractXlsx/xlsxFileName
    returns: {"ok":true,"tickets":{"file":..., "metadata":..., "xlsxFromMsg":...}}
    """
    try:
        meta_text = request.form.get("metadata", "") or ""
        if not meta_text and "metadata" in request.files:
            try:
                meta_text = request.files["metadata"].read().decode("utf-8", errors="ignore")
            except Exception:
                meta_text = ""
        if not meta_text:
            meta_text = request.form.get("meta", "") or request.form.get("metadata_json", "") or ""
        meta_json = {}
        if meta_text.strip():
            try:
                meta_json = json.loads(meta_text)
            except Exception:
                meta_json = {}
        ttl = int(meta_json.get("ttlSec") or DEFAULT_TICKET_TTL)

        made_file_ticket = None
        made_meta_ticket = None
        made_xlsx_ticket = None

        f = request.files.get("file")
        raw = None
        if f:
            raw = f.read()
            up_name = getattr(f, "filename", None) or "upload.bin"
            file_name_for_file = (meta_json.get("fileName") or up_name or "upload.bin").strip() or "upload.bin"
            file_mime_for_file = (
                meta_json.get("mime")
                or f.mimetype
                or mimetypes.guess_type(file_name_for_file)[0]
                or "application/octet-stream"
            )
            made_file_ticket = save_ticket({
                "type": "base64",
                "fileName": file_name_for_file,
                "mime": file_mime_for_file,
                "data": base64.b64encode(raw).decode("ascii"),
            }, ttl=ttl)

        if meta_json:
            mtype = (meta_json.get("type") or "text").lower()
            meta = {
                "type": mtype,
                "fileName": meta_json.get("fileName") or "download.bin",
                "mime": meta_json.get("mime"),
            }
            if mtype in ("text", "base64"):
                meta["data"] = meta_json.get("data") or ""
            elif mtype == "url":
                meta["href"] = meta_json.get("href") or ""
            elif mtype == "eml":
                meta["payload"] = meta_json.get("payload") or {}
                if meta_json.get("data"): meta["data"] = meta_json.get("data")
                if not meta.get("mime"): meta["mime"] = "message/rfc822"
            else:
                meta["data"] = meta_json.get("data") or ""
            made_meta_ticket = save_ticket(meta, ttl=ttl)

        if f and raw is not None and meta_json.get("extractXlsx"):
            # If the uploaded file is .msg, extract first excel/csv as another ticket
            hit = _extract_first_excel_from_msg(raw)
            if hit:
                att_name, att_bytes, att_mime = hit
                save_name = (meta_json.get("xlsxFileName") or att_name or "attachment.xlsx").strip()
                if not save_name.lower().endswith((".xlsx", ".xlsm", ".xls", ".csv")):
                    save_name += ".xlsx"
                made_xlsx_ticket = save_ticket({
                    "type": "base64",
                    "fileName": save_name,
                    "mime": att_mime or "application/octet-stream",
                    "data": base64.b64encode(att_bytes).decode("ascii")
                }, ttl=ttl)

        return jsonify({"ok": True, "tickets": {"file": made_file_ticket, "metadata": made_meta_ticket, "xlsxFromMsg": made_xlsx_ticket}})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# helper: first excel from .msg
def _extract_first_excel_from_msg(msg_bytes: bytes):
    with tempfile.NamedTemporaryFile(suffix=".msg", delete=True) as tmp:
        tmp.write(msg_bytes); tmp.flush()
        m = extract_msg.Message(tmp.name)
        for att in m.attachments:
            fname = getattr(att, "longFilename", None) or getattr(att, "shortFilename", None) or "attachment"
            lower = (fname or "").lower()
            if lower.endswith((".xlsx", ".xlsm", ".xls", ".csv")):
                data = att.data
                mime = (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if lower.endswith(".xlsx") else
                    "application/vnd.ms-excel" if lower.endswith((".xls", ".xlsm")) else
                    "text/csv" if lower.endswith(".csv") else
                    mimetypes.guess_type(fname)[0] or "application/octet-stream"
                )
                return fname, data, mime
    return None

@app.post("/api/msg-to-xlsx-ticket")
def api_msg_to_xlsx_ticket():
    """
    Keeps the schema: {"ticket":"<MSG_TICKET>","fileName":"Attachment.xlsx"}
    """
    try:
        msg_tid = (
            request.form.get("ticket")
            or request.form.get("msg_ticket")
            or request.args.get("ticket")
            or request.args.get("msg_ticket")
        )
        name_ovr = request.form.get("fileName")
        ttl_override = request.form.get("ttlSec")

        j = request.get_json(silent=True) or {}
        if not msg_tid:
            msg_tid = j.get("ticket") or j.get("msg_ticket")
            if not msg_tid and isinstance(j.get("tickets"), dict):
                msg_tid = j["tickets"].get("file") or j["tickets"].get("msg") or j["tickets"].get("ticket_msg")
            if not msg_tid and isinstance(j.get("arg1"), str):
                try:
                    j2 = json.loads(j["arg1"])
                    msg_tid = j2.get("ticket") or j2.get("msg_ticket")
                    if not name_ovr: name_ovr = j2.get("fileName")
                    if not ttl_override: ttl_override = j2.get("ttlSec")
                except Exception:
                    pass
        if not name_ovr:
            name_ovr = j.get("fileName")
        if not ttl_override:
            ttl_override = j.get("ttlSec")

        if not msg_tid:
            return jsonify({"error": "missing msg ticket"}), 400
        ttl = int(ttl_override or DEFAULT_TICKET_TTL)

        meta = redeem_ticket(msg_tid, consume=False)
        _, msg_bytes, _ = materialize_bytes(meta)
        hit = _extract_first_excel_from_msg(msg_bytes)
        if not hit:
            return jsonify({"error": "no_excel_attachment_found"}), 404

        att_name, att_bytes, att_mime = hit
        save_name = (name_ovr or att_name or "attachment.xlsx").strip()
        if not save_name.lower().endswith((".xlsx", ".xlsm", ".xls", ".csv")):
            save_name += ".xlsx"

        x_tid = save_ticket({
            "type": "base64",
            "fileName": save_name,
            "mime": att_mime or "application/octet-stream",
            "data": base64.b64encode(att_bytes).decode("ascii")
        }, ttl=ttl)

        return jsonify({"ok": True, "ticket": x_tid, "fileName": save_name})
    except KeyError:
        return jsonify({"error": "ticket_not_found_or_expired"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ----- Upload to OneDrive (org) -----
@app.post("/api/upload")
def api_upload():
    """
    form-data: ticket, folderId, (fileName)
    """
    try:
        ticket   = request.form.get("ticket")
        folderId = request.form.get("folderId")
        name_ovr = request.form.get("fileName")
        if not ticket or not folderId:
            return jsonify({"error": "missing parameters"}), 400
        meta = redeem_ticket(ticket, consume=True)
        file_name, data_bytes, mime = materialize_bytes(meta)
        if name_ovr:
            file_name = name_ovr.strip() or file_name
        if len(data_bytes) <= SMALL_MAX_BYTES:
            r = graph_put_small_to_folder_org(folderId, file_name, mime, data_bytes)
        else:
            r = graph_put_chunked_to_folder_org(folderId, file_name, data_bytes)
        if r.status_code in (200, 201):
            j = r.json()
            return jsonify({"ok": True, "id": j.get("id"), "webUrl": j.get("webUrl"), "name": j.get("name")})
        else:
            return jsonify({"error": "graph_upload_failed", "status": r.status_code, "detail": r.text}), r.status_code
    except KeyError:
        return jsonify({"error": "ticket_not_found_or_expired"}), 404
    except requests.HTTPError as e:
        return jsonify({"error": "graph_http_error", "status": e.response.status_code, "detail": e.response.text}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ----- excel_api.py compatibility -----
@app.post("/extract_mail")
def extract_mail():
    """
    Multipart:
      - file: required (.msg or .eml)
    Returns:
      {
        "ok": true,
        "format": "msg"|"eml",
        "body_text": "...",
        "excel_attachments": [ { "filename": "...", "cells": "A1\\t..." } ]
      }
    """
    up = request.files.get("file")
    if not up:
        return jsonify({"error": "file is required (multipart/form-data)"}), 400
    filename = (up.filename or "").lower()
    data = up.read()
    try:
        if filename.endswith(".msg") or _looks_like_msg(data):
            payload = _handle_msg_bytes(data)
        elif filename.endswith(".eml") or _looks_like_eml(data):
            payload = _handle_eml_bytes(data)
        else:
            try:
                payload = _handle_eml_bytes(data)
            except Exception:
                payload = _handle_msg_bytes(data)
        return Response(json.dumps(payload, ensure_ascii=False), mimetype="application/json; charset=utf-8")
    except Exception as e:
        return jsonify({"error": f"failed to process mail: {e}"}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
