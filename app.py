import os, re, json, time, base64, mimetypes, tempfile
from uuid import uuid4
from flask import Flask, request, jsonify, send_from_directory, Response
import requests

# 追加: EML生成用
from email.message import EmailMessage
from email.utils import formatdate
import html as _html

# ===== excel_api.py 由来の追加 import（仕様変更なしで合体） =====
import html  # excel_api.py で使用
from io import BytesIO
from typing import List, Dict
from openpyxl import load_workbook
import xlrd  # .xls対応
from email import policy
from email.parser import BytesParser
import extract_msg  # .msg対応（excel_api 由来機能で使用）

app = Flask(__name__, static_folder="static", template_folder="templates")

# ===============================
# 設定
# ===============================
TENANT_ID         = os.getenv("AZ_TENANT_ID", "")
CLIENT_ID_ORG     = os.getenv("AZ_CLIENT_ID", "")
CLIENT_SECRET_ORG = os.getenv("AZ_CLIENT_SECRET", "")
TARGET_USER_ID    = os.getenv("TARGET_USER_ID", "")  # /users/{id} or UPN

GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

SMALL_MAX_BYTES    = 250 * 1024 * 1024  # 250MB
CHUNK_SIZE         = 5   * 1024 * 1024  # 5MB per chunk
DEFAULT_TICKET_TTL = 600                # 10 min

# ===============================
# チケット & トークン
# ===============================
TICKETS = {}  # { ticket_id: { type, fileName, mime, data|payload|href, expire, ... } }
TOKENS  = {"access_token": "", "expire": 0}

def save_ticket(meta, ttl=DEFAULT_TICKET_TTL):
    tid = uuid4().hex
    meta["expire"] = time.time() + ttl
    TICKETS[tid] = meta
    return tid

def redeem_ticket(ticket, consume=True):
    if ticket not in TICKETS:
        raise KeyError("ticket_not_found_or_expired")
    meta = TICKETS[ticket]
    if meta.get("expire", 0) < time.time():
        raise KeyError("ticket_expired")
    if consume:
        TICKETS.pop(ticket, None)
    return meta

# ===============================
# Graph ヘルパ
# ===============================
def refresh_if_needed():
    if TOKENS["expire"] > time.time() + 60:
        return
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    r = requests.post(url, data={
        "client_id": CLIENT_ID_ORG,
        "client_secret": CLIENT_SECRET_ORG,
        "scope": GRAPH_SCOPE,
        "grant_type": "client_credentials"
    }, timeout=30)
    r.raise_for_status()
    j = r.json()
    TOKENS["access_token"] = j["access_token"]
    TOKENS["expire"] = time.time() + int(j.get("expires_in", 3600))

def _auth_headers(extra=None):
    refresh_if_needed()
    h = {"Authorization": f"Bearer {TOKENS['access_token']}"}
    if extra:
        h.update(extra)
    return h

def _sanitize_name(name: str) -> str:
    return (re.sub(r'[\\/:*?"<>|]', "_", name or "").strip() or "NewItem")

# ---- アップロード関連
def graph_put_small_to_folder_org(folder_id, name, mime, data):
    safe_name = _sanitize_name(name)
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{folder_id}:/{safe_name}:/content"
    return requests.put(url, headers=_auth_headers({"Content-Type": mime or "application/octet-stream"}),
                        data=data, timeout=300)

def graph_create_upload_session_org(folder_id, name):
    safe_name = _sanitize_name(name)
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{folder_id}:/{safe_name}:/createUploadSession"
    r = requests.post(url, headers=_auth_headers(), json={}, timeout=60)
    r.raise_for_status()
    return r.json()["uploadUrl"]

def graph_put_chunked_to_folder_org(folder_id, name, data):
    upload_url = graph_create_upload_session_org(folder_id, name)
    size = len(data); off = 0; last = None
    while off < size:
        chunk = data[off: off + CHUNK_SIZE]
        start = off; end = off + len(chunk) - 1
        headers = {
            "Content-Length": str(len(chunk)),
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Content-Type": "application/octet-stream",
        }
        last = requests.put(upload_url, headers=headers, data=chunk, timeout=600)
        if last.status_code not in (200, 201, 202):
            break
        off += len(chunk)
    return last

# ---- メタ/フォルダ列挙/パンくず/作成
def graph_get_item_meta(item_id):
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{item_id}"
    r = requests.get(url, headers=_auth_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def graph_list_child_folders(parent_id: str):
    if not parent_id or parent_id == "root":
        url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/root/children?$select=id,name,folder&$top=200"
    else:
        url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{parent_id}/children?$select=id,name,folder&$top=200"
    r = requests.get(url, headers=_auth_headers(), timeout=30)
    r.raise_for_status()
    arr = []
    for it in r.json().get("value", []):
        if isinstance(it.get("folder"), dict):
            arr.append({"id": it.get("id"), "name": it.get("name")})
    return arr

def graph_get_item_parent(item_id: str):
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{item_id}?$select=id,name,parentReference"
    r = requests.get(url, headers=_auth_headers(), timeout=30)
    r.raise_for_status()
    j = r.json()
    parent_id = (j.get("parentReference") or {}).get("id")
    return {"id": j.get("id"), "name": j.get("name"), "parentId": parent_id}

def graph_create_folder(parent_id: str, name: str, conflict_behavior="rename"):
    safe_name = _sanitize_name(name)
    if not parent_id or parent_id == "root":
        url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/root/children"
    else:
        url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{parent_id}/children"
    body = {
        "name": safe_name,
        "folder": {},
        "@microsoft.graph.conflictBehavior": conflict_behavior
    }
    r = requests.post(url, headers=_auth_headers({"Content-Type":"application/json"}), json=body, timeout=30)
    r.raise_for_status()
    return r.json()

def graph_delete_item(item_id: str):
    """
    OneDrive 上の任意アイテム（フォルダ/ファイル）を削除。
    成功時は True を返す。失敗時は requests.HTTPError を送出。
    """
    if not item_id:
        raise ValueError("missing item_id")
    url = f"{GRAPH_BASE}/users/{TARGET_USER_ID}/drive/items/{item_id}"
    r = requests.delete(url, headers=_auth_headers(), timeout=30)
    r.raise_for_status()
    return True

# ===============================
# EML生成（空EML対策の修正ポイント）
# ===============================
def _text_to_html(s: str) -> str:
    if not s:
        return "<p></p>"
    t = (_html.escape(s or "").replace("\r\n", "\n").replace("\r", "\n"))
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

# ===============================
# バイト展開 / .msg抽出 / .eml抽出
# ===============================
def materialize_bytes(meta):
    """
    チケットメタから (file_name, bytes, mime) を返す
    type: "base64" / "text" / "eml" / "url"
    """
    t = (meta.get("type") or "").lower()
    if t == "base64":
        return meta.get("fileName") or "download.bin", base64.b64decode(meta.get("data") or ""), meta.get("mime") or "application/octet-stream"
    elif t == "text":
        return meta.get("fileName") or "note.txt", (meta.get("data") or "").encode("utf-8"), meta.get("mime") or "text/plain"
    elif t == "url":
        u = meta.get("href")
        if not u:
            raise RuntimeError("payload.url missing")
        r = requests.get(u, timeout=60)
        r.raise_for_status()
        return meta.get("fileName") or "download.bin", r.content, meta.get("mime") or "application/octet-stream"
    elif t == "eml":
        # 1) data（base64）があればそれを使う
        data_b64 = meta.get("data")
        if data_b64:
            return meta.get("fileName") or "message.eml", base64.b64decode(data_b64), meta.get("mime") or "message/rfc822"
        # 2) payload から EML を組み立てる
        p = meta.get("payload") or {}
        body_text = p.get("text") or p.get("body") or ""
        body_html = p.get("html")
        if body_html in (None, False, "") and (p.get("htmlFromText") or True):
            body_html = _text_to_html(body_text)
        eml = build_eml_bytes(
            subject=p.get("subject"),
            from_addr=p.get("from"),
            to_addrs=p.get("to") or ["user@example.com"],
            body_text=body_text,
            body_html=body_html,
            date_str=p.get("date"),
        )
        return meta.get("fileName") or "message.eml", eml, meta.get("mime") or "message/rfc822"
    else:
        return meta.get("fileName") or "download.bin", base64.b64decode(meta.get("data") or ""), meta.get("mime") or "application/octet-stream"

def _extract_first_excel_from_msg(msg_bytes: bytes):
    """
    .msgバイナリから最初の Excel/CSV を抽出
    戻り値: (filename, data_bytes, mime) / None
    """
    import extract_msg  # pip install extract-msg
    with tempfile.NamedTemporaryFile(suffix=".msg", delete=True) as tmp:
        tmp.write(msg_bytes); tmp.flush()
        m = extract_msg.Message(tmp.name)
        for att in m.attachments:
            fname = getattr(att, "longFilename", None) or getattr(att, "shortFilename", None) or "attachment"
            lower = fname.lower()
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

# NEW: .eml から最初の Excel/CSV を抽出
def _extract_first_excel_from_eml(eml_bytes: bytes):
    """
    .eml(RFC822) から最初の Excel/CSV 添付を抽出
    戻り値: (filename, data_bytes, mime) / None
    """
    from email import policy
    from email.parser import BytesParser

    msg = BytesParser(policy=policy.default).parsebytes(eml_bytes)

    for part in msg.walk():
        fname = part.get_filename()
        if not fname:
            continue
        lower = fname.lower()
        if lower.endswith((".xlsx", ".xlsm", ".xls", ".csv")):
            data = part.get_payload(decode=True) or b""
            mime = part.get_content_type() or (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if lower.endswith(".xlsx") else
                "application/vnd.ms-excel" if lower.endswith((".xls", ".xlsm")) else
                "text/csv" if lower.endswith(".csv") else
                mimetypes.guess_type(fname)[0] or "application/octet-stream"
            )
            return fname, data, mime
    return None

# ===============================
# OneDrive系 ルーティング
# ===============================
@app.get("/")
def index():
    return jsonify({"ok": True, "service": "onedrive-uploader-org", "version": "2025-09-03"})

@app.get("/front")
def serve_front():
    # 自前ピッカーUIのHTML（templates/onedrive.html）を返す
    return send_from_directory(app.template_folder, "onedrive.html")

@app.get("/picker-redirect.html")
def picker_redirect():
    # 必要に応じて使う軽量ページ（CSP干渉回避）
    html_s = "<!doctype html><meta charset='utf-8'><title>ok</title>OK"
    resp = app.response_class(html_s, mimetype="text/html")
    resp.headers.pop("Content-Security-Policy", None)
    return resp

# --- 子フォルダ一覧
@app.get("/api/drive/folders")
def api_list_folders():
    """
    GET /api/drive/folders?parentId=<id|root>
    戻り:
      { "ok": true,
        "parent": {"id": "...", "name": "...", "parentId": "..."} | null,
        "folders": [ {"id":"...","name":"..."} ... ]
      }
    """
    parent_id = request.args.get("parentId", "root")
    try:
        folders = graph_list_child_folders(parent_id)
        parent = None if parent_id == "root" else graph_get_item_parent(parent_id)
        return jsonify({"ok": True, "parent": parent, "folders": folders})
    except requests.HTTPError as e:
        return jsonify({"error": "graph_http_error", "status": e.response.status_code, "detail": e.response.text}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# --- 新規フォルダ作成（JSON/Form/Query 互換）
@app.post("/api/drive/create-folder")
@app.post("/api/drive/createFolder")
def api_create_folder():
    try:
        # JSON / Form / Query 互換
        j = request.get_json(silent=True) or {}
        parent_id = (
            request.args.get("parentId")
            or request.form.get("parentId")
            or j.get("parentId")
            or "root"
        )
        name = (
            request.args.get("name")
            or request.form.get("name")
            or j.get("name")
        )
        behavior = (
            request.args.get("conflictBehavior")
            or request.form.get("conflictBehavior")
            or j.get("conflictBehavior")
            or "rename"
        )
        if not name:
            return jsonify({"error": "missing name"}), 400
        item = graph_create_folder(parent_id, name, behavior)
        return jsonify({"ok": True, "id": item.get("id"), "name": item.get("name"), "webUrl": item.get("webUrl")})
    except requests.HTTPError as e:
        return jsonify({"error": "graph_http_error", "status": e.response.status_code, "detail": e.response.text}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
# --- フォルダ削除（新規）
@app.post("/api/drive/delete-folder")
@app.delete("/api/drive/delete-folder")
def api_delete_folder():
    """
    JSON/Form/Query 互換:
      id: 削除するフォルダ（またはアイテム）の itemId
    """
    try:
        j = request.get_json(silent=True) or {}
        item_id = (
            request.args.get("id")
            or request.form.get("id")
            or j.get("id")
        )
        if not item_id:
            return jsonify({"error": "missing id"}), 400
        graph_delete_item(item_id)
        return jsonify({"ok": True})
    except requests.HTTPError as e:
        return jsonify({"error": "graph_http_error", "status": e.response.status_code, "detail": e.response.text}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 400    

# --- Tickets: create
@app.post("/tickets/create")
def tickets_create():
    """
    JSONでチケット作成
      {"type":"base64","fileName":"Report.eml","mime":"message/rfc822","data":"<b64>","ttlSec":600}
      {"type":"text","fileName":"note.txt","mime":"text/plain","data":"hello"}
      {"type":"url","fileName":"download.bin","href":"https://..."}
      {"type":"eml","fileName":"Report.eml","payload":{...},"data":"<b64>"}
    """
    try:
        j = request.get_json(force=True, silent=False)
        mtype = (j.get("type") or "text").lower()
        meta = {
            "type": mtype,
            "fileName": j.get("fileName") or ("llm.eml" if mtype == "eml" else "download.bin"),
            "mime": j.get("mime"),
        }
        if mtype in ("text", "base64"):
            meta["data"] = j.get("data") or ""
        elif mtype == "url":
            meta["href"] = j.get("href") or ""
        elif mtype == "eml":
            meta["payload"] = j.get("payload") or {}
            if j.get("data"):
                meta["data"] = j.get("data")
            if not meta.get("mime"):
                meta["mime"] = "message/rfc822"
        else:
            meta["data"] = j.get("data") or ""
        ttl = int(j.get("ttlSec") or DEFAULT_TICKET_TTL)
        tid = save_ticket(meta, ttl=ttl)
        return jsonify({"ticket": tid})
    except Exception as e:
        return jsonify({ "error": str(e)}), 400

# --- Tickets: create-multipart（ファイル＋メタ／msg・eml→xlsx抽出オプション）
@app.post("/tickets/create-multipart")
def tickets_create_multipart():
    """
    form-data:
      file      : (任意) アップロードファイル（.msg / .eml 等）
      metadata  : (任意) JSON文字列
        例のオプション:
          ttlSec: number
          fileName, mime
          type: "base64"|"text"|"eml"|...
          data: (typeに応じて)
          extractXlsx: true    # file が .msg/.eml のとき最初のExcelを抽出
          xlsxFileName: "Attachment.xlsx"
    戻り:
      {"ok":true,"tickets":{"file":<ticket>|None,"metadata":<ticket>|None,"xlsxFromMsg":<ticket>|None}}
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
                if meta_json.get("data"):
                    meta["data"] = meta_json.get("data")
                if not meta.get("mime"):
                    meta["mime"] = "message/rfc822"
            else:
                meta["data"] = meta_json.get("data") or ""
            made_meta_ticket = save_ticket(meta, ttl=ttl)

        # CHANGED: .msg/.eml どちらでも Excel/CSV 抽出
        if f and raw is not None and meta_json.get("extractXlsx"):
            up_lower = (getattr(f, "filename", "") or "").lower()
            hit = None
            if up_lower.endswith(".msg"):
                hit = _extract_first_excel_from_msg(raw)
            elif up_lower.endswith(".eml"):
                hit = _extract_first_excel_from_eml(raw)
            else:
                # 拡張子不明なら両方試す
                hit = _extract_first_excel_from_msg(raw) or _extract_first_excel_from_eml(raw)

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

# --- Tickets: peek
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

# ---- チケットユーティリティ: .msg/.eml → xlsx チケット化（非消費 & 直ファイル対応）
@app.post("/api/msg-to-xlsx-ticket")
def api_msg_to_xlsx_ticket():
    """
    入力（どれでも可）:
      - form-data / query:
          ticket=<.msg/.eml のチケット> / msg_ticket= / fileName= / ttlSec=
          file=<.msg/.eml を直接アップロード>   ← 新規対応
      - JSON:
          {"ticket":"<MSG_OR_EML_TICKET>","fileName":"Attachment.xlsx","ttlSec":600}
          {"tickets":{"file":"<MSG_TICKET>"}}
          {"arg1":"{\"ticket\":\"...\",\"fileName\":\"...\"}"}
          {"data":"<base64-raw>","fileName":"mail.eml","ttlSec":600}  ← 新規対応
    出力: {"ok": true, "ticket": "<XLSX_TICKET>", "fileName": "…"}
    """
    try:
        # 1) まずフォーム/クエリ系
        msg_tid = (
            request.form.get("ticket")
            or request.form.get("msg_ticket")
            or request.args.get("ticket")
            or request.args.get("msg_ticket")
        )
        name_ovr = request.form.get("fileName") or request.args.get("fileName")
        ttl_override = request.form.get("ttlSec") or request.args.get("ttlSec")

        # 2) JSON も見て補完
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

        ttl = int(ttl_override or DEFAULT_TICKET_TTL)

        # 3) 入力ソースの決定（優先順：form の file > ticket > JSON data）
        raw = None
        src_name = None

        f = request.files.get("file") or request.files.get("msg") or request.files.get("eml")
        if f:
            raw = f.read()
            src_name = getattr(f, "filename", None) or "upload.bin"
        elif msg_tid:
            meta = redeem_ticket(msg_tid, consume=False)  # 非消費
            src_name, raw, _ = materialize_bytes(meta)
        elif j.get("data"):
            raw = base64.b64decode(j["data"])
            src_name = j.get("fileName") or "upload.bin"

        if not raw:
            return jsonify({"error": "missing input (ticket or file or data)"}), 400

        # 4) .msg/.eml を判定して抽出（拡張子で分けつつ、必要なら両方試す）
        lower = (src_name or "").lower()
        hit = None
        if lower.endswith(".msg"):
            hit = _extract_first_excel_from_msg(raw)
        elif lower.endswith(".eml"):
            hit = _extract_first_excel_from_eml(raw)
        else:
            hit = _extract_first_excel_from_msg(raw) or _extract_first_excel_from_eml(raw)

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

# ---- OneDrive: アイテムメタ取得
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

# ---- アップロード（組織 OneDrive）
@app.post("/api/upload")
def api_upload():
    """
    form-data: ticket, folderId, (fileName)
    チケット実体を組織OneDriveのフォルダに保存
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
            file_name = (name_ovr or "").strip() or file_name

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

# ---- .msg 直アップ（最初のExcel/CSVだけ抽出して保存）
@app.post("/api/upload-msg-xlsx")
def api_upload_msg_xlsx():
    """
    form-data: ticket(必須: .msgのチケット), folderId(必須), (fileName)
    .msg から最初のExcel/CSVを抽出して保存
    """
    try:
        ticket   = request.form.get("ticket")
        folderId = request.form.get("folderId")
        name_ovr = request.form.get("fileName")
        if not ticket or not folderId:
            return jsonify({"error": "missing parameters"}), 400

        meta = redeem_ticket(ticket, consume=True)
        _, msg_bytes, _ = materialize_bytes(meta)
        hit = _extract_first_excel_from_msg(msg_bytes)
        if not hit:
            return jsonify({"error": "no_excel_attachment_found"}), 404

        att_name, att_bytes, att_mime = hit
        save_name = (name_ovr or att_name or "attachment.xlsx").strip()
        if not save_name.lower().endswith((".xlsx", ".xlsm", ".xls", ".csv")):
            save_name += ".xlsx"

        if len(att_bytes) <= SMALL_MAX_BYTES:
            r = graph_put_small_to_folder_org(folderId, save_name, att_mime, att_bytes)
        else:
            r = graph_put_chunked_to_folder_org(folderId, save_name, att_bytes)

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


# ===============================
# ======== excel_api 追加分: /extract /extract_mail （仕様そのまま） ========
# ===============================

# excel_api 上限（元のまま）
MAX_ROWS = 200
MAX_COLS = 50
MAX_NONEMPTY = 2000  # 非空セル最大数

def to_str(v) -> str:
    if v is None:
        return ""
    s = str(v)
    return (
        s.replace("_x000D_", " ")
         .replace("\t", " ")
         .replace("\r\n", " ")
         .replace("\n", " ")
         .replace("\r", " ")
         .strip()
    )

def _html_to_text(html_s: str) -> str:
    if not html_s:
        return ""
    s = re.sub(r'(?is)<(script|style).*?>.*?</\1>', '', html_s)
    s = re.sub(r'(?is)<br\s*/?>', '\n', s)
    s = re.sub(r'(?is)</p\s*>', '\n', s)
    s = re.sub(r'(?is)<.*?>', '', s)
    s = html.unescape(s)
    return to_str(s)

def _num_to_col(n: int) -> str:
    s = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s.append(chr(65 + rem))
    return "".join(reversed(s))

def _excel_sparse_from_xlsx_bytes(xlsx_bytes: bytes,
                                  sheet_req: str | None = None,
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
            if v is None:
                continue
            txt = to_str(v)
            if not txt:
                continue
            lines.append(f"{cell.coordinate}\t{txt}")
            count += 1
            if count >= max_nonempty:
                lines.append("# ...truncated...")
                break
        if count >= max_nonempty:
            break
    return "\n".join(lines)

def _excel_sparse_from_xls_bytes(xls_bytes: bytes,
                                 max_rows=MAX_ROWS, max_cols=MAX_COLS, max_nonempty=MAX_NONEMPTY) -> str:
    with tempfile.NamedTemporaryFile(delete=True, suffix=".xls") as tmp:
        tmp.write(xls_bytes)
        tmp.flush()
        book = xlrd.open_workbook(tmp.name)
        sheet = book.sheet_by_index(0)

        lines, count = [], 0
        max_r = min(sheet.nrows, max_rows)
        max_c = min(sheet.ncols, max_cols)
        for r in range(max_r):
            for c in range(max_c):
                v = sheet.cell_value(r, c)
                txt = to_str(v)
                if not txt:
                    continue
                coord = f"{_num_to_col(c+1)}{r+1}"
                lines.append(f"{coord}\t{txt}")
                count += 1
                if count >= max_nonempty:
                    lines.append("# ...truncated...")
                    break
            if count >= max_nonempty:
                break
    return "\n".join(lines)

def _excel_sparse_from_bytes(data: bytes,
                             filename: str | None = None,
                             sheet_req: str | None = None,
                             max_rows=MAX_ROWS, max_cols=MAX_COLS, max_nonempty=MAX_NONEMPTY) -> str:
    name = (filename or "").lower()
    if name.endswith(".xls"):
        return _excel_sparse_from_xls_bytes(data, max_rows, max_cols, max_nonempty)
    return _excel_sparse_from_xlsx_bytes(data, sheet_req, max_rows, max_cols, max_nonempty)

def _is_excel_filename(name: str) -> bool:
    n = (name or "").lower()
    return n.endswith((".xlsx", ".xlsm", ".xls"))

def _is_excel_mime(mime: str) -> bool:
    m = (mime or "").lower()
    return (
        m.startswith("application/vnd.openxmlformats-officedocument.spreadsheetml")
        or m == "application/vnd.ms-excel"
    )

@app.route("/extract", methods=["POST"])
def extract():
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "file is required (multipart/form-data)"}), 400

    bom_on = (request.form.get("bom", "true").lower() != "false")
    inline_on = (request.form.get("inline", "true").lower() != "false")
    sheet_req = request.form.get("sheet")

    data = f.read()
    if not data:
        return jsonify({"error": "empty file"}), 400

    try:
        payload = _excel_sparse_from_bytes(data, filename=f.filename, sheet_req=sheet_req)
    except Exception as e:
        return jsonify({"error": f"failed to read workbook: {e}"}), 400

    if bom_on:
        payload = "\ufeff" + payload

    headers = {}
    if not inline_on:
        headers["Content-Disposition"] = 'attachment; filename="extract.tsv"'
    return Response(payload, mimetype="text/plain; charset=utf-8", headers=headers)

@app.route("/extract_mail", methods=["POST"])
def extract_mail():
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
                try:
                    payload = _handle_msg_bytes(data)
                except Exception as e:
                    return jsonify({"error": f"unsupported or unreadable mail file: {e}"}), 400
        return Response(json.dumps(payload, ensure_ascii=False),
                        mimetype="application/json; charset=utf-8")
    except Exception as e:
        return jsonify({"error": f"failed to process mail: {e}"}), 400

def _looks_like_msg(b: bytes) -> bool:
    return len(b) >= 8 and b[:8] == b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"

def _looks_like_eml(b: bytes) -> bool:
    head = b[:512].decode("utf-8", errors="ignore")
    return ("From:" in head or "Subject:" in head) and "\n\n" in head

def _handle_msg_bytes(b: bytes) -> Dict:
    with tempfile.NamedTemporaryFile(delete=True, suffix=".msg") as tmp:
        tmp.write(b)
        tmp.flush()
        msg = extract_msg.Message(tmp.name)

    raw_text = to_str(getattr(msg, "body", "") or "")
    raw_html = getattr(msg, "bodyHTML", "") or ""
    body_text = raw_text or _html_to_text(raw_html)

    excel_results: List[Dict] = []
    for att in msg.attachments:
        name = getattr(att, "longFilename", "") or getattr(att, "shortFilename", "") or "attachment"
        data = getattr(att, "data", None)
        if not data:
            continue
        if _is_excel_filename(name):
            try:
                cells_text = _excel_sparse_from_bytes(data, filename=name)
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
                if body_text:
                    break
        if not body_text:
            for part in msg.walk():
                if part.get_content_type() == "text/html" and part.get_content_disposition() in (None, "inline"):
                    body_text = _html_to_text(part.get_content())
                    if body_text:
                        break
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
            if _is_excel_filename(fname) or _is_excel_mime(ctype):
                data = part.get_payload(decode=True) or b""
                if not data:
                    continue
                try:
                    cells_text = _excel_sparse_from_bytes(data, filename=fname or "")
                except Exception as e:
                    cells_text = f"# ERROR: excel parse failed: {e}"
                excel_results.append({"filename": fname or "attachment.xlsx", "cells": cells_text})

    return {"ok": True, "format": "eml", "body_text": body_text, "excel_attachments": excel_results}

# ---- GitHub Raw などのURLから直接ダウンロードして保存（第4のファイル）
@app.post("/api/upload-from-url")
def api_upload_from_url():
    """
    JSON:
      {
        "url": "https://raw.githubusercontent.com/<user>/<repo>/<ref>/<path/to/file>",
        "folderId": "<OneDrive itemId>",
        "fileName": "任意の保存名（省略可。省略時はURL末尾名を使用）"
      }
    成功: {"ok": true, "id": "...", "webUrl": "...", "name": "..."}
    """
    try:
        j = request.get_json(force=True, silent=False)
        url = (j.get("url") or j.get("href") or "").strip()
        folder_id = (j.get("folderId") or "").strip()
        name_ovr = (j.get("fileName") or "").strip()

        if not url or not folder_id:
            return jsonify({"error": "missing url or folderId"}), 400

        # 1) URL 叩いてバイト取得
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.content
        mime = (r.headers.get("Content-Type") or
                mimetypes.guess_type(url)[0] or
                "application/octet-stream")

        # 2) 保存名の決定（指定がなければURL末尾）
        if not name_ovr:
            from urllib.parse import urlparse, unquote
            path = unquote(urlparse(url).path or "")
            base = (path.rsplit("/", 1)[-1] or "download.bin")
            name_ovr = base

        # 3) OneDrive へアップロード（小/大で分岐）
        if len(data) <= SMALL_MAX_BYTES:
            up = graph_put_small_to_folder_org(folder_id, name_ovr, mime, data)
        else:
            up = graph_put_chunked_to_folder_org(folder_id, name_ovr, data)

        if up.status_code in (200, 201):
            jj = up.json()
            return jsonify({
                "ok": True,
                "id": jj.get("id"),
                "webUrl": jj.get("webUrl"),
                "name": jj.get("name")
            })
        else:
            return jsonify({
                "error": "graph_upload_failed",
                "status": up.status_code,
                "detail": up.text[:4000]
            }), up.status_code

    except requests.HTTPError as e:
        return jsonify({
            "error": "download_http_error",
            "status": getattr(e.response, "status_code", None),
            "detail": getattr(e.response, "text", "")[:4000]
        }), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ===============================
# メイン
# ===============================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
