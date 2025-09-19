import os, re, json, time, base64, mimetypes, tempfile
from uuid import uuid4
from flask import Flask, request, jsonify, send_from_directory
import requests

# 追加: EML生成用
from email.message import EmailMessage
from email.utils import formatdate
import html as _html

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
# ルーティング
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
    html = "<!doctype html><meta charset='utf-8'><title>ok</title>OK"
    resp = app.response_class(html, mimetype="text/html")
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
# メイン
# ===============================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)


# ============================================================
# excel_api.py の機能を app.py に内包するための追記ブロック
# - 追加エンドポイント: POST /extract_mail
# - 仕様: multipart/form-data で file を1つ渡すと、
#         {"ok":true,"format":"msg|eml","body_text":"...","excel_attachments":[{"filename":"...","cells":"A1\t..."}]}
#         を返す（元の excel_api.py と同じI/F）
# ============================================================

# ---- 依存 import（既存と重複していてもOK） ----
try:
    from openpyxl import load_workbook    # .xlsx/.xlsm
except ImportError as _e:
    load_workbook = None

try:
    import extract_msg                    # .msg
except ImportError as _e:
    extract_msg = None

from io import BytesIO
import re, html as _html_mod, tempfile
from email import policy as _email_policy
from email.parser import BytesParser as _EmailBytesParser
from typing import List, Dict

# ---- ユーティリティ ----
def _excelapi_to_str(v) -> str:
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

def _excelapi_html_to_text(html_s: str) -> str:
    if not html_s:
        return ""
    s = re.sub(r'(?is)<(script|style).*?>.*?</\1>', '', html_s)
    s = re.sub(r'(?is)<br\s*/?>', '\n', s)
    s = re.sub(r'(?is)</p\s*>', '\n', s)
    s = re.sub(r'(?is)<.*?>', '', s)
    s = _html_mod.unescape(s)
    return _excelapi_to_str(s)

def _excelapi_is_excel_filename(name: str) -> bool:
    n = (name or "").lower()
    return n.endswith((".xlsx", ".xlsm", ".xls", ".csv"))

def _excelapi_is_excel_mime(mime: str) -> bool:
    m = (mime or "").lower()
    return (
        m.startswith("application/vnd.openxmlformats-officedocument.spreadsheetml")
        or m == "application/vnd.ms-excel"
        or m == "text/csv"
    )

def _excelapi_looks_like_msg(b: bytes) -> bool:
    # OLE ヘッダ: D0 CF 11 E0 A1 B1 1A E1
    return len(b) >= 8 and b[:8] == b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"

def _excelapi_looks_like_eml(b: bytes) -> bool:
    head = b[:512].decode("utf-8", errors="ignore")
    return (("From:" in head or "Subject:" in head) and "\n\n" in head)

# ---- Excel 抜粋（sparse TSV）: .xlsx/.xlsm を openpyxl で読む。xls/csv は簡易扱い ----
def _excelapi_sparse_from_xlsx_bytes(xlsx_bytes: bytes, max_rows=200, max_cols=50, max_nonempty=2000) -> str:
    if load_workbook is None:
        return "# ERROR: openpyxl is not installed"
    wb = load_workbook(BytesIO(xlsx_bytes), data_only=True, read_only=True)
    ws = wb.active
    lines, count = [], 0
    for row in ws.iter_rows(min_row=1, max_row=max_rows, min_col=1, max_col=max_cols, values_only=False):
        for cell in row:
            v = cell.value
            if v is None:
                continue
            txt = _excelapi_to_str(v)
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

def _excelapi_sparse_from_csv_bytes(csv_bytes: bytes, max_rows=200, max_cols=50, max_nonempty=2000) -> str:
    # 簡易CSV → 擬似セル座標 A1, B1 ... で出す
    import csv
    import io
    f = io.StringIO(csv_bytes.decode("utf-8", errors="ignore"))
    rdr = csv.reader(f)
    def _num_to_col(n: int) -> str:
        s = []
        while n > 0:
            n, rem = divmod(n - 1, 26)
            s.append(chr(65 + rem))
        return "".join(reversed(s))
    lines, cnt = [], 0
    for r_idx, row in enumerate(rdr, start=1):
        if r_idx > max_rows: break
        for c_idx, val in enumerate(row, start=1):
            if c_idx > max_cols: break
            txt = _excelapi_to_str(val)
            if not txt: continue
            lines.append(f"{_num_to_col(c_idx)}{r_idx}\t{txt}")
            cnt += 1
            if cnt >= max_nonempty:
                lines.append("# ...truncated...")
                break
        if cnt >= max_nonempty:
            break
    return "\n".join(lines)

def _excelapi_sparse_from_bytes_auto(filename_lower: str, data: bytes) -> str:
    if filename_lower.endswith((".xlsx", ".xlsm")):
        return _excelapi_sparse_from_xlsx_bytes(data)
    if filename_lower.endswith(".csv"):
        return _excelapi_sparse_from_csv_bytes(data)
    # .xls の厳密処理は省略（依存を増やさないため）。openpyxlで失敗する前提でメッセージ。
    if filename_lower.endswith(".xls"):
        return "# NOTE: .xls is not supported here (consider converting to .xlsx)"
    # extensionなし → xlsxトライ
    try:
        return _excelapi_sparse_from_xlsx_bytes(data)
    except Exception as e:
        return f"# ERROR: excel parse failed: {e}"

# ---- .msg / .eml の解析 ----
def _excelapi_handle_msg_bytes(b: bytes) -> Dict:
    if extract_msg is None:
        return {"ok": False, "error": "extract-msg not installed"}
    with tempfile.NamedTemporaryFile(delete=True, suffix=".msg") as tmp:
        tmp.write(b); tmp.flush()
        msg = extract_msg.Message(tmp.name)

    raw_text = _excelapi_to_str(getattr(msg, "body", "") or "")
    raw_html = getattr(msg, "bodyHTML", "") or ""
    body_text = raw_text or _excelapi_html_to_text(raw_html)

    excel_results: List[Dict] = []
    for att in msg.attachments:
        name = getattr(att, "longFilename", "") or getattr(att, "shortFilename", "") or "attachment"
        data = getattr(att, "data", None)
        if not data:
            continue
        if _excelapi_is_excel_filename(name):
            try:
                cells_text = _excelapi_sparse_from_bytes_auto(name.lower(), data)
            except Exception as e:
                cells_text = f"# ERROR: excel parse failed: {e}"
            excel_results.append({"filename": name, "cells": cells_text})

    return {"ok": True, "format": "msg", "body_text": body_text, "excel_attachments": excel_results}

def _excelapi_handle_eml_bytes(b: bytes) -> Dict:
    msg = _EmailBytesParser(policy=_email_policy.default).parsebytes(b)

    # 本文: text/plain を優先、なければ text/html をテキスト化
    body_text = ""
    if msg.is_multipart():
        # text/plain を探す
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and part.get_content_disposition() in (None, "inline"):
                body_text = _excelapi_to_str(part.get_content())
                if body_text: break
        if not body_text:
            for part in msg.walk():
                if part.get_content_type() == "text/html" and part.get_content_disposition() in (None, "inline"):
                    body_text = _excelapi_html_to_text(part.get_content())
                    if body_text: break
    else:
        ctype = msg.get_content_type()
        if ctype == "text/plain":
            body_text = _excelapi_to_str(msg.get_content())
        elif ctype == "text/html":
            body_text = _excelapi_html_to_text(msg.get_content())

    # 添付: Excel系を抽出
    excel_results: List[Dict] = []
    for part in msg.walk():
        fname = part.get_filename()
        cdisp = part.get_content_disposition()
        ctype = part.get_content_type()
        if cdisp == "attachment" or fname:
            if _excelapi_is_excel_filename(fname or "") or _excelapi_is_excel_mime(ctype):
                data = part.get_payload(decode=True) or b""
                if not data:
                    continue
                try:
                    cells_text = _excelapi_sparse_from_bytes_auto((fname or "").lower(), data)
                except Exception as e:
                    cells_text = f"# ERROR: excel parse failed: {e}"
                excel_results.append({"filename": fname or "attachment.xlsx", "cells": cells_text})

    return {"ok": True, "format": "eml", "body_text": body_text, "excel_attachments": excel_results}

# ---- 既存の Flask アプリに /extract_mail を追加 ----
@app.post("/extract_mail")
def _excelapi_extract_mail():
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
        if filename.endswith(".msg") or _excelapi_looks_like_msg(data):
            payload = _excelapi_handle_msg_bytes(data)
        elif filename.endswith(".eml") or _excelapi_looks_like_eml(data):
            payload = _excelapi_handle_eml_bytes(data)
        else:
            # 拡張子が怪しいときは両方試す（eml→msgの順に）
            try:
                payload = _excelapi_handle_eml_bytes(data)
            except Exception:
                payload = _excelapi_handle_msg_bytes(data)

        # ensure_ascii=False で日本語を素のUTF-8で返す
        return Response(json.dumps(payload, ensure_ascii=False), mimetype="application/json; charset=utf-8")
    except Exception as e:
        return jsonify({"error": f"failed to process mail: {e}"}), 400
