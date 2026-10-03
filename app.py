#!/usr/bin/env python3
"""Agent Hub - Flask + Flask-SocketIO orchestrator that routes messages and
files between HTTP clients and AI agents stuck behind NAT (persistent
outbound socket). Run:  AGENT_AUTH_TOKEN=... LOG_SECRET_TOKEN=... python3 app.py
AGENT_AUTH_TOKEN is the sole hub credential - holders get full access, so any
agent may task any other agent. Optional: NGROK_AUTHTOKEN=... opens a public
HTTPS tunnel from inside this app (NGROK_DOMAIN=... pins a reserved free
domain so the URL survives restarts); without a token the hub warns and serves
localhost only. Agent-facing docs: GET /llms.txt (markdown) and GET /api
(machine manifest) - both unauthenticated. Errors come back as JSON with an
actionable hint unless the caller asks for HTML."""

import hashlib
import html as html_mod
import json
import os
import re
import secrets as pysecrets
import threading
import time
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, Response, abort, jsonify, request, send_file
from flask_socketio import ConnectionRefusedError, SocketIO
from werkzeug.exceptions import HTTPException
from werkzeug.serving import BaseWSGIServer
from werkzeug.utils import secure_filename

# ----------------------------------------------------------------- config
BASE_DIR = Path(__file__).resolve().parent
FILE_STORE = Path(os.environ.get("HUB_FILE_STORE") or BASE_DIR / "file_store")
LOG_FILE = Path(os.environ.get("HUB_LOG_FILE") or BASE_DIR / "logs.html")
FILE_STORE.mkdir(parents=True, exist_ok=True)
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
INDEX_FILE = FILE_STORE / "index.json"
FAVICON_FILE = BASE_DIR / "favicon.ico"
FAVICON_ROUTE = "/favicon.ico"

AGENT_TOKEN = os.environ.get("AGENT_AUTH_TOKEN", "")     # sole hub token (6-50 chars)
LOG_SECRET = os.environ.get("LOG_SECRET_TOKEN", "")      # secret path segment for /logs/<token>
HUB_PORT = int(os.environ.get("HUB_PORT", "5000"))
HUB_BIND = os.environ.get("HUB_BIND", "127.0.0.1")  # local only; publish behind nginx or ngrok
ACK_TIMEOUT = float(os.environ.get("ACK_TIMEOUT", "10"))
NGROK_AUTHTOKEN = os.environ.get("NGROK_AUTHTOKEN", "")  # optional: public tunnel, else localhost only
NGROK_DOMAIN = os.environ.get("NGROK_DOMAIN", "")        # optional: reserved ngrok domain (stable URL)
HUB_DEBUG = os.environ.get("HUB_DEBUG", "") == "1"       # 500 responses include exception detail

HUB_VERSION = "1.4.0"
FEATURES = ["llms_txt", "api_manifest", "json_errors", "method_405", "inbox_peek",
            "agents_detail", "upload_sha256", "autojson_name", "file_index",
            "events_json", "ngrok_domain", "result_lookup", "events_mine",
            "client_source", "dedupe_uploads", "superseded_notice", "single_result_log_rows",
            "standby_takeover_chain", "result_inbox_status", "sid_scope_notes",
            "unique_agent_ids"]
STARTED_AT = time.time()

ALLOWED_EXT = (".json", ".txt", ".html", ".htm", ".tar.gz", ".tgz")
MAX_UPLOAD = 25 * 1024 * 1024
AGENT_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,40}$")

if not (6 <= len(AGENT_TOKEN) <= 50):
    raise SystemExit("[agent-hub] FATAL: AGENT_AUTH_TOKEN env var must be a 6-50 char string.")
if not (4 <= len(LOG_SECRET) <= 64) or not re.fullmatch(r"[A-Za-z0-9_\-]+", LOG_SECRET):
    raise SystemExit("[agent-hub] FATAL: LOG_SECRET_TOKEN must be 4-64 URL-safe chars.")

NS = "/agents"

app = Flask(__name__)
app.config["SECRET_KEY"] = pysecrets.token_hex(16)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD + 64 * 1024  # headroom for multipart framing
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

agents = {}                # agent_id -> sid
sid_to_agent = {}          # sid -> agent_id
agent_since = {}           # agent_id -> iso connect time
agent_superseded = {}      # agent_id -> iso of last socket take-over
standby = defaultdict(dict)   # agent_id -> {sid: iso} sockets that lost a take-over but are still open
id_rejects = OrderedDict()    # agent_id -> {at, holder, reason} last duplicate-id connect refusals
reg_lock = threading.Lock()
client_inboxes = defaultdict(lambda: deque(maxlen=200))   # agent_id -> agent->client msgs
RESULTS = OrderedDict()    # msg_id -> {"agent": ..., "results": [...]} last 500 tasks
file_meta = {}             # file_id -> {name, path, type, size, sha256, by, created, created_iso}
sha_index = {}             # sha256 -> newest file_id (advisory duplicate detection only)
pending = {}               # msg_id -> {"event": Event, "replies": []}
meta_lock = threading.Lock()

TUNNEL_URL = ""            # set by open_tunnel() when the ngrok edge comes up

# ----------------------------------------------------------------- logging
LOG_ROWS = deque(maxlen=3000)      # pre-rendered HTML rows (human log page)
LOG_EVENTS = deque(maxlen=3000)    # structured dicts (GET /logs/<token>/events.json)
log_lock = threading.Lock()

LOG_CSS = """
:root{--bg:#f5f5f3;--fg:#1c1c1e;--card:#ffffff;--line:#e3e3df;--muted:#6b6b6f}
@media(prefers-color-scheme:dark){:root{--bg:#111214;--fg:#e7e7ea;--card:#1a1c1f;--line:#2a2d31;--muted:#8b8f96}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,"Courier New",monospace;font-size:13px}
header{padding:14px 18px;background:var(--card);border-bottom:1px solid var(--line);display:flex;gap:12px;flex-wrap:wrap;align-items:center;position:sticky;top:0}
header h1{font-size:15px;margin:0}
header .sub{color:var(--muted);font-size:12px}
#log{padding:8px 14px}
.row{display:flex;flex-wrap:wrap;gap:10px;align-items:baseline;padding:6px 8px;border-bottom:1px solid var(--line);border-radius:6px}
.row:hover{background:var(--card)}
.ts{color:var(--muted);white-space:nowrap}
.badge{padding:1px 9px;border-radius:999px;font-size:11px;font-weight:700;color:#fff;letter-spacing:.4px}
.b-CONNECTED{background:#15803d}.b-DISCONNECTED{background:#b91c1c}.b-MSG_SENT{background:#1d4ed8}
.b-MSG_RCVD{background:#7c3aed}.b-AUTH_FAIL{background:#c2410c}.b-FILE_SENT{background:#0e7490}
.b-FILE_RCVD{background:#0f766e}.b-MSG_FAIL{background:#52525b}.b-SERVER{background:#334155}
.b-ID_REJECTED{background:#a16207}
.aid{font-weight:700}.dir{color:var(--muted);font-size:12px}.pl{flex:1;min-width:220px;word-break:break-word;color:var(--fg)}
"""


LOG_JS = """
(function(){
  var K='hub-log-gap', el=document.documentElement;
  function bottom(){return Math.max(0,el.scrollHeight-window.innerHeight)}
  function gap(){
    try{var g=parseFloat(sessionStorage.getItem(K));return isFinite(g)&&g>0?g:0}catch(e){return 0}
  }
  window.scrollTo(0,Math.max(0,bottom()-gap()));
  window.addEventListener('pagehide',function(){
    try{sessionStorage.setItem(K,String(Math.max(0,bottom()-window.scrollY)))}catch(e){}
  });
})();
"""


def _render_log() -> str:
    rows = "".join(LOG_ROWS)
    return ("<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<meta http-equiv='refresh' content='5'>"
            "<title>Agent Hub - Event Log</title>"
            f"<link rel='icon' href='{FAVICON_ROUTE}'>"
            f"<style>{LOG_CSS}</style></head><body>"
            "<header><h1>Agent Hub - Event Log</h1>"
            "<span class='sub'>auto-refresh 5s &middot; auto-scroll &middot; append-only &middot; "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span></header>"
            f"<div id='log'>{rows}</div>"
            f"<script>{LOG_JS}</script></body></html>")


def log_event(event: str, agent_id: str, direction: str, payload: str = "") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = (payload or "")[:160].replace("\n", " ")
    row = (f"<div class='row'><span class='ts'>{ts}</span>"
           f"<span class='badge b-{event}'>{event}</span>"
           f"<span class='aid'>{html_mod.escape(agent_id or '-')}</span>"
           f"<span class='dir'>{html_mod.escape(direction)}</span>"
           f"<span class='pl'>{html_mod.escape(summary)}</span></div>")
    with log_lock:
        LOG_ROWS.append(row)
        LOG_EVENTS.append({"ts": ts, "event": event, "agent": agent_id or "-",
                           "dir": direction, "payload": summary})
        LOG_FILE.write_text(_render_log(), encoding="utf-8")


def init_log_file() -> None:
    LOG_FILE.write_text(_render_log(), encoding="utf-8")


def load_file_index() -> None:
    """Rehydrate file_meta from the sidecar, then adopt any objects on disk the
    index never saw (uploads made by pre-v1.1 hubs, or while the index was down).
    Only runs at startup, so sha recomputation costs nothing per request."""
    try:
        with INDEX_FILE.open(encoding="utf-8") as f:
            stored = json.load(f)
        if isinstance(stored, dict):
            with meta_lock:
                for fid, meta in stored.items():
                    if isinstance(meta, dict) and (FILE_STORE / f"{fid}__{meta.get('name', '')}").exists():
                        file_meta[fid] = meta
                        if meta.get("sha256"):
                            sha_index[meta["sha256"]] = fid
    except FileNotFoundError:
        pass
    except Exception as exc:  # noqa: BLE001 - a broken index must never stop the hub
        print(f"[agent-hub] WARN: could not load {INDEX_FILE.name}: {exc}")

    adopted = 0
    try:
        for path in FILE_STORE.iterdir():
            if not path.is_file() or "__" not in path.name:
                continue
            fid, _, fname = path.name.partition("__")
            if fid in file_meta:
                continue
            try:
                data = path.read_bytes()
            except OSError:
                continue
            sha = hashlib.sha256(data).hexdigest()
            created_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            meta = {"name": fname, "type": "application/octet-stream", "size": len(data),
                    "sha256": sha, "by": "unknown (rehydrated from disk)",
                    "created": created_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "created_iso": created_dt.isoformat(timespec="seconds"),
                    "created_epoch": int(path.stat().st_mtime),
                    "note": "uploader and content-type unknown; recovered at startup"}
            with meta_lock:
                file_meta[fid] = meta
                sha_index.setdefault(sha, fid)
            adopted += 1
    except OSError as exc:
        print(f"[agent-hub] WARN: file store scan failed: {exc}")
    if file_meta:
        with meta_lock:
            persist_file_index()
        print(f"[agent-hub] file index restored: {len(file_meta)} object(s), "
              f"{adopted} adopted from disk")


def persist_file_index() -> None:
    """Best effort, called under meta_lock after mutations. A disk hiccup must not
    fail the upload that already succeeded."""
    try:
        INDEX_FILE.write_text(json.dumps(file_meta, indent=1), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        print(f"[agent-hub] WARN: file index persist failed: {exc}")


def eprint_summary(data) -> str:
    if isinstance(data, dict):
        text = data.get("text") or json.dumps(data, default=str)
    else:
        text = str(data)
    return text


# ----------------------------------------------------------------- auth helpers
def presented_token() -> str:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return (request.headers.get("X-Auth-Token", "").strip()
            or request.headers.get("X-Agent-Token", "").strip())


def authorized() -> bool:
    return bool(AGENT_TOKEN) and pysecrets.compare_digest(presented_token(), AGENT_TOKEN)


def actor_label() -> str:
    aid = request.headers.get("X-Agent-Id", "").strip()
    if authorized() and AGENT_ID_RE.fullmatch(aid or ""):
        return f"agent:{aid}"
    return "client"


# ----------------------------------------------------------------- error contract
def wants_html() -> bool:
    return "text/html" in request.headers.get("Accept", "")


def _err(status: int, error: str, hint: str = "", **fields):
    body = {"error": error, "hint": hint, "docs": "/llms.txt", "api": "/api"}
    body.update(fields)
    return jsonify(body), status


# ----------------------------------------------------------------- API contract
# Single source of truth: /api manifest, /llms.txt markdown and the onboarding
# endpoint table are all generated from these structures.
API_ENDPOINTS = [
    {"method": "GET", "path": "/", "auth": "none", "summary": "HTML onboarding page (also served as the 401 body to browsers)."},
    {"method": "GET", "path": "/health", "auth": "none",
     "summary": "Liveness + capability discovery.", "returns": {"keys": ["status", "agents_connected", "version", "features", "uptime_seconds", "docs", "api", "public_url"]},
     "example": "curl -s $HUB/health"},
    {"method": "GET", "path": "/api", "auth": "none", "summary": "Machine-readable manifest of this whole table plus the socket contract, footguns and features."},
    {"method": "GET", "path": "/llms.txt", "auth": "none", "summary": "Plain-markdown API guide for LLM agents (text/markdown)."},
    {"method": "GET", "path": "/agents", "auth": "token",
     "summary": "Registry of connected agents.",
     "returns": {"keys": ["agents (id->sid, stable shape)", "count", "agent_ids", "detail (id->{sid, connected_at, last_superseded_at, standby_sockets})", "standby (id->{sid: since})", "standby_note"]},
     "example": "curl -s $HUB/agents -H \"Authorization: Bearer $T\" -H \"ngrok-skip-browser-warning: true\""},
    {"method": "GET", "path": "/agent-id/{agent_id}", "auth": "token",
     "summary": "Pre-flight the v1.4 uniqueness rule: is this agent_id free, who holds it, when was it last refused.",
     "returns": {"keys": ["agent_id", "available", "taken_by_sid", "connected_at", "standby_sockets", "last_rejection", "note"]},
     "errors": [400, 401],
     "example": "curl -s $HUB/agent-id/scout -H \"Authorization: Bearer $T\" -H \"ngrok-skip-browser-warning: true\""},
    {"method": "POST", "path": "/agent/{agent_id}/message", "auth": "token",
     "summary": "Route a task to one agent; returns its first reply.",
     "body": {"text": "string"}, "query": {"wait": "reply budget seconds 0-60, default ACK_TIMEOUT"},
     "returns": {"keys": ["status", "msg_id", "agent_id", "reply"]},
     "note": "status 'replied' = first result received - with mock_agent that is the instant ACK, not completion; the real answer lands on GET /agent/{id}/inbox.",
     "errors": [400, 401, 404, 405],
     "example": "curl -s -X POST $HUB/agent/scout/message -H \"Authorization: Bearer $T\" -H \"X-Agent-Id: builder\" -H \"ngrok-skip-browser-warning: true\" -H \"Content-Type: application/json\" -d '{\"text\":\"scan the dataset\"}'"},
    {"method": "GET", "path": "/agent/{agent_id}/inbox", "auth": "token",
     "summary": "Unsolicited agent->client messages. DEFAULT DRAINS AND CLEARS the queue.",
     "query": {"peek": "1/true/yes = read without clearing"},
     "returns": {"keys": ["agent_id", "messages", "count", "drained", "queue_max", "agent_online"]},
     "errors": [401]},
    {"method": "GET", "path": "/result/{msg_id}", "auth": "token",
     "summary": "Everything the hub recorded for one task msg_id - the correlation hook for two-phase replies (last 500 tasks).",
     "returns": {"keys": ["msg_id", "agent", "results", "count", "status (first_result|done|answered_via_inbox)", "answered_via_inbox", "updated", "note"]},
     "note": "results is the ordered list of every `result` the agent emitted for this msg_id, including answers that arrived after the POST already returned. Only the last 500 tasks (and only since the current hub process) are retained.",
     "errors": [401, 404],
     "example": "curl -s $HUB/result/c68e84affe12 -H \"Authorization: Bearer $T\" -H \"ngrok-skip-browser-warning: true\""},
    {"method": "POST", "path": "/relay", "auth": "token",
     "summary": "Deliver text to a connected agent as peer_msg (operators and agents alike).",
     "body": {"to": "agent_id", "text": "string"}, "errors": [400, 401, 404]},
    {"method": "POST", "path": "/file", "auth": "token",
     "summary": "Upload a file (<=25 MB): .json .txt .html .htm .tar.gz .tgz, ASCII names only.",
     "body": "multipart -F file=@report.json, or raw bytes + X-Filename header; a raw application/json body with no X-Filename is stored as body.json",
     "headers": {"X-Target-Agent": "optional; pushes file_ready so the agent auto-pulls it"},
     "query": {"dedupe": "1/true/yes = if these exact bytes are already stored, reuse that file_id (HTTP 200, nothing written) instead of minting a new one"},
     "returns": {"keys": ["status (stored|existing)", "file_id", "name", "size", "sha256", "delivered", "target_error", "duplicate_of", "deduped", "bytes_stored", "download_url"]},
     "errors": [400, 401, 413, 415]},
    {"method": "GET", "path": "/files", "auth": "token", "summary": "Stored file metadata table {file_id: {...}}; survives restarts via file_store/index.json."},
    {"method": "GET", "path": "/file/{file_id}", "auth": "token", "summary": "Download stored bytes (as attachment).", "errors": [401, 404]},
    {"method": "GET", "path": "/events/mine", "auth": "token",
     "summary": "Structured event rows involving YOU (agent, source or target) - no log secret needed. Send X-Agent-Id.",
     "query": {"limit": "1-500, default 100"},
     "returns": {"keys": ["events", "count", "total_matching", "caller", "note"]},
     "errors": [400, 401],
     "example": "curl -s \"$HUB/events/mine?limit=20\" -H \"Authorization: Bearer $T\" -H \"X-Agent-Id: scout\" -H \"ngrok-skip-browser-warning: true\""},
    {"method": "GET", "path": "/client.py", "auth": "token",
     "summary": "The reference agent client (mock_agent.py) as plain Python text - read it, save it, run it.",
     "example": "curl -s $HUB/client.py -H \"Authorization: Bearer $T\" -H \"ngrok-skip-browser-warning: true\" -o mock_agent.py"},
    {"method": "GET", "path": "/logs/{LOG_SECRET_TOKEN}", "auth": "secret path", "summary": "Auto-refreshing (5s) + auto-scrolling HTML event log. Any other token => 404."},
    {"method": "GET", "path": "/logs/{LOG_SECRET_TOKEN}/events.json", "auth": "secret path",
     "summary": "Structured event log for agents.", "query": {"limit": "1-3000, default 50"}},
    {"method": "GET", "path": "/favicon.ico", "auth": "none", "summary": "Hub icon."},
]

SOCKET_CONTRACT = {
    "namespace": "/agents",
    "connect": {
        "auth": {"token": "<AGENT_AUTH_TOKEN>", "agent_id": "<1-40 chars, [A-Za-z0-9_-]>",
                  "force_takeover": "optional true/1/yes - displace the live socket holding this id"},
        "fallback": "query string ?token=...&agent_id=...(&force=1) also accepted",
        "wrong_token": "socket refused, hub logs AUTH_FAIL",
        "id_taken": ("since v1.4 a second live socket on the same agent_id is REFUSED with a "
                     "reason string (connect_error) and logged as ID_REJECTED - ids are unique; "
                     "use another id, or force_takeover to displace deliberately"),
    },
    "hub_to_agent": {
        "task": {"msg_id": "str - echo it back in result", "from": "agent:<id> or client", "text": "str"},
        "peer_msg": {"from": "str", "text": "str"},
        "file_ready": {"file_id": "str", "url": "/file/<id>", "note": "plus full file meta (name, size, sha256, by, created)"},
        "superseded": {"agent_id": "str", "reason": "str", "at": "iso", "sid": "your hub-registry sid", "sid_note": "str", "note": "sent to the OLD socket when another process registers the same agent_id; your own task/peer_msg traffic moves to the new socket from then on"},
        "reactivated": {"agent_id": "str", "reason": "str", "sid": "your hub-registry sid (compare with sio.get_sid(namespace='/agents'), NOT sio.sid)", "at": "iso", "note": "sent to a standby socket when the process that superseded it disconnects - routing of that agent_id comes back to you automatically (v1.3)"},
    },
    "agent_to_hub": {
        "result": {"msg_id": "str (echo of task msg_id)", "text": "str", "note": "every result is recorded under msg_id - read them back with GET /result/<msg_id>"},
        "agent_to_client": {"text": "str - queues on GET /agent/<your-id>/inbox", "msg_id": "optional str - echoed into the inbox entry, and since v1.3 flips GET /result/<msg_id> to status answered_via_inbox"},
        "agent_to_agent": {"to": "agent_id", "text": "str", "reply": "ack {status:relayed,to} or {error}"},
    },
    "note": "receiving task/peer_msg/file_ready REQUIRES a live socket; pure-HTTP callers can only read registries, move files, relay and drain inboxes.",
}

FOOTGUNS = [
    "GET /agent/<id>/inbox DRAINS AND CLEARS its queue (maxlen 200) by default. Poll with ?peek=true; drain only when you mean to consume.",
    "POST /agent/<id>/message returns the FIRST result the agent emits. mock_agent auto-ACKs instantly, so status 'replied' usually means 'received', not 'done'. Poll GET /result/<msg_id> (every result recorded against that msg_id) or read the inbox for the real answer.",
    "agent_id is unique since v1.4: a second socket connecting with a live id is REFUSED at connect (connect_error says who holds it, log row ID_REJECTED) - no more silent routing theft. Check GET /agent-id/<id> before you connect, give each process its own id (e.g. suffix the pid), and use auth force_takeover:true only when you mean to kick the current holder. A forced take-over still sends the loser `superseded`, keeps it as a standby, and hands routing back with `reactivated` if the winner dies (v1.3 chain).",
    "agent_id is still not authenticated as an identity: X-Agent-Id only labels HTTP traffic, so the `from` on a task is whatever the caller typed. Uniqueness stops collisions, not impersonation.",
    "Pick ONE stable X-Agent-Id per caller and keep sending it (an operator should look like `client` or a fixed `operator` id on every row). Because the header is forgeable and optional, drifting labels - `operator`, `qoder-operator`, `selftest` - make mesh attribution unreadable for the agents on the other end; the hub cannot fix that for you.",
    "The `from` on a task or peer_msg is only the label the caller put in X-Agent-Id - it is not evidence of who posted it (an agent can POST /agent/<id>/message with X-Agent-Id of anyone, including the target itself). Never make routing or trust decisions off that string.",
    "Hub sid fields (`superseded.sid`, `reactivated.sid`, /agents `agents[id]`) are /agents-namespace sids. python-socketio clients expose the transport sid as `sio.sid`, which will NEVER match - self-check with `sio.get_sid(namespace='/agents')`.",
    "One token = full access. Anyone holding AGENT_AUTH_TOKEN can task any agent, drain any inbox and move any file; the hub cannot tell operators from agents.",
    "ngrok free tier: send 'ngrok-skip-browser-warning: true' on every request or you get an HTML interstitial instead of JSON.",
    "ngrok free URLs change on every hub restart unless NGROK_DOMAIN pins a reserved domain.",
    "There is no file delete endpoint - the store grows forever; uploads persist across restarts via file_store/index.json (v1.1+). Re-uploading identical bytes mints a NEW id by default (response says duplicate_of); POST /file?dedupe=1 reuses the existing id instead.",
]

ONBOARDING_TEMPLATE = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>Agent Hub - Orchestrator</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="/favicon.ico">
<style>
:root{--bg:#f5f5f3;--fg:#1c1c1e;--card:#fff;--line:#e3e3df;--muted:#6b6b6f;--acc:#1d4ed8}
@media(prefers-color-scheme:dark){:root{--bg:#111214;--fg:#e7e7ea;--card:#1a1c1f;--line:#2a2d31;--muted:#8b8f96;--acc:#60a5fa}}
body{margin:0;background:var(--bg);color:var(--fg);font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:14px;line-height:1.55}
main{max-width:860px;margin:0 auto;padding:28px 18px 60px}
h1{font-size:20px;margin:0 0 4px}
h2{font-size:15px;margin:26px 0 8px;border-bottom:1px solid var(--line);padding-bottom:6px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px;margin:10px 0}
code,pre{background:rgba(127,127,127,.12);border-radius:6px;padding:1px 5px;font-size:12.5px}
pre{padding:10px 12px;overflow:auto}
.k{color:var(--acc);font-weight:700}
.mut{color:var(--muted);font-size:12.5px}
table{width:100%;border-collapse:collapse;font-size:12.5px}
td,th{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);vertical-align:top}
.pill{display:inline-block;background:var(--acc);color:#fff;border-radius:999px;padding:1px 9px;font-size:12px}
</style></head><body><main>
<h1>Agent Hub <span class="mut">/ NAT orchestrator for AI agents</span> <span class="pill">v__VERSION__</span></h1>
<p>The hub keeps <span class="k">persistent outbound Socket.IO connections</span> open for agents
behind NAT; clients POST tasks by <code>agent_id</code> and the hub routes replies and files back.
No agent needs an inbound port.</p>
<div class="card"><span class="k">Machine-readable docs:</span>
<a href="/llms.txt">/llms.txt</a> (markdown guide - start here if you are an agent) &middot;
<a href="/api">/api</a> (JSON endpoint manifest + socket contract).</div>

<h2>Quick start</h2>
<div class="card"><pre># 0. one token guards EVERYTHING (env at hub startup; export AGENT_AUTH_TOKEN=$T below)
# 1. an agent joins (outbound only, keeps a socket open):
python3 mock_agent.py --server $HUB --agent-id scout --token "$AGENT_AUTH_TOKEN"

# 2. anyone tasks it (operator or another agent):
curl -s -X POST $HUB/agent/scout/message \\
  -H "Authorization: Bearer $AGENT_AUTH_TOKEN" \\
  -H "X-Agent-Id: builder" -H "ngrok-skip-browser-warning: true" \\
  -H "Content-Type: application/json" -d '{"text": "scan the dataset"}'

# 3. move a file (X-Target-Agent pushes a file_ready notify so it auto-pulls):
curl -s -X POST $HUB/file -H "Authorization: Bearer $AGENT_AUTH_TOKEN" \\
  -H "X-Agent-Id: builder" -H "ngrok-skip-browser-warning: true" \\
  -H "X-Target-Agent: scout" -F file=@report.json

# check: who is connected / hub version + capabilities:
curl -s $HUB/agents -H "Authorization: Bearer $AGENT_AUTH_TOKEN" -H "ngrok-skip-browser-warning: true"
curl -s $HUB/health</pre>
<p class="mut" style="margin:6px 0 0">Running the hub itself: <code>AGENT_AUTH_TOKEN=&lt;6-50 chars&gt;
LOG_SECRET_TOKEN=&lt;4-64 url-safe&gt; [NGROK_AUTHTOKEN=... [NGROK_DOMAIN=...]] python3 app.py</code> -
without an ngrok token it stays on localhost:5000 and warns.</p></div>

<h2>Required headers</h2>
<table>
<tr><th>Who</th><th>Header(s)</th><th>Used on</th></tr>
<tr><td>Any caller</td><td><code>Authorization: Bearer &lt;AGENT_AUTH_TOKEN&gt;</code> (or <code>X-Auth-Token</code> / <code>X-Agent-Token</code> - all accepted)</td><td>every <code>/agent*</code>, <code>/file*</code>, <code>/relay</code>, <code>/agents</code></td></tr>
<tr><td>Agents, to be labelled</td><td><code>X-Agent-Id: &lt;your id&gt;</code></td><td>any - labels messages and log rows; it does NOT authenticate</td></tr>
<tr><td>Everyone through ngrok free tier</td><td><code>ngrok-skip-browser-warning: true</code></td><td>every request - otherwise ngrok returns an interstitial HTML page</td></tr>
</table>

<h2>Endpoints</h2>
<table>
<tr><th>Method / Path</th><th>Auth</th><th>Purpose</th></tr>
<!--ENDPOINTS-->
</table>

<h2>Errors</h2>
<div class="card">Every error is JSON <code>{"error", "hint", "docs", "api", ...}</code> with a
fix suggestion, unless the caller sends <code>Accept: text/html</code>. Codes:
<code>401</code> missing/invalid token &middot; <code>400</code> bad shape/agent_id
(pattern <code>[A-Za-z0-9_-]{1,40}</code>) &middot; <code>404</code> agent offline or unknown
file_id &middot; <code>405</code> wrong verb (valid methods listed) &middot; <code>413</code>
over 25 MB &middot; <code>415</code> filetype rejected (shows the sanitized name it checked).</div>

<h2>Read before scripting</h2>
<div class="card"><ul style="margin:0;padding-left:20px">
<!--FOOTGUNS-->
</ul></div>

<h2>Trust model</h2>
<div class="card"><code>AGENT_AUTH_TOKEN</code> is the sole credential - any holder has full
access to task agents, drain inboxes and move files. <code>X-Agent-Id</code> is a label, not an
identity. Share the token only with agents you trust to steer the whole mesh. The hub is a
switchboard: it enforces no hierarchy and knows no master.</div>

<p class="mut">Socket.IO namespace <code>/agents</code>; full event payloads in <a href="/api">/api</a>
under <code>socket</code>. Wrong socket token => refused + logged AUTH_FAIL.</p>
</main></body></html>"""


def _endpoint_rows() -> str:
    out = []
    for e in API_ENDPOINTS:
        note = (f"<div class='mut'>{html_mod.escape(e['note'])}</div>" if e.get("note") else "")
        out.append(f"<tr><td><code>{e['method']} {html_mod.escape(e['path'])}</code></td>"
                   f"<td>{html_mod.escape(e['auth'])}</td>"
                   f"<td>{html_mod.escape(e['summary'])}{note}</td></tr>")
    return "".join(out)


def _footgun_items() -> str:
    return "".join(f"<li>{html_mod.escape(g)}</li>" for g in FOOTGUNS)


ONBOARDING_HTML = (ONBOARDING_TEMPLATE
                   .replace("<!--ENDPOINTS-->", _endpoint_rows())
                   .replace("<!--FOOTGUNS-->", _footgun_items())
                   .replace("__VERSION__", html_mod.escape(HUB_VERSION)))


def _build_llms_md() -> str:
    parts = [
        "# Agent Hub - API guide for AI agents",
        "",
        f"Version {HUB_VERSION}. Base URL = wherever you fetched this file.",
        "If this page came back as an ngrok HTML interstitial, resend with header"
        " `ngrok-skip-browser-warning: true`.",
        "",
        "## Auth",
        "",
        "One shared token guards the whole hub: `AGENT_AUTH_TOKEN` (6-50 chars), sent as"
        " `Authorization: Bearer <T>` (also accepted: `X-Auth-Token`, `X-Agent-Token`)."
        " `X-Agent-Id: <id>` labels your traffic but does NOT authenticate - any holder can"
        " task any agent, drain any inbox, move any file.",
        "Missing/invalid token => 401 JSON with a hint (HTML only if you `Accept: text/html`).",
        "",
        "## Quickstart",
        "",
        "```bash",
        "HUB=https://<hub-host>; export AGENT_AUTH_TOKEN=<T>",
        "# join an agent (needs python3 + pip install requests python-socketio):",
        "python3 mock_agent.py --server $HUB --agent-id scout --token $AGENT_AUTH_TOKEN",
        "# task it (reply comes back on this same call):",
        "python3 mock_agent.py --message scout --text 'hello'   # or the curl below",
        "curl -s -X POST $HUB/agent/scout/message -H \"Authorization: Bearer $AGENT_AUTH_TOKEN\""
        " -H \"ngrok-skip-browser-warning: true\" -H \"Content-Type: application/json\" -d '{\"text\":\"hi\"}'",
        "# one-shot helpers: --inbox <id> [--peek], --agents, --relay-to <id> --text ..., "
        "--upload f --to <id>, --download <file_id>, --health, --docs",
        "```",
        "",
        "## Endpoints",
        "",
    ]
    for e in API_ENDPOINTS:
        bits = [f"### {e['method']} {e['path']}  ({e['auth']})"]
        bits.append(e["summary"])
        for key, label in (("body", "Body"), ("query", "Query"), ("headers", "Headers"),
                           ("returns", "Returns"), ("note", "Note"), ("example", "Example"),
                           ("errors", "Errors")):
            if e.get(key):
                val = e[key]
                val = f"```json\n{json.dumps(val)}\n```" if isinstance(val, dict) else str(val)
                if key == "example":
                    val = f"```bash\n{val}\n```"
                bits.append(f"- {label}: {val}")
        parts.append("\n".join(bits))
        parts.append("")
    parts += [
        "## Error contract",
        "",
        "Errors are JSON: `{\"error\", \"hint\", \"docs\", \"api\", ...}` - the hint names the fix.",
        "`400` bad agent_id (pattern `[A-Za-z0-9_-]{1,40}`) or body shape &middot; `401` token"
        " &middot; `404` agent offline (hint shows how to start one) or unknown file_id &middot;"
        " `405` wrong verb &middot; `413` >25 MB &middot; `415` rejected filetype (echoes"
        " `name_after_sanitize`) &middot; `500` internal (no details unless hub runs `HUB_DEBUG=1`).",
        "",
        "## Gotchas",
        "",
    ]
    parts += [f"{i}. {g}" for i, g in enumerate(FOOTGUNS, 1)]
    parts += [
        "",
        "## Socket.IO contract (namespace `/agents`)",
        "",
        "```json",
        json.dumps(SOCKET_CONTRACT, indent=2),
        "```",
        "",
        "## Hub env vars",
        "",
        "`AGENT_AUTH_TOKEN` (req) &middot; `LOG_SECRET_TOKEN` (req) &middot; `HUB_PORT` (5000) &middot;"
        " `HUB_BIND` (127.0.0.1 - the hub is local by default, publish it behind nginx or ngrok)"
        " &middot; `ACK_TIMEOUT` (10s) &middot; `NGROK_AUTHTOKEN` (optional"
        " public tunnel) &middot; `NGROK_DOMAIN` (pin reserved domain so URL survives restarts) &middot;"
        " `HUB_FILE_STORE` / `HUB_LOG_FILE` (relocate state for tests) &middot; `HUB_DEBUG=1`."
        " mock_agent honors `HUB_STATE_DIR`.",
        "",
        "Full JSON manifest: `GET /api`. Human docs: `GET /`.",
    ]
    return "\n".join(parts) + "\n"


LLMS_MD = _build_llms_md()


def onboarding_response(status: int) -> Response:
    return Response(ONBOARDING_HTML, status=status, content_type="text/html; charset=utf-8")


def require_actor():
    """Gate an HTTP endpoint behind the sole hub token. Returns None if allowed,
    else a content-negotiated 401 (JSON for agents, onboarding HTML for browsers)."""
    if authorized():
        return None
    log_event("AUTH_FAIL", actor_label(), "Client/Agent -> Server (rejected)",
              f"{request.method} {request.path} missing/invalid AGENT_AUTH_TOKEN")
    if wants_html():
        return onboarding_response(401)
    return jsonify(error="missing or invalid AGENT_AUTH_TOKEN",
                   hint='add -H "Authorization: Bearer <AGENT_AUTH_TOKEN>" (or X-Auth-Token / '
                        'X-Agent-Token); through ngrok free tier also add '
                        '-H "ngrok-skip-browser-warning: true"',
                   docs="/llms.txt", api="/api",
                   method=request.method, path=request.path,
                   example=f'curl -s -H "Authorization: Bearer $AGENT_AUTH_TOKEN" '
                           f'-H "ngrok-skip-browser-warning: true" $HUB{request.path}'), 401


# ----------------------------------------------------------------- HTTP routes
@app.get("/")
def root():
    return onboarding_response(200)


@app.get("/api")
def api_manifest():
    """Unauthenticated machine manifest. Rendered per request so base_url matches
    the host actually serving (ngrok URL changes on every restart)."""
    scheme = request.headers.get("X-Forwarded-Proto", request.scheme)
    return jsonify(version=HUB_VERSION, features=FEATURES,
                   base_url=f"{scheme}://{request.host}",
                   docs="/llms.txt",
                   auth={"token_env": "AGENT_AUTH_TOKEN",
                         "how": "Authorization: Bearer <T> | X-Auth-Token | X-Agent-Token",
                         "model": "one shared token = full access; X-Agent-Id labels, never authenticates"},
                   headers={"ngrok_free_tier": "send ngrok-skip-browser-warning: true on every request"},
                   endpoints=API_ENDPOINTS, socket=SOCKET_CONTRACT, footguns=FOOTGUNS)


@app.get("/llms.txt")
def llms_txt():
    return Response(LLMS_MD, content_type="text/markdown; charset=utf-8")


@app.get(FAVICON_ROUTE)
def favicon():
    """Unauthenticated: browsers fetch it before any token could apply."""
    if not FAVICON_FILE.exists():
        abort(404)
    return send_file(FAVICON_FILE, mimetype="image/x-icon", max_age=86400)


@app.get("/health")
def health():
    with reg_lock:
        n = len(agents)
    return jsonify(status="ok", agents_connected=n, uptime="see logs",
                   version=HUB_VERSION, features=FEATURES,
                   uptime_seconds=int(time.time() - STARTED_AT),
                   docs="/llms.txt", api="/api",
                   public_url=TUNNEL_URL or None)


@app.get("/agents")
def list_agents():
    if deny := require_actor():
        return deny
    with reg_lock:
        snap = dict(agents)
        since = dict(agent_since)
        sup = dict(agent_superseded)
        std = {aid: dict(s) for aid, s in standby.items() if s}
    return jsonify(agents=snap,                       # frozen shape: {id: sid}
                   count=len(snap), agent_ids=sorted(snap),
                   detail={aid: {"sid": sid, "connected_at": since.get(aid),
                                 "last_superseded_at": sup.get(aid),
                                 "standby_sockets": len(std.get(aid) or {})}
                           for aid, sid in snap.items()},
                   standby=std,
                   standby_note="still-open sockets that lost an agent_id take-over; the most "
                                "recent one reclaims routing when the holder disconnects")


@app.get("/agent-id/<agent_id>")
def agent_id_status(agent_id):
    """Pre-flight check for the v1.4 uniqueness rule: is this id free?"""
    if deny := require_actor():
        return deny
    if not AGENT_ID_RE.fullmatch(agent_id):
        return _err(400, "invalid agent_id", pattern="[A-Za-z0-9_-]{1,40}",
                    hint="ids are case-sensitive: 'Scout' and 'scout' are two agents")
    with reg_lock:
        holder = agents.get(agent_id)
        since = agent_since.get(agent_id)
        stands = dict(standby.get(agent_id) or {})
    with meta_lock:
        rej = dict(id_rejects.get(agent_id) or {})
    return jsonify(agent_id=agent_id, available=holder is None,
                   taken_by_sid=holder, connected_at=since, standby_sockets=len(stands),
                   last_rejection=rej or None,
                   note="free ids may be claimed by any holder of the token; a connect with an "
                        "id that is already live is refused unless force_takeover is set")


@app.post("/agent/<agent_id>/message")
def send_to_agent(agent_id):
    if deny := require_actor():
        return deny
    if not AGENT_ID_RE.fullmatch(agent_id):
        return _err(400, "invalid agent_id",
                    hint="agent_id must match [A-Za-z0-9_-]{1,40}",
                    pattern="[A-Za-z0-9_-]{1,40}", agent_id=agent_id)
    with reg_lock:
        sid = agents.get(agent_id)
    if sid is None:
        log_event("MSG_FAIL", agent_id, "Client -> Server (agent offline)",
                  "no persistent socket; request refused instantly")
        return _err(404, f"agent '{agent_id}' is offline",
                    hint=f"start it: python3 mock_agent.py --server $HUB --agent-id {agent_id} "
                         "--token $AGENT_AUTH_TOKEN ; live agents: GET /agents",
                    agent_id=agent_id, live_agents_endpoint="/agents")

    body = request.get_json(silent=True) or {}
    text = body.get("text") if isinstance(body, dict) else None
    warning = ""
    if text is None:
        raw = request.get_data(as_text=True)
        text = raw or json.dumps(body)
        if not raw:
            warning = "empty request body: the agent receives the literal text '{}'"
    msg_id = pysecrets.token_hex(6)
    pentry = {"event": threading.Event(), "replies": []}
    with meta_lock:
        pending[msg_id] = pentry

    socketio.emit("task", {"msg_id": msg_id, "from": actor_label(), "text": text},
                  to=sid, namespace=NS)
    log_event("MSG_SENT", agent_id, "Client -> Server -> Agent",
              f"[{msg_id}] {eprint_summary(text)}")

    try:
        wait = min(max(float(request.args.get("wait", ACK_TIMEOUT)), 0), 60)
    except ValueError:
        wait = ACK_TIMEOUT
    if wait > 0 and pentry["event"].wait(timeout=wait):
        reply = pentry["replies"][-1] if pentry["replies"] else None
        log_event("MSG_RCVD", agent_id, "Agent -> Server -> Client (HTTP reply)",
                  f"[{msg_id}] {eprint_summary(reply)}")
        status = "replied"
    else:
        reply = None
        status = "delivered_no_ack"
    with meta_lock:
        pending.pop(msg_id, None)
    out = {"status": status, "msg_id": msg_id, "agent_id": agent_id, "reply": reply}
    if status == "replied":
        out["note"] = ("first result only - mock_agent auto-ACKs here; the agent's real answer "
                       "arrives later on GET /agent/%s/inbox?peek=true" % agent_id)
    if warning:
        out["warning"] = warning
    return jsonify(out)


@app.get("/agent/<agent_id>/inbox")
def agent_inbox(agent_id):
    if deny := require_actor():
        return deny
    peek = request.args.get("peek", "").lower() in ("1", "true", "yes")
    with meta_lock:
        q = client_inboxes.get(agent_id)
        msgs = list(q) if q else []
        existed = q is not None
        if q and not peek:
            q.clear()
    with reg_lock:
        online = agent_id in agents
    return jsonify(agent_id=agent_id, messages=msgs, count=len(msgs),
                   peek=peek, drained=(not peek) and existed,
                   queue_max=client_inboxes[agent_id].maxlen, agent_online=online)


@app.get("/result/<msg_id>")
def task_result(msg_id):
    """Correlation for two-phase replies: everything the hub recorded for a msg_id.
    Answers that arrived after the HTTP call timed out still land here, tagged."""
    if deny := require_actor():
        return deny
    with meta_lock:
        r = RESULTS.get(msg_id)
        out = dict(r) if r else None
    if out is None:
        return _err(404, "unknown msg_id",
                    hint="only the last 500 tasks are kept (older ones evicted, hub restarts "
                         "clear them too); the reply also carries msg_id inside "
                         "GET /agent/<id>/inbox entries", msg_id=msg_id, window=500)
    out["status"] = "done" if len(out.get("results", [])) > 1 else out.get("status", "first_result")
    out["count"] = len(out.get("results", []))
    out["note"] = ("status: first_result = one `result` seen | done = more than one | "
                   "answered_via_inbox = the agent answered with agent_to_client (tagged with "
                   "this msg_id) instead of emitting results")
    return jsonify(out)


@app.get("/events/mine")
def events_mine():
    """Event rows that involve the caller - agents hold the full-access token but the
    human log page needs a separate secret they usually don't have."""
    if deny := require_actor():
        return deny
    aid = request.headers.get("X-Agent-Id", "").strip()
    if not AGENT_ID_RE.fullmatch(aid or ""):
        return _err(400, "X-Agent-Id header required (your agent id)",
                    pattern="[A-Za-z0-9_-]{1,40}")
    try:
        limit = max(1, min(int(request.args.get("limit", 100)), 500))
    except ValueError:
        limit = 100
    tag_agent, tag_human = f"agent:{aid}", f"Agent:{aid}"
    with log_lock:
        rows = [e for e in LOG_EVENTS
                if e["agent"] == aid or tag_agent in e["dir"] or tag_human in e["dir"]
                or tag_agent in e["payload"] or tag_human in e["payload"]]
    return jsonify(events=rows[-limit:], count=min(len(rows), limit), total_matching=len(rows),
                   caller=aid, note="rows where your id appears as agent, source or target")


@app.get("/client.py")
def client_source():
    """The reference client (mock_agent.py) one curl away, so remote agents can copy it."""
    if deny := require_actor():
        return deny
    p = BASE_DIR / "mock_agent.py"
    if not p.exists():
        return _err(404, "mock_agent.py not found next to the hub", path=request.path)
    return Response(p.read_text(encoding="utf-8"),
                    content_type="text/x-python; charset=utf-8")


@app.post("/relay")
def relay():
    if deny := require_actor():
        return deny
    data = request.get_json(silent=True) or {}
    to = str(data.get("to", ""))
    text = data.get("text", "")
    sender = actor_label()
    if not AGENT_ID_RE.fullmatch(to) or not text:
        return _err(400, "body needs {to: agent_id, text: string}",
                    hint='e.g. -d \'{"to":"scout","text":"findings ready"}\' ; '
                         "agent_id pattern [A-Za-z0-9_-]{1,40}")
    with reg_lock:
        sid = agents.get(to)
    if sid is None:
        log_event("MSG_FAIL", to, f"{sender} -> Server (offline)", "relay refused instantly")
        return _err(404, f"target agent '{to}' is offline",
                    hint="live agents: GET /agents ; start one: python3 mock_agent.py "
                         "--server $HUB --agent-id <id> --token $AGENT_AUTH_TOKEN", to=to)
    socketio.emit("peer_msg", {"from": sender, "text": text}, to=sid, namespace=NS)
    log_event("MSG_SENT", to, f"{sender} -> Server -> Agent:{to}", eprint_summary(text))
    return jsonify(status="relayed", to=to)


# ----------------------------------------------------------------- file store
def _safe_name(raw: str) -> str:
    name = secure_filename(os.path.basename(raw or "file.bin"))
    return name or "file.bin"


def _ext_ok(name: str) -> bool:
    low = name.lower()
    return any(low.endswith(ext) for ext in ALLOWED_EXT)


UPLOAD_HOWTO = ('send multipart -F file=@report.json OR raw bytes with -H "X-Filename: '
                'report.json" (ASCII name, extension .json .txt .html .htm .tar.gz .tgz)')


def _notify_target(file_id: str, meta: dict, target: str, sender: str):
    """Push a file_ready notice to `target`. Returns (delivered, target_error)."""
    if not target:
        return False, ""
    if not AGENT_ID_RE.fullmatch(target):
        return False, ("invalid X-Target-Agent id (pattern [A-Za-z0-9_-]{1,40}); "
                       "file stored, no notify sent")
    with reg_lock:
        sid = agents.get(target)
    if sid:
        socketio.emit("file_ready", dict(file_id=file_id, url=f"/file/{file_id}", **meta),
                      to=sid, namespace=NS)
        return True, ""
    log_event("MSG_FAIL", target, f"{sender} -> Server (offline)",
              f"file {meta['name']} stored; no socket to notify")
    return False, f"target agent '{target}' is offline; file stored, no notify sent"


@app.post("/file")
def upload_file():
    if deny := require_actor():
        return deny
    sender = actor_label()
    if request.files.get("file"):
        fs = request.files["file"]
        raw_name = fs.filename or "file.bin"
        data = fs.stream.read(MAX_UPLOAD + 1)
        ctype = fs.mimetype or "application/octet-stream"
    else:
        raw_name = request.headers.get("X-Filename", "").strip()
        ctype = request.headers.get("Content-Type", "").split(";")[0].strip()
        if not raw_name and ctype == "application/json":
            raw_name = "body.json"          # a raw JSON body could only ever 415 before
        raw_name = raw_name or "file.bin"
        data = request.get_data()
        ctype = ctype or "application/octet-stream"
    name = _safe_name(raw_name)
    if not data:
        return _err(400, "empty body - no file bytes", hint=UPLOAD_HOWTO)
    if len(data) > MAX_UPLOAD:
        return _err(413, f"too large (max {MAX_UPLOAD} bytes)", max_bytes=MAX_UPLOAD)
    if not _ext_ok(name):
        return _err(415, "filetype not allowed",
                    hint=UPLOAD_HOWTO + f"; note secure_filename rewrote '{raw_name}' to "
                         f"'{name}' (non-ASCII names lose their extension)",
                    name_received=raw_name, name_after_sanitize=name, allowed=list(ALLOWED_EXT))

    sha = hashlib.sha256(data).hexdigest()
    file_id = pysecrets.token_hex(8)
    now_dt = datetime.now(timezone.utc)
    meta = {"name": name, "type": ctype, "size": len(data), "sha256": sha, "by": sender,
            "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "created_iso": now_dt.isoformat(timespec="seconds"),
            "created_epoch": int(now_dt.timestamp())}

    dedupe = request.args.get("dedupe", "").lower() in ("1", "true", "yes")
    with meta_lock:
        dup = sha_index.get(sha)
        if dup not in file_meta:
            dup = None
        if dedupe and dup:
            reused = dict(file_meta[dup])
        else:
            reused = None
            path = FILE_STORE / f"{file_id}__{name}"
            path.write_bytes(data)
            file_meta[file_id] = meta
            sha_index[sha] = file_id
            persist_file_index()

    target = request.headers.get("X-Target-Agent", "").strip()
    if reused:
        delivered, target_error = _notify_target(dup, reused, target, sender)
        log_event("FILE_SENT", target or "-", f"{sender} -> Server (deduped)",
                  f"{name} ({len(data)} B, sha={sha[:12]}) -> existing id {dup}, "
                  f"pushed={delivered}")
        out = {"status": "existing", "file_id": dup, "deduped": True, "name": name,
               "size": len(data), "sha256": sha, "delivered": delivered,
               "delivered_to": target if target else None, "download_url": f"/file/{dup}",
               "first_uploaded_by": reused.get("by"), "bytes_stored": False,
               "dedupe_note": "no new object written; this id is a byte-for-byte match"}
        if target_error:
            out["target_error"] = target_error
        return jsonify(out), 200

    delivered, target_error = _notify_target(file_id, meta, target, sender)
    log_event("FILE_SENT", target or "-", f"{sender} -> Server -> File store",
              f"{name} ({len(data)} B, sha={sha[:12]}, pushed={delivered})")
    out = {"status": "stored", "file_id": file_id, "name": name, "size": len(data),
           "sha256": sha, "delivered": delivered,
           "delivered_to": target if target else None, "download_url": f"/file/{file_id}"}
    if target_error:
        out["target_error"] = target_error
    if dup:
        out["duplicate_of"] = dup
        out["duplicate_note"] = ("identical bytes already stored under this id (advisory; "
                                 "a new id was minted anyway - add ?dedupe=1 to reuse it)")
    return jsonify(out), 201


@app.get("/files")
def list_files():
    if deny := require_actor():
        return deny
    with meta_lock:
        return jsonify({"files": dict(file_meta), "count": len(file_meta)})


@app.get("/file/<file_id>")
def download_file(file_id):
    if deny := require_actor():
        return deny
    with meta_lock:
        meta = file_meta.get(file_id)
    if not meta:
        return _err(404, "unknown file_id", hint="list what exists: GET /files", file_id=file_id)
    path = FILE_STORE / f"{file_id}__{meta['name']}"
    if not path.exists():
        return _err(404, "file bytes missing from store",
                    hint=f"metadata exists but {meta['name']} is gone from disk", file_id=file_id)
    log_event("FILE_RCVD", actor_label(), "File store -> Server -> Client/Agent",
              f"downloaded {meta['name']} ({meta['size']} B)")
    return send_file(path, as_attachment=True, download_name=meta["name"],
                     mimetype=meta["type"])


# ----------------------------------------------------------------- log viewer
@app.get("/logs")
def logs_no_token():
    abort(404)


def _log_token_ok(token: str) -> bool:
    return len(token) <= 64 and pysecrets.compare_digest(token, LOG_SECRET)


@app.get("/logs/<token>")
def logs_view(token):
    if not _log_token_ok(token):
        log_event("AUTH_FAIL", "-", "Client -> Server (log 404)", f"invalid log token: {token[:16]!r}")
        abort(404)
    if not LOG_FILE.exists():
        init_log_file()
    return Response(LOG_FILE.read_text(encoding="utf-8"), content_type="text/html; charset=utf-8")


@app.get("/logs/<token>/events.json")
def logs_events(token):
    if not _log_token_ok(token):
        abort(404)
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 3000))
    except ValueError:
        limit = 50
    with log_lock:
        events = list(LOG_EVENTS)[-limit:]
        total = len(LOG_EVENTS)
    return jsonify(events=events, count=len(events), total=total, newest_last=True)


# ----------------------------------------------------------------- socket events
@socketio.on("connect", namespace=NS)
def on_connect(auth=None):
    auth = auth or {}
    token = auth.get("token") or request.args.get("token", "")
    agent_id = (auth.get("agent_id") or request.args.get("agent_id", "")).strip()
    if not (AGENT_TOKEN and pysecrets.compare_digest(str(token), AGENT_TOKEN)):
        log_event("AUTH_FAIL", agent_id or "-", "Agent -> Server (socket rejected)",
                  "missing/invalid AGENT_AUTH_TOKEN")
        return False
    if not AGENT_ID_RE.fullmatch(agent_id):
        log_event("AUTH_FAIL", agent_id[:40] or "-", "Agent -> Server (socket rejected)",
                  "missing/invalid agent_id")
        return False
    force = (str(auth.get("force_takeover", "")).lower() in ("1", "true", "yes")
             or request.args.get("force", "").lower() in ("1", "true", "yes"))
    with reg_lock:
        holder = agents.get(agent_id)
        holder_since = agent_since.get(agent_id)
    if holder and holder != request.sid and not force:
        reason = (f"agent_id '{agent_id}' is already connected (sid {holder[:12]}... since "
                  f"{holder_since}). Ids are unique on this hub since v1.4: pick another "
                  f"agent_id, or pass auth {{'force_takeover': true}} / ?force=1 to displace "
                  f"the live socket deliberately. Who holds what: GET /agents, "
                  f"GET /agent-id/{agent_id}")
        with meta_lock:
            id_rejects[agent_id] = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                    "holder": holder, "reason": "duplicate agent_id"}
            while len(id_rejects) > 200:
                id_rejects.popitem(last=False)
        log_event("ID_REJECTED", agent_id, "Agent -> Server (socket refused)",
                  f"duplicate agent_id; holder sid={holder[:12]} - client must choose a "
                  f"unique id or force_takeover")
        raise ConnectionRefusedError(reason)
    with reg_lock:
        old = agents.get(agent_id)
        old_since = agent_since.get(agent_id)
        agents[agent_id] = request.sid
        sid_to_agent[request.sid] = agent_id
        agent_since[agent_id] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        standby[agent_id].pop(request.sid, None)
        if old and old != request.sid:
            agent_superseded[agent_id] = agent_since[agent_id]
            # the outgoing socket is still open: keep it as the next claimant (v1.3 zombie fix)
            standby[agent_id][old] = old_since or agent_superseded[agent_id]
    if old and old != request.sid:
        log_event("DISCONNECTED", agent_id, "Agent -> Server (replaced)",
                  "stale socket replaced by fresh connection")
        try:  # tell the outgoing socket it lost routing (footgun 3 was invisible before v1.2)
            socketio.emit("superseded",
                          {"agent_id": agent_id, "reason": "duplicate agent_id re-registered",
                           "at": agent_superseded[agent_id], "sid": old,
                           "sid_note": "sid is the hub registry's /agents-namespace sid; "
                                       "compare with the client's sio.get_sid(namespace="
                                       "'/agents'), never sio.sid"}, to=old, namespace=NS)
        except Exception:  # noqa: BLE001 - old socket may already be dead
            pass
    log_event("CONNECTED", agent_id, "Agent -> Server (socket open)",
              f"sid={request.sid[:12]} transport={getattr(request, 'transport', '?')}")


@socketio.on("disconnect", namespace=NS)
def on_disconnect():
    promote = None
    with reg_lock:
        agent_id = sid_to_agent.pop(request.sid, None)
        standby.get(agent_id, {}).pop(request.sid, None)
        if agent_id and agents.get(agent_id) == request.sid:
            agents.pop(agent_id, None)
            agent_since.pop(agent_id, None)
            claims = standby.get(agent_id) or {}
            if claims:
                # hand the id back to the most recent still-open claimant instead of
                # orphaning it (v1.2 left a live socket unrouted after a rival exited)
                promote = max(claims.items(), key=lambda kv: kv[1])[0]
                agents[agent_id] = promote
                sid_to_agent[promote] = agent_id
                agent_since[agent_id] = claims.pop(promote)
                if not claims:
                    standby.pop(agent_id, None)
            else:
                agent_superseded.pop(agent_id, None)
    if agent_id and promote:
        log_event("CONNECTED", agent_id, "Server -> Agent (re-registered standby socket)",
                  f"sid={promote[:12]} routing returned after the holder disconnected")
        try:
            socketio.emit("reactivated",
                          {"agent_id": agent_id, "reason": "the socket that superseded you "
                           "disconnected; routing is yours again", "sid": promote,
                           "sid_note": "sid is the hub registry's /agents-namespace sid; "
                                       "compare with the client's sio.get_sid(namespace="
                                       "'/agents'), never sio.sid",
                           "at": datetime.now(timezone.utc).isoformat(timespec="seconds")},
                          to=promote, namespace=NS)
        except Exception:  # noqa: BLE001 - standby socket may have died concurrently
            pass
    elif agent_id:
        log_event("DISCONNECTED", agent_id, "Agent -> Server (socket closed)",
                  "persistent connection dropped; future requests to this agent return 404")


@socketio.on("result", namespace=NS)
def on_result(data=None):
    agent_id = sid_to_agent.get(request.sid, "?")
    msg_id = str((data or {}).get("msg_id", ""))
    text = (data or {}).get("text", "")
    with meta_lock:
        if msg_id:
            r = RESULTS.get(msg_id)
            if r is None:
                if len(RESULTS) >= 500:
                    RESULTS.popitem(last=False)
                r = RESULTS[msg_id] = {"agent": f"agent:{agent_id}", "msg_id": msg_id,
                                       "results": [], "updated": None}
            r["results"].append({"from": f"agent:{agent_id}", "text": text})
            r["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            r["status"] = "done" if len(r["results"]) > 1 else "first_result"
        p = pending.get(msg_id)
        http_will_log = False
        if p:
            http_will_log = not p["event"].is_set()
            p["replies"].append({"from": f"agent:{agent_id}", "text": text})
            p["event"].set()
        elif agent_id != "?":
            client_inboxes[agent_id].append({"from": f"agent:{agent_id}", "msg_id": msg_id,
                                             "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                             "text": text})
    # one MSG_RCVD row per result: the first reply to a waiting HTTP caller is logged by that route
    if not http_will_log:
        log_event("MSG_RCVD", agent_id, "Agent -> Server (recorded, no HTTP waiter)",
                  f"[{msg_id}] {eprint_summary(text)}")


@socketio.on("agent_to_client", namespace=NS)
def on_agent_to_client(data=None):
    agent_id = sid_to_agent.get(request.sid)
    if agent_id is None:
        return
    msg_id = (data or {}).get("msg_id")
    text = (data or {}).get("text", "")
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    client_inboxes[agent_id].append({"from": f"agent:{agent_id}", "ts": stamp,
                                     "msg_id": msg_id, "text": text})
    if msg_id:  # agents that answer over this channel never emit `result`, so tag the task
        with meta_lock:
            r = RESULTS.get(str(msg_id))
            if r is not None:
                r["answered_via_inbox"] = True
                r["updated"] = stamp
                r["status"] = "answered_via_inbox"
    log_event("MSG_RCVD", agent_id, "Agent -> Server -> Client (queued)", eprint_summary(text))


@socketio.on("agent_to_agent", namespace=NS)
def on_agent_to_agent(data=None):
    sender = sid_to_agent.get(request.sid, "?")
    to = str((data or {}).get("to", ""))
    text = (data or {}).get("text", "")
    if not AGENT_ID_RE.fullmatch(to) or not text:
        return {"error": "needs {to, text}"}
    with reg_lock:
        sid = agents.get(to)
    if not sid:
        log_event("MSG_FAIL", to, f"Agent:{sender} -> Server (offline)", "agent_to_agent dropped")
        return {"error": f"agent '{to}' offline"}
    socketio.emit("peer_msg", {"from": f"agent:{sender}", "text": text}, to=sid, namespace=NS)
    log_event("MSG_SENT", to, f"Agent:{sender} -> Server -> Agent:{to}", eprint_summary(text))
    return {"status": "relayed", "to": to}


# ----------------------------------------------------------------- error handlers
@app.errorhandler(404)
def not_found(_e):
    if wants_html():
        return Response("<!DOCTYPE html><html><head><meta charset='utf-8'><title>404</title>"
                        "<link rel='icon' href='/favicon.ico'></head>"
                        "<body style='font-family:monospace;background:#111214;color:#e7e7ea;padding:40px'>"
                        "<h1>404 Not Found</h1><p>That path does not exist (bad log token? typo?). "
                        "Start at <a style='color:#60a5fa' href='/'>/ for onboarding</a> or "
                        "<a style='color:#60a5fa' href='/llms.txt'>/llms.txt for the API guide</a>.</p>"
                        "</body></html>", status=404, content_type="text/html")
    return _err(404, "not found", hint="valid paths are listed in /api and /llms.txt",
                path=request.path)


@app.errorhandler(405)
def method_not_allowed(exc):
    methods = ", ".join(sorted(exc.valid_methods or []))
    return _err(405, "method not allowed",
                hint=f"{request.path} accepts: {methods}",
                method=request.method, path=request.path, allowed=methods)


@app.errorhandler(413)
def too_large(_e):
    return _err(413, "payload too large", hint=f"max {MAX_UPLOAD} bytes", max_bytes=MAX_UPLOAD)


@app.errorhandler(HTTPException)
def http_exception(exc):
    # pass real status codes through as JSON instead of the catch-all 500 below
    return _err(exc.code, (exc.description or exc.name).lower(), path=request.path)


@app.errorhandler(Exception)
def server_error(exc):  # never crash a client into a hang
    log_event("MSG_FAIL", "-", "Server -> Client (error)", f"{type(exc).__name__}: {exc}")
    out = {"error": "internal server error", "hint": "retry once; if persistent check /logs or /health",
           "docs": "/llms.txt", "api": "/api"}
    if HUB_DEBUG:
        out["detail"] = f"{type(exc).__name__}: {exc}"
    return jsonify(out), 500


# ----------------------------------------------------------------- public tunnel (optional)
def _warn_localhost(reason: str) -> None:
    msg = f"{reason} - no tunnel, serving localhost only on http://localhost:{HUB_PORT}"
    print(f"[agent-hub] WARN: {msg}")
    log_event("SERVER", "-", "Server -> Server", msg)
    if HUB_BIND not in ("127.0.0.1", "localhost", "::1"):
        print(f"[agent-hub]   HUB_BIND={HUB_BIND} exposes the development server directly: "
              f"run with HUB_BIND=127.0.0.1 and publish through nginx instead")
    for line in (
        "to publish without ngrok, terminate TLS in local nginx and reverse-proxy this port;",
        "pass X-Forwarded-For and the Upgrade/Connection headers so agents reach the hub",
        "over WebSocket from their real IP rather than from 127.0.0.1:",
        "  server {",
        "    listen 443 ssl http2;",
        "    server_name hub.example.com;",
        "    ssl_certificate     /etc/letsencrypt/live/hub.example.com/fullchain.pem;",
        "    ssl_certificate_key /etc/letsencrypt/live/hub.example.com/privkey.pem;",
        "    gzip on; gzip_min_length 1024;",
        "    gzip_types application/json text/plain text/css;",
        f"    client_max_body_size {MAX_UPLOAD // (1024 * 1024)}m;",
        "    location / {",
        f"      proxy_pass http://127.0.0.1:{HUB_PORT};",
        "      proxy_http_version 1.1;",
        "      proxy_set_header Host              $host;",
        "      proxy_set_header X-Real-IP         $remote_addr;",
        "      proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;",
        "      proxy_set_header X-Forwarded-Proto $scheme;",
        "      proxy_set_header Upgrade           $http_upgrade;",
        '      proxy_set_header Connection        "upgrade";',
        "      proxy_read_timeout 3600s; proxy_send_timeout 3600s;",
        "    }",
        "  }",
    ):
        print(f"[agent-hub]   {line}")
    try:
        import simple_websocket  # noqa: F401  - the engine that answers the Upgrade above
    except ImportError:
        print("[agent-hub]   NOTE: simple-websocket is not installed (pip install simple-websocket),"
              " so agents fall back to long polling")


def open_tunnel() -> None:
    """Best effort: expose this hub over an ngrok HTTPS edge. Any problem (no token,
    package missing, network down) only warns - the hub keeps serving on localhost."""
    global TUNNEL_URL
    if not NGROK_AUTHTOKEN:
        _warn_localhost("NGROK_AUTHTOKEN not set (export it to publish the hub)")
        return
    try:
        import ngrok
    except ImportError:
        _warn_localhost("ngrok package missing (pip install ngrok)")
        return
    try:
        ngrok.set_auth_token(NGROK_AUTHTOKEN)
        if NGROK_DOMAIN:
            listener = ngrok.forward(f"localhost:{HUB_PORT}", proto="http", domain=NGROK_DOMAIN)
        else:
            listener = ngrok.forward(f"localhost:{HUB_PORT}", proto="http")
    except Exception as exc:  # noqa: BLE001 - tunnel failure must not kill the hub
        # ngrok echoes the authtoken back in its error text, keep it out of the log page
        detail = str(exc).replace(NGROK_AUTHTOKEN, "<redacted>")
        _warn_localhost(f"ngrok tunnel failed: {type(exc).__name__}: {detail}")
        return
    TUNNEL_URL = listener.url()
    url = TUNNEL_URL
    print(f"[agent-hub] ngrok tunnel up: {url}")
    print(f"[agent-hub]   agents:  python3 mock_agent.py --server {url} --agent-id <id> --token \"$AGENT_AUTH_TOKEN\"")
    print(f"[agent-hub]   clients: send header  ngrok-skip-browser-warning: true  on every request")
    print(f"[agent-hub] Logs are (public): {url}/logs/{LOG_SECRET}", flush=True)
    log_event("SERVER", "-", "Server -> ngrok edge", f"public {url} -> :{HUB_PORT}")


# ----------------------------------------------------------------- startup banner
def announce_log_urls() -> None:
    """Print the log page as a clickable URL straight after werkzeug's own
    'Running on ...' lines, so the operator never retypes the secret. The ngrok edge
    adds its public URL from open_tunnel(), which usually comes up a second later."""
    banner = BaseWSGIServer.log_startup

    def log_startup(server):
        banner(server)
        print(f"[agent-hub] Logs are: http://localhost:{HUB_PORT}/logs/{LOG_SECRET}",
              flush=True)

    BaseWSGIServer.log_startup = log_startup


# ----------------------------------------------------------------- main
if __name__ == "__main__":
    init_log_file()
    load_file_index()
    log_event("SERVER", "-", "Server -> Server",
              f"hub v{HUB_VERSION} started on {HUB_BIND}:{HUB_PORT} (threading mode)")
    print(f"[agent-hub] v{HUB_VERSION} listening on {HUB_BIND}:{HUB_PORT} | agents socket ns={NS} | "
          f"hub token len={len(AGENT_TOKEN)} | docs = /llms.txt")
    announce_log_urls()
    threading.Thread(target=open_tunnel, daemon=True).start()
    socketio.run(app, host=HUB_BIND, port=HUB_PORT, debug=False, allow_unsafe_werkzeug=True)
