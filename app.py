#!/usr/bin/env python3
"""Agent Hub - Flask + Flask-SocketIO orchestrator that routes messages and
files between HTTP clients and AI agents stuck behind NAT (persistent
outbound socket). Run:  AGENT_AUTH_TOKEN=... LOG_SECRET_TOKEN=... python3 app.py
AGENT_AUTH_TOKEN is the operator credential - holders get full access, so any
agent may task any other agent. Agents may instead use the scoped per-socket
credential the hub pushes as `agent_token` (v1.5). Optional: NGROK_AUTHTOKEN=... opens a public
HTTPS tunnel from inside this app (NGROK_DOMAIN=... pins a reserved free
domain so the URL survives restarts); without a token the hub warns and serves
localhost only. Agent-facing docs: GET /llms.txt (markdown) and GET /api
(machine manifest) - both unauthenticated. Errors come back as JSON with an
actionable hint unless the caller asks for HTML."""

import hashlib
import hmac as hmac_mod
import html as html_mod
import json
import mimetypes
import os
import re
import secrets as pysecrets
import threading
import time
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timedelta, timezone
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
# How long a delivered task may sit with no result at all before the hub calls it dead. Set well
# above the HTTP wait budget (60s max) so a slow agent is not executed by a impatient caller.
TASK_TTL_SECONDS = max(30, int(os.environ.get("TASK_TTL_SECONDS", "900")))
TASK_LEDGER_MAX = 500          # msg_ids kept in the ledger before the oldest is evicted
DEAD_LETTER_MAX = 200
# ----------------------------------------------------------------- principals (v1.5)
# Each agent socket is handed a credential at connect:
#   "<agent_id>.<epoch>.<HMAC(CRED_SECRET, "agent_id|epoch")>"
# Only the epoch is kept server-side - one per live agent_id - so rotating it (reconnect,
# take-over, disconnect) revokes whatever the old socket was given, with no token store to sweep.
# The credential is what makes `from` a fact instead of a typed header.
CRED_SECRET = os.environ.get("HUB_CRED_SECRET") or pysecrets.token_hex(16)
# Set HUB_OPERATOR_HTTP=0 to demote the master token to a socket-connect secret only: then every
# HTTP call must present a named agent credential, and one-shot curl has to go through an agent.
OPERATOR_HTTP = os.environ.get("HUB_OPERATOR_HTTP", "1") == "1"
NGROK_AUTHTOKEN = os.environ.get("NGROK_AUTHTOKEN", "")  # optional: public tunnel, else localhost only
NGROK_DOMAIN = os.environ.get("NGROK_DOMAIN", "")        # optional: reserved ngrok domain (stable URL)
HUB_DEBUG = os.environ.get("HUB_DEBUG", "") == "1"       # 500 responses include exception detail

HUB_VERSION = "1.5.0"
FEATURES = ["llms_txt", "api_manifest", "json_errors", "method_405", "inbox_peek",
            "agents_detail", "upload_sha256", "autojson_name", "file_index",
            "events_json", "ngrok_domain", "result_lookup", "events_mine",
            "client_source", "dedupe_uploads", "superseded_notice", "single_result_log_rows",
            "standby_takeover_chain", "result_inbox_status", "sid_scope_notes",
            "unique_agent_ids", "task_ledger", "dead_letter_queue", "agent_last_seen",
            "agent_credentials", "scoped_reads", "operator_principal"]
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
agent_epoch = {}           # agent_id -> epoch half of the live socket's credential (v1.5)
agent_superseded = {}      # agent_id -> iso of last socket take-over
standby = defaultdict(dict)   # agent_id -> {sid: iso} sockets that lost a take-over but are still open
id_rejects = OrderedDict()    # agent_id -> {at, holder, reason} last duplicate-id connect refusals
reg_lock = threading.Lock()
INBOX_QUEUE_MAX = 200
# Only the write paths below ever allocate a queue, and they key it on an agent_id that was
# validated at connect - reads must use .get() so a probe cannot mint keys (see agent_inbox).
client_inboxes = defaultdict(lambda: deque(maxlen=INBOX_QUEUE_MAX))   # agent_id -> agent->client msgs
RESULTS = OrderedDict()    # msg_id -> task ledger row, created when the task is EMITTED (last 500)
# The ledger used to be written only when an answer arrived, so a task an agent never replied to
# left no trace anywhere and GET /result/<msg_id> answered 404 "unknown msg_id" while blaming the
# eviction window. Rows now open at emit time and move through delivered -> acked -> answered, or
# -> expired, and expired rows are also collected in DEAD_LETTER for one-call triage.
DEAD_LETTER = deque(maxlen=DEAD_LETTER_MAX)   # {reason, msg_id, agent, from, delivered_at, ...}
expired_total = 0
agent_last_seen = {}       # agent_id -> iso, last traffic the hub saw FROM that agent's socket
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
.b-TASK_EXPIRED{background:#9f1239}.b-SCOPE_DENY{background:#7c2d12}
.b-CRED_MINTED{background:#4338ca}.b-CRED_FAIL{background:#be123c}
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


# ----------------------------------------------------------------- task ledger
def stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def note_agent_traffic(agent_id: str) -> None:
    """Record that the hub just heard FROM this agent's socket. Never called with meta_lock
    held: the hub does not nest reg_lock and meta_lock anywhere."""
    if agent_id and agent_id != "?":
        with reg_lock:
            agent_last_seen[agent_id] = stamp()


def dead_letter_put(entry: dict, reason: str) -> None:
    """Caller must hold meta_lock: DEAD_LETTER is copied with list() on the read path, and
    mutating a deque mid-copy from another thread raises RuntimeError."""
    DEAD_LETTER.append({**entry, "reason": reason, "died": stamp()})


def ledger_open(msg_id: str, agent_id: str, caller: str, text: str) -> None:
    """Open a task's ledger row at emit time, whether or not anyone ever answers it."""
    at = stamp()
    entry = {"msg_id": msg_id, "agent": f"agent:{agent_id}", "hub_issued": True,
             "from": caller, "task": (text or "")[:200],
             "delivered_at": at,
             "deadline_at": (datetime.now(timezone.utc)
                             + timedelta(seconds=TASK_TTL_SECONDS)).isoformat(timespec="seconds"),
             "results": [], "status": "delivered", "state": "delivered", "updated": at}
    with meta_lock:
        RESULTS[msg_id] = entry
        while len(RESULTS) > TASK_LEDGER_MAX:
            _, evicted = RESULTS.popitem(last=False)
            if not evicted["results"]:
                dead_letter_put(evicted, "evicted")


def reap_expired_tasks() -> None:
    """'Nobody answered' becomes a logged event the moment it becomes true, instead of an
    absence an operator has to think to poll for."""
    global expired_total
    while True:
        time.sleep(15)
        dead = []
        with meta_lock:
            now = datetime.now(timezone.utc)
            for entry in list(RESULTS.values()):
                if entry["state"] != "delivered" or entry["results"]:
                    continue
                try:
                    deadline = datetime.fromisoformat(entry["deadline_at"])
                except (TypeError, ValueError):
                    continue
                if now < deadline:
                    continue
                entry["state"] = entry["status"] = "expired"
                entry["updated"] = stamp()
                dead.append(dict(entry))
                dead_letter_put(entry, "expired")
            expired_total += len(dead)
        for entry in dead:
            log_event("TASK_EXPIRED", entry["agent"].partition(":")[2],
                      "Server -> Agent (task abandoned)",
                      f"[{entry['msg_id']}] delivered {entry['delivered_at']}, no result ever "
                      f"arrived within {TASK_TTL_SECONDS}s")


# ----------------------------------------------------------------- auth helpers
def presented_token() -> str:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return (request.headers.get("X-Auth-Token", "").strip()
            or request.headers.get("X-Agent-Token", "").strip())


def cred_sign(agent_id: str, epoch: str) -> str:
    return hmac_mod.new(CRED_SECRET.encode(), f"{agent_id}|{epoch}".encode(),
                        hashlib.sha256).hexdigest()


def token_eq(a: str, b: str) -> bool:
    """Constant-time compare that cannot blow up on a caller: a latin-1 header value can be
    non-ASCII, and secrets.compare_digest raises TypeError on that (a free 500)."""
    return hmac_mod.compare_digest((a or "").encode("utf-8", "replace"),
                                   (b or "").encode("utf-8", "replace"))


def mint_credential(agent_id: str) -> str:
    """Rotate the epoch and hand back the fresh credential. Called on every successful connect
    and on every standby promotion, so nothing an older socket was given keeps working."""
    epoch = pysecrets.token_urlsafe(12)
    with reg_lock:
        agent_epoch[agent_id] = epoch
    return f"{agent_id}.{epoch}.{cred_sign(agent_id, epoch)}"


def credential_agent(presented: str) -> str:
    """The agent_id behind a credential, or "". Verified by recomputing the signature (no
    credential table) and then requiring the epoch to still be the live one for that id."""
    parts = (presented or "").split(".")
    if len(parts) != 3 or not all(parts):
        return ""
    agent_id, epoch, sig = parts
    if not AGENT_ID_RE.fullmatch(agent_id):
        return ""
    if not token_eq(cred_sign(agent_id, epoch), sig):
        return ""
    with reg_lock:
        live = agent_epoch.get(agent_id)
    return agent_id if live == epoch else ""


def principal() -> tuple:
    """Who this HTTP call came FROM, decided by which credential authenticated - never by
    X-Agent-Id, which any caller can type. ("operator", "") | ("agent", "<id>") | (None, None)."""
    tok = presented_token()
    if not tok:
        return None, None
    if AGENT_TOKEN and token_eq(tok, AGENT_TOKEN):
        return ("operator", "") if OPERATOR_HTTP else (None, None)
    who = credential_agent(tok)
    if who:
        return "agent", who
    return None, None


def authorized() -> bool:
    return principal()[0] is not None


def actor_label() -> str:
    kind, who = principal()
    if kind == "agent":
        return f"agent:{who}"          # from the credential; a header cannot override this
    if kind == "operator":
        label = request.headers.get("X-Agent-Id", "").strip()
        return f"operator:{label}" if AGENT_ID_RE.fullmatch(label) else "operator"
    return "client"


def own_agent() -> str:
    """The agent_id this call is scoped to: the credential's own id, or "" for an operator
    (the master token sees everything - that is what it is for)."""
    kind, who = principal()
    return who if kind == "agent" else ""


def scope_violation(tried: str, what: str):
    """403 when an agent reaches for another agent's thing. Returns None when allowed."""
    mine = own_agent()
    if not mine or mine == tried:
        return None
    log_event("SCOPE_DENY", mine, f"Agent:{mine} -> Server (refused)",
              f"asked for {what} belonging to '{tried}' - credentials are per-agent")
    return _err(403, f"this credential is '{mine}', it may not read {what} belonging to '{tried}'",
                hint="agent credentials only cover their own agent_id (v1.5). Either connect as "
                     f"'{tried}' to get its credential, or use the operator's AGENT_AUTH_TOKEN, "
                     "which is intentionally all-seeing.",
                your_agent_id=mine, asked_for=tried, what=what)


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
     "returns": {"keys": ["agents (id->sid, stable shape)", "count", "agent_ids", "detail (id->{sid, connected_at, last_seen, last_superseded_at, standby_sockets, inbox_backlog, outstanding_tasks})", "standby (id->{sid: since})", "standby_note", "last_seen_note", "task_ledger (outstanding_by_agent, stranded, dead_letter, ttl_seconds, expiry_note, endpoint)", "stranded_note"]},
     "example": "curl -s $HUB/agents -H \"Authorization: Bearer $T\" -H \"ngrok-skip-browser-warning: true\""},
    {"method": "GET", "path": "/agent-id/{agent_id}", "auth": "token",
     "summary": "Pre-flight the v1.4 uniqueness rule: is this agent_id free, who holds it, when was it last refused.",
     "returns": {"keys": ["agent_id", "available", "taken_by_sid", "connected_at", "standby_sockets", "last_rejection", "note"]},
     "errors": [400, 401],
     "example": "curl -s $HUB/agent-id/scout -H \"Authorization: Bearer $T\" -H \"ngrok-skip-browser-warning: true\""},
    {"method": "POST", "path": "/agent/{agent_id}/message", "auth": "token",
     "summary": "Route a task to one agent; returns its first reply.",
     "body": {"text": "string"}, "query": {"wait": "reply budget seconds 0-60, default ACK_TIMEOUT"},
     "returns": {"keys": ["status", "msg_id", "agent_id", "reply", "task_state", "result_endpoint", "expires_at (only when not replied)", "watch", "note", "warning"]},
     "note": "status 'replied' = first result received - with mock_agent that is the instant ACK, not completion; the real answer lands on GET /agent/{id}/inbox. Every POST opens a ledger row: follow it with GET /result/{msg_id}.",
     "errors": [400, 401, 404, 405],
     "example": "curl -s -X POST $HUB/agent/scout/message -H \"Authorization: Bearer $T\" -H \"X-Agent-Id: builder\" -H \"ngrok-skip-browser-warning: true\" -H \"Content-Type: application/json\" -d '{\"text\":\"scan the dataset\"}'"},
    {"method": "GET", "path": "/agent/{agent_id}/inbox", "auth": "token|credential",
     "summary": "Unsolicited agent->client messages. DEFAULT DRAINS AND CLEARS the queue.",
     "query": {"peek": "1/true/yes = read without clearing"},
     "returns": {"keys": ["agent_id", "messages", "count", "drained", "queue_max", "agent_online"]},
     "note": "since v1.5 an agent credential may only read its OWN inbox; the operator token reads any (that is what it is for).",
     "errors": [400, 401, 403]},
    {"method": "GET", "path": "/result/{msg_id}", "auth": "token|credential",
     "summary": "The ledger row for one task msg_id. The row opens when the hub EMITS the task, so a task nobody answered is readable, not missing.",
     "returns": {"keys": ["msg_id", "agent", "from", "task", "delivered_at", "deadline_at", "results", "count", "state (delivered|acked|answered|expired)", "status (first_result|done|answered_via_inbox)", "answered_via_inbox", "late", "caller_outcome", "waited_seconds", "hub_issued", "seconds_until_expiry", "updated", "note"]},
     "note": "results is the ordered list of every `result` the agent emitted for this msg_id, including answers that arrived after the POST already returned. 404 here means the msg_id was never issued by this process (or it restarted) - NOT that a task went unanswered. An agent credential reads only rows it owns (targeted at it, or answered by it); operators read all.",
     "errors": [401, 403, 404],
     "example": "curl -s $HUB/result/c68e84affe12 -H \"Authorization: Bearer $T\" -H \"ngrok-skip-browser-warning: true\""},
    {"method": "GET", "path": "/tasks/dead-letter", "auth": "token|credential",
     "summary": "Triage: every task the hub delivered that produced nothing (expired or evicted), newest last, plus how many tasks are still waiting per agent.",
     "query": {"limit": "1-200, default 50"},
     "returns": {"keys": ["count", "tasks", "newest_last", "states", "ledger_rows", "scoped_to", "outstanding_by_agent", "expired_total", "ttl_seconds", "note"]},
     "note": "An agent credential sees only its OWN dead rows and its own outstanding count (scoped_to names the filter); the operator token sees the whole mesh.",
     "errors": [401],
     "example": "curl -s $HUB/tasks/dead-letter -H \"Authorization: Bearer $T\" -H \"ngrok-skip-browser-warning: true\""},
    {"method": "POST", "path": "/relay", "auth": "token",
     "summary": "Deliver text to a connected agent as peer_msg (operators and agents alike).",
     "body": {"to": "agent_id", "text": "string"}, "errors": [400, 401, 404]},
    {"method": "POST", "path": "/file", "auth": "token",
     "summary": "Upload a file (<=25 MB): .json .txt .html .htm .tar.gz .tgz, ASCII names only.",
     "body": "multipart -F file=@report.json, or raw bytes + X-Filename header; a raw application/json body with no X-Filename is stored as body.json",
     "headers": {"X-Target-Agent": "optional; pushes file_ready so the agent auto-pulls it, AND records it in shared_with - since v1.5 being addressed is what grants that agent the download"},
     "query": {"dedupe": "1/true/yes = if these exact bytes are already stored, reuse that file_id (HTTP 200, nothing written) instead of minting a new one"},
     "returns": {"keys": ["status (stored|existing)", "file_id", "name", "size", "sha256", "delivered", "target_error", "duplicate_of", "deduped", "bytes_stored", "download_url"]},
     "errors": [400, 401, 413, 415]},
    {"method": "GET", "path": "/files", "auth": "token|credential", "summary": "Stored file metadata table {file_id: {...}}; survives restarts via file_store/index.json. An agent credential sees only files it uploaded or that named it in X-Target-Agent (response then carries scoped_to)."},
    {"method": "GET", "path": "/file/{file_id}", "auth": "token|credential", "summary": "Download stored bytes (as attachment).", "errors": [401, 403, 404]},
    {"method": "GET", "path": "/events/mine", "auth": "token|credential",
     "summary": "Structured event rows involving YOU (agent, source or target) - no log secret needed. With an agent credential the id comes from the credential; the operator token still passes X-Agent-Id.",
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
        "credential": ("immediately after a successful connect the hub emits `agent_token` down "
                       "your socket: your per-agent_id HTTP credential (v1.5). Send it as "
                       "X-Agent-Token; mock_agent does this automatically"),
        "id_taken": ("since v1.4 a second live socket on the same agent_id is REFUSED with a "
                     "reason string (connect_error) and logged as ID_REJECTED - ids are unique; "
                     "use another id, or force_takeover to displace deliberately"),
    },
    "hub_to_agent": {
        "agent_token": {"agent_id": "str", "token": "<agent_id>.<epoch>.<hmac> - send it as X-Agent-Token on every HTTP call", "note": "arrives right after connect (v1.5). This is what identifies you to the hub: `from` on your traffic comes from it, not from X-Agent-Id. Revoked when this socket disconnects or another process takes over your agent_id; a reactivated standby socket is re-minted one"},
        "task": {"msg_id": "str - echo it back in result", "from": "agent:<id> (credential) | operator[:<label>] | client", "text": "str"},
        "peer_msg": {"from": "str - same rule: derived from the credential that sent it", "text": "str"},
        "file_ready": {"file_id": "str", "url": "/file/<id>", "note": "plus full file meta (name, size, sha256, by, created, shared_with)"},
        "superseded": {"agent_id": "str", "reason": "str", "at": "iso", "sid": "your hub-registry sid", "sid_note": "str", "note": "sent to the OLD socket when another process registers the same agent_id; your own task/peer_msg traffic moves to the new socket from then on - and so does your HTTP credential, which stops working"},
        "reactivated": {"agent_id": "str", "reason": "str", "sid": "your hub-registry sid (compare with sio.get_sid(namespace='/agents'), NOT sio.sid)", "at": "iso", "note": "sent to a standby socket when the process that superseded it disconnects - routing of that agent_id comes back to you automatically (v1.3), followed by a freshly minted agent_token (v1.5)"},
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
    "A task is not the same as an answer. Since v1.5 every POST opens a ledger row immediately (state delivered) that becomes acked/answered when results land, or expired after TASK_TTL_SECONDS (default 900) if the agent took the task and went quiet. GET /tasks/dead-letter lists the dead ones and GET /agents shows per-agent outstanding_tasks + last_seen - that pair is how you spot a wedged agent whose socket is still open. Expired rows are kept: if the answer shows up late it still lands, tagged late=true. Caveat: only a row with NO result at all ever expires - a mock_agent-style instant ACK puts it at acked, where it can sit forever, because the hub cannot tell a task finished with one result from an agent that ACKed and wedged. Emit a second `result` for the msg_id (or agent_to_client tagged with it) to move it to answered.",
    "agent_id is unique since v1.4: a second socket connecting with a live id is REFUSED at connect (connect_error says who holds it, log row ID_REJECTED) - no more silent routing theft. Check GET /agent-id/<id> before you connect, give each process its own id (e.g. suffix the pid), and use auth force_takeover:true only when you mean to kick the current holder. A forced take-over still sends the loser `superseded`, keeps it as a standby, and hands routing back with `reactivated` if the winner dies (v1.3 chain).",
    "X-Agent-Id is only a label, never a credential: since v1.5 an agent's own HTTP calls are authenticated by its minted credential, but a caller holding the operator token can put anything in that header. Uniqueness (v1.4) stops routing collisions, not impersonation - only a credential proves who you are.",
    "Pick ONE stable X-Agent-Id per caller and keep sending it (an operator should look like `client` or a fixed `operator` id on every row). Because the header is forgeable and optional, drifting labels - `operator`, `qoder-operator`, `selftest` - make mesh attribution unreadable for the agents on the other end; the hub cannot fix that for you.",
    "The `from` on a task or peer_msg comes from the credential that authenticated (v1.5): an agent's minted credential yields `agent:<id>` and nothing else. An operator-token caller's X-Agent-Id is still a self-declared label and is not evidence of who posted it - which is exactly why those rows are written as `operator:<label>` instead of pretending to be an agent.",
    "Two principals since v1.5. An agent credential reaches only its own inbox, task rows and files (anything else is 403 with `your_agent_id`). The master AGENT_AUTH_TOKEN is the operator principal and still sees everything - deliberately, so a human can curl the hub in one shot. Run with HUB_OPERATOR_HTTP=0 to force every HTTP call, operator included, through a named credential.",
    "A credential is tied to one socket: it stops working the moment that socket disconnects, the agent_id is taken over, or the hub restarts (unless you pin HUB_CRED_SECRET). mock_agent re-mints automatically on reconnect and on `reactivated`; a hand-rolled client that cached its old token will just start seeing 401s.",
    "Hub sid fields (`superseded.sid`, `reactivated.sid`, /agents `agents[id]`) are /agents-namespace sids. python-socketio clients expose the transport sid as `sio.sid`, which will NEVER match - self-check with `sio.get_sid(namespace='/agents')`.",
    "AGENT_AUTH_TOKEN is still full access for whoever holds it - that is the operator seat. What changed in v1.5 is that agents no longer need it for HTTP: they present the credential their socket was minted, so the hub can tell operators from agents. Until every agent is on a client that does this, treat the master token as a shared root password and keep it off the wire where you can.",
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
<div class="card"><pre># 0. two principals (v1.5): AGENT_AUTH_TOKEN is the operator seat and sees everything;
#    an agent instead uses the scoped credential the hub pushes down its socket (agent_token)
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
<tr><td>Operators (and any agent that has no credential yet)</td><td><code>Authorization: Bearer &lt;AGENT_AUTH_TOKEN&gt;</code> (or <code>X-Auth-Token</code> / <code>X-Agent-Token</code> - all accepted)</td><td>every <code>/agent*</code>, <code>/file*</code>, <code>/relay</code>, <code>/agents</code>, <code>/result*</code>, <code>/tasks/*</code> - full access, labelled <code>operator[:&lt;X-Agent-Id&gt;]</code></td></tr>
<tr><td>Agents (v1.5, preferred)</td><td><code>X-Agent-Token: &lt;agent_token you were pushed&gt;</code></td><td>same calls, scoped to your own inbox / task rows / files; <code>from</code> becomes <code>agent:&lt;your id&gt;</code> and cannot be forged</td></tr>
<tr><td>Callers, to be labelled</td><td><code>X-Agent-Id: &lt;your id&gt;</code></td><td>labels an operator's messages and log rows; it does NOT authenticate and never overrides a credential</td></tr>
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
<code>401</code> missing/invalid token, or a credential whose socket died &middot; <code>400</code> bad shape/agent_id
(pattern <code>[A-Za-z0-9_-]{1,40}</code>) &middot; <code>403</code> an agent credential reaching another agent's inbox/task/file (names <code>your_agent_id</code>) &middot; <code>404</code> agent offline or unknown
file_id &middot; <code>405</code> wrong verb (valid methods listed) &middot; <code>413</code>
over 25 MB &middot; <code>415</code> filetype rejected (shows the sanitized name it checked).</div>

<h2>Read before scripting</h2>
<div class="card"><ul style="margin:0;padding-left:20px">
<!--FOOTGUNS-->
</ul></div>

<h2>Trust model</h2>
<div class="card">Two principals since v1.5.
<code>AGENT_AUTH_TOKEN</code> is the <b>operator</b> seat: any holder has full access to task
agents, drain any inbox, read the whole ledger and move any file - share it only with who you trust
to steer the whole mesh. An <b>agent</b> needs it only to connect; the hub then mints a scoped
credential and pushes it down that socket as <code>agent_token</code>, and on HTTP that credential
is the proof of who is calling: it reaches only its own inbox / task rows / files, it fixes the
<code>from</code> label (so it cannot be forged), and it dies with the socket - disconnect,
take-over or restart is revocation. <code>X-Agent-Id</code> remains a label, never an identity. Run
with <code>HUB_OPERATOR_HTTP=0</code> if you would rather the master token not work over HTTP at
all. The hub is still a switchboard: it enforces no hierarchy among agents - any agent may task any
other.</div>

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
        "Two principals since v1.5. **Operator:** `AGENT_AUTH_TOKEN` (6-50 chars), sent as"
        " `Authorization: Bearer <T>` (also accepted: `X-Auth-Token`, `X-Agent-Token`) - full"
        " access, every row it produces is labeled `operator`. **Agent:** a credential the hub"
        " mints when a socket connects and pushes down it as an `agent_token` event,"
        " `<agent_id>.<epoch>.<hmac>` - put it in `X-Agent-Token`. That credential is what"
        " proves who you are: `from` is taken from it, never from `X-Agent-Id`, and it only"
        " reaches its own inbox / task rows / files. It goes dead when the socket disconnects"
        " or another process takes over the id (the hub re-mints on both). Set"
        " `HUB_OPERATOR_HTTP=0` to make the master token socket-connect only.",
        "Missing/invalid credential => 401 JSON with a hint (HTML only if you"
        " `Accept: text/html`). Reaching another agent's thing => 403 with `your_agent_id`.",
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
        "`400` bad agent_id (pattern `[A-Za-z0-9_-]{1,40}`) or body shape · `401` token"
        " missing, wrong, or a credential whose socket already died · `403` scope"
        " violation (`your_agent_id` + `asked_for`, also logged as `SCOPE_DENY`) ·"
        " `404` agent offline (hint shows how to start one) or a msg_id this hub never"
        " issued · `405` wrong verb · `413` >25 MB · `415` rejected filetype"
        " (echoes `name_after_sanitize`) · `500` internal (no details unless hub"
        " runs `HUB_DEBUG=1`).",
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
        "`AGENT_AUTH_TOKEN` (req, operator seat) · `LOG_SECRET_TOKEN` (req) ·"
        " `HUB_PORT` (5000) · `HUB_BIND` (127.0.0.1 - the hub is local by default, publish"
        " it behind nginx or ngrok)"
        " · `ACK_TIMEOUT` (10s) · `TASK_TTL_SECONDS` (900, floor 30 - when an"
        " unanswered task goes `expired` and lands in /tasks/dead-letter) ·"
        " `HUB_CRED_SECRET` (pin it or every agent credential dies with a hub restart) ·"
        " `HUB_OPERATOR_HTTP=0` (master token becomes socket-only: every HTTP caller needs a"
        " named credential) · `NGROK_AUTHTOKEN` (optional"
        " public tunnel) · `NGROK_DOMAIN` (pin reserved domain so URL survives restarts) ·"
        " `HUB_FILE_STORE` / `HUB_LOG_FILE` (relocate state for tests) · `HUB_DEBUG=1`."
        " mock_agent honors `HUB_STATE_DIR`.",
        "",
        "Full JSON manifest: `GET /api`. Human docs: `GET /`.",
    ]
    return "\n".join(parts) + "\n"


LLMS_MD = _build_llms_md()


def onboarding_response(status: int) -> Response:
    return Response(ONBOARDING_HTML, status=status, content_type="text/html; charset=utf-8")


def require_actor():
    """Gate an HTTP endpoint behind a principal: an agent credential (minted at socket connect)
    or the operator's AGENT_AUTH_TOKEN. Returns None if allowed, else a content-negotiated 401
    (JSON for agents, onboarding HTML for browsers)."""
    if authorized():
        return None
    master_only = (not OPERATOR_HTTP
                   and token_eq(presented_token(), AGENT_TOKEN or ""))
    log_event("AUTH_FAIL", actor_label(), "Client/Agent -> Server (rejected)",
              f"{request.method} {request.path} "
              + ("operator token not accepted over HTTP (HUB_OPERATOR_HTTP=0)"
                 if master_only else "missing/invalid credential or AGENT_AUTH_TOKEN"))
    if wants_html():
        return onboarding_response(401)
    if master_only:
        return jsonify(error="the master token is socket-connect only on this hub",
                       hint="this hub runs with HUB_OPERATOR_HTTP=0: agents must present the "
                            "credential the hub emits as `agent_token` right after connect, in "
                            "X-Agent-Token. Start one: python3 mock_agent.py --server $HUB "
                            "--agent-id <id> --token $AGENT_AUTH_TOKEN (it uses the credential "
                            "automatically).",
                       docs="/llms.txt", api="/api",
                       method=request.method, path=request.path), 401
    return jsonify(error="missing or invalid credential",
                   hint='agents: send the X-Agent-Token credential the hub emitted at connect. '
                        'operators: -H "Authorization: Bearer <AGENT_AUTH_TOKEN>" (X-Auth-Token / '
                        'X-Agent-Token also accepted). A credential stops working when its socket '
                        'disconnects or its agent_id is taken over - reconnect to get a fresh one. '
                        'Through ngrok free tier also add -H "ngrok-skip-browser-warning: true"',
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
                   auth={"env_token": "AGENT_AUTH_TOKEN",
                         "how": "Authorization: Bearer <T> | X-Auth-Token | X-Agent-Token",
                         "principals": {
                             "operator": "the AGENT_AUTH_TOKEN itself - full read/write, labels "
                                         "rows 'operator' (or 'operator:<X-Agent-Id>'), and can be "
                                         "turned off over HTTP with HUB_OPERATOR_HTTP=0",
                             "agent": "a credential minted at socket connect and pushed down that "
                                      "socket as `agent_token`: <agent_id>.<epoch>."
                                      "<HMAC-sha256>. Sent in X-Agent-Token. Proves who the caller "
                                      "is, scopes /result /files and inboxes to that agent_id, and "
                                      "is revoked when the socket dies or the id is taken over"},
                         "model": "since v1.5 identity comes from the credential that "
                                  "authenticated, never from X-Agent-Id; a master-token caller's "
                                  "X-Agent-Id is still a self-declared label"},
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
        seen = dict(agent_last_seen)
        std = {aid: dict(s) for aid, s in standby.items() if s}
    with meta_lock:
        backlog = {aid: len(q) for aid, q in client_inboxes.items()}
        outstanding: dict = {}
        for entry in RESULTS.values():
            if entry["state"] in ("delivered", "acked"):
                aid = entry["agent"].partition(":")[2]
                outstanding[aid] = outstanding.get(aid, 0) + 1
        dead = len(DEAD_LETTER)
    return jsonify(agents=snap,                       # frozen shape: {id: sid}
                   count=len(snap), agent_ids=sorted(snap),
                   detail={aid: {"sid": sid, "connected_at": since.get(aid),
                                 "last_seen": seen.get(aid),
                                 "last_superseded_at": sup.get(aid),
                                 "standby_sockets": len(std.get(aid) or {}),
                                 "inbox_backlog": backlog.get(aid, 0),
                                 "outstanding_tasks": outstanding.get(aid, 0)}
                           for aid, sid in snap.items()},
                   standby=std,
                   standby_note="still-open sockets that lost an agent_id take-over; the most "
                                "recent one reclaims routing when the holder disconnects",
                   last_seen_note="last traffic the hub saw FROM that agent's socket (connect, "
                                  "result, agent_to_client or agent_to_agent). Being connected and "
                                  "having a stale last_seen with outstanding_tasks > 0 is the "
                                  "wedged-agent signature - the socket is up, the work is not.",
                   task_ledger={"outstanding_by_agent": outstanding,
                                "stranded": {aid: n for aid, n in outstanding.items()
                                             if aid not in snap},
                                "dead_letter": dead, "ttl_seconds": TASK_TTL_SECONDS,
                                "expiry_note": "outstanding counts rows in state delivered or "
                                               "acked. Only a delivered row that never got a "
                                               "single result goes expired (and dead-lettered) at "
                                               "ttl_seconds; an acked row can sit there forever, "
                                               "because the hub cannot tell a task the agent "
                                               "finished with one result from one that only "
                                               "auto-ACKed and then wedged. Read last_seen next to "
                                               "outstanding_tasks, not the ttl, for that case.",
                                "endpoint": "/tasks/dead-letter"},
                   stranded_note="tasks waiting on an agent that is not connected right now; one "
                                 "with no result yet goes expired at ttl_seconds, an already-acked "
                                 "one keeps its row until it is answered")


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
    caller = actor_label()
    # Ledger first, then the socket: a task the agent swallows whole is still on the record.
    ledger_open(msg_id, agent_id, caller, text)

    socketio.emit("task", {"msg_id": msg_id, "from": caller, "text": text},
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
        row = RESULTS.get(msg_id)
        if row is not None:
            row["caller_outcome"] = status
            row["waited_seconds"] = wait
        state = (row or {}).get("state", "delivered")
        deadline = (row or {}).get("deadline_at")
    out = {"status": status, "msg_id": msg_id, "agent_id": agent_id, "reply": reply,
           "task_state": state, "result_endpoint": f"/result/{msg_id}"}
    if status == "replied":
        out["note"] = ("first result only - mock_agent auto-ACKs here; the agent's real answer "
                       "arrives later on GET /agent/%s/inbox?peek=true" % agent_id)
    else:
        out["expires_at"] = deadline
        out["watch"] = (f"GET /result/{msg_id} tracks this task: state goes delivered -> acked "
                        f"-> answered, or -> expired after {TASK_TTL_SECONDS}s with nothing "
                        f"to show for it; abandoned tasks overall: GET /tasks/dead-letter")
    if warning:
        out["warning"] = warning
    return jsonify(out)


@app.get("/agent/<agent_id>/inbox")
def agent_inbox(agent_id):
    if deny := require_actor():
        return deny
    if not AGENT_ID_RE.fullmatch(agent_id):
        return _err(400, "invalid agent_id",
                    hint="agent_id must match [A-Za-z0-9_-]{1,40}",
                    pattern="[A-Za-z0-9_-]{1,40}", agent_id=agent_id)
    if deny := scope_violation(agent_id, "an inbox"):
        return deny                          # draining someone else's queue is not an agent right
    peek = request.args.get("peek", "").lower() in ("1", "true", "yes")
    with meta_lock:
        q = client_inboxes.get(agent_id)      # never client_inboxes[...]: an unknown id must
        msgs = list(q) if q else []           # not allocate a queue that outlives the request
        existed = q is not None
        if q and not peek:
            q.clear()
    with reg_lock:
        online = agent_id in agents
    return jsonify(agent_id=agent_id, messages=msgs, count=len(msgs),
                   peek=peek, drained=(not peek) and existed,
                   queue_max=INBOX_QUEUE_MAX, agent_online=online)


@app.get("/result/<msg_id>")
def task_result(msg_id):
    """Correlation for two-phase replies: everything the hub recorded for a msg_id.
    A row exists from the moment the task is emitted, so 'nothing happened yet' and
    'this msg_id was never issued' are different, separately stated answers."""
    if deny := require_actor():
        return deny
    with meta_lock:
        r = RESULTS.get(msg_id)
        out = dict(r) if r else None
    if out is None:
        return _err(404, "unknown msg_id",
                    hint="this hub never issued that msg_id, or it restarted (the ledger lives "
                         "in memory) or the row aged out of the last %d tasks. A task that WAS "
                         "issued keeps its row here forever, going state 'expired' when nobody "
                         "answers it - see GET /tasks/dead-letter for all of them at once."
                         % TASK_LEDGER_MAX,
                    msg_id=msg_id, window=TASK_LEDGER_MAX, ttl_seconds=TASK_TTL_SECONDS,
                    dead_letter="/tasks/dead-letter")
    owner = out.get("agent", "").partition(":")[2]
    mine = own_agent()
    if mine and mine not in (owner, (out.get("answered_by") or "").partition(":")[2]):
        return scope_violation(owner or "?", "a task ledger row")
    out["status"] = "done" if len(out.get("results", [])) > 1 else out.get("status", "first_result")
    out["count"] = len(out.get("results", []))
    if out.get("state") == "delivered":
        try:
            out["seconds_until_expiry"] = max(
                0, int((datetime.fromisoformat(out["deadline_at"])
                        - datetime.now(timezone.utc)).total_seconds()))
        except (KeyError, TypeError, ValueError):
            pass
    out["note"] = ("state: delivered = emitted, no result yet | acked = one `result` seen | "
                   "answered = more than one, or the agent replied via agent_to_client | "
                   "expired = the deadline passed with no result at all (the agent took the task "
                   "and went quiet; an answer arriving after that still lands here, tagged "
                   "late=true). status is the legacy field: first_result|done|answered_via_inbox.")
    return jsonify(out)


@app.get("/tasks/dead-letter")
def tasks_dead_letter():
    """Triage view: every task the hub delivered that produced nothing, why it is dead,
    plus a live count of the tasks still waiting."""
    if deny := require_actor():
        return deny
    with meta_lock:
        rows = list(DEAD_LETTER)
        states: dict = {}
        outstanding: dict = {}
        for entry in RESULTS.values():
            states[entry["state"]] = states.get(entry["state"], 0) + 1
            if entry["state"] in ("delivered", "acked"):
                aid = entry["agent"].partition(":")[2]
                outstanding[aid] = outstanding.get(aid, 0) + 1
        ledger_rows = len(RESULTS)
    mine = own_agent()
    if mine:            # an agent credential sees its own dead tasks, not the whole mesh's
        rows = [r for r in rows if r.get("agent") == f"agent:{mine}"]
        states = {}
        for r in rows:
            states[r["state"]] = states.get(r["state"], 0) + 1
        outstanding = {mine: outstanding[mine]} if mine in outstanding else {}
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), DEAD_LETTER_MAX))
    except ValueError:
        limit = 50
    dead_note = (f"reason: expired = no result within ttl_seconds | evicted = the row was pushed "
                 f"out of the {TASK_LEDGER_MAX}-task ledger before anything answered. A growing "
                 f"outstanding_by_agent entry next to a connected agent is the wedged-agent "
                 f"signature: the socket is up, the work is not.")
    if mine:
        dead_note += (f" Scoped to your agent '{mine}': states and expired_total describe only "
                      f"your rows; the operator token sees every agent's rows.")
    return jsonify(count=len(rows), tasks=rows[-limit:], newest_last=True,
                   states=states, ledger_rows=ledger_rows, scoped_to=mine or "all agents",
                   outstanding_by_agent=outstanding,
                   expired_total=(sum(1 for r in rows if r.get("reason") == "expired")
                                  if mine else expired_total),
                   ttl_seconds=TASK_TTL_SECONDS, note=dead_note)


@app.get("/events/mine")
def events_mine():
    """Event rows that involve the caller - agents hold the full-access token but the
    human log page needs a separate secret they usually don't have."""
    if deny := require_actor():
        return deny
    # a credential decides whose feed this is; the header is only believed for the operator token
    aid = own_agent() or request.headers.get("X-Agent-Id", "").strip()
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


def _record_shared(file_id: str, target: str) -> None:
    """Dedupe path: reusing older bytes must still grant the new recipient a download."""
    if not AGENT_ID_RE.fullmatch(target or ""):
        return
    with meta_lock:
        m = file_meta.get(file_id)
        if m is not None:
            shared = m.setdefault("shared_with", [])
            if target not in shared:
                shared.append(target)
            persist_file_index()


@app.post("/file")
def upload_file():
    if deny := require_actor():
        return deny
    sender = actor_label()
    if request.files.get("file"):
        fs = request.files["file"]
        raw_name = fs.filename or "file.bin"
        data = fs.stream.read(MAX_UPLOAD + 1)
        ctype = fs.mimetype or ""
        if not ctype or ctype == "application/octet-stream":
            # most clients post the part without a Content-Type; the extension is the
            # only type signal there is, and it is validated below anyway
            ctype = mimetypes.guess_type(raw_name)[0] or "application/octet-stream"
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
    target = request.headers.get("X-Target-Agent", "").strip()
    meta = {"name": name, "type": ctype, "size": len(data), "sha256": sha, "by": sender,
            "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "created_iso": now_dt.isoformat(timespec="seconds"),
            "created_epoch": int(now_dt.timestamp())}
    if AGENT_ID_RE.fullmatch(target):
        # v1.5 scoping: an upload is readable by whoever stored it and whoever it was addressed
        # to. Addressing is the sharing event, so X-Target-Agent is not just a notification.
        meta["shared_with"] = [target]

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

    if reused:
        delivered, target_error = _notify_target(dup, reused, target, sender)
        _record_shared(dup, target)
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


def _file_visible(meta: dict, mine: str) -> bool:
    """Agents see files they stored or that were addressed to them; operators see everything."""
    if not mine:
        return True
    return meta.get("by") == f"agent:{mine}" or mine in (meta.get("shared_with") or [])


@app.get("/files")
def list_files():
    if deny := require_actor():
        return deny
    mine = own_agent()
    with meta_lock:
        rows = dict(file_meta)
    if mine:
        rows = {fid: m for fid, m in rows.items() if _file_visible(m, mine)}
        return jsonify(files=rows, count=len(rows), scoped_to=mine,
                       scope_note="an agent credential lists only what it uploaded or what was "
                                  "addressed to it via X-Target-Agent; the operator token lists "
                                  "the whole store")
    return jsonify(files=rows, count=len(rows))


@app.get("/file/<file_id>")
def download_file(file_id):
    if deny := require_actor():
        return deny
    with meta_lock:
        meta = file_meta.get(file_id)
    if not meta:
        return _err(404, "unknown file_id", hint="list what exists: GET /files", file_id=file_id)
    mine = own_agent()
    if mine and not _file_visible(meta, mine):
        return scope_violation(str(meta.get("by") or "?").partition(":")[2] or "?",
                               "a stored file")
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
    if not (AGENT_TOKEN and token_eq(str(token), AGENT_TOKEN)):
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
        agent_last_seen[agent_id] = agent_since[agent_id]
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
    # v1.5: hand this socket its own credential. Minting here also revokes whatever the socket
    # it replaced (if any) was holding, because there is exactly one live epoch per agent_id.
    deliver_credential(agent_id, request.sid)


def deliver_credential(agent_id: str, sid: str) -> str:
    """Mint a fresh credential for this agent_id and push it down the given socket."""
    cred = mint_credential(agent_id)
    try:
        socketio.emit("agent_token",
                      {"agent_id": agent_id, "token": cred,
                       "note": "send this as X-Agent-Token on HTTP. It proves who you are, so "
                               "`from` is taken from it rather than from your X-Agent-Id header. "
                               "It dies when this socket disconnects or when another process "
                               "takes over this agent_id."},
                      to=sid, namespace=NS)
        log_event("CRED_MINTED", agent_id, "Server -> Agent (credential pushed)",
                  f"agent-scoped HTTP credential for '{agent_id}' sent to socket {sid} "
                  f"(value never logged; it rotates with this socket)")
    except Exception as exc:  # noqa: BLE001 - a socket that cannot hear it is not worth a 500
        log_event("CRED_FAIL", agent_id, "Server -> Agent (credential push failed)",
                  f"{type(exc).__name__}: {exc} - this agent has no credential and will keep "
                  f"using the operator token for HTTP")
    return cred


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
                agent_epoch.pop(agent_id, None)   # nobody routes this id: its credential is dead
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
        # its old credential was revoked the moment the winner took the id, so re-mint now
        # that this socket routes the agent_id again
        deliver_credential(agent_id, promote)
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
                # the agent answered under a msg_id this hub never issued (self-invented id, or a
                # hub restart dropped the ledger) - record it rather than drop the reply
                while len(RESULTS) >= TASK_LEDGER_MAX:
                    _, evicted = RESULTS.popitem(last=False)
                    if not evicted["results"]:
                        dead_letter_put(evicted, "evicted")
                r = RESULTS[msg_id] = {"agent": f"agent:{agent_id}", "msg_id": msg_id,
                                       "hub_issued": False, "results": [], "updated": None,
                                       "status": "first_result", "state": "acked"}
            r["results"].append({"from": f"agent:{agent_id}", "text": text})
            r["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            late = r["state"] == "expired"
            r["state"] = "answered" if len(r["results"]) > 1 else "acked"
            r["status"] = "done" if len(r["results"]) > 1 else "first_result"
            if late:
                r["late"] = True          # arrived after the reaper had already called it dead
            if r.get("agent") != f"agent:{agent_id}":
                r["answered_by"] = f"agent:{agent_id}"   # someone else owns the target id
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
    note_agent_traffic(agent_id)


@socketio.on("agent_to_client", namespace=NS)
def on_agent_to_client(data=None):
    agent_id = sid_to_agent.get(request.sid)
    if agent_id is None:
        return
    msg_id = (data or {}).get("msg_id")
    text = (data or {}).get("text", "")
    at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with meta_lock:   # every reader of client_inboxes copies it with list(); appends must be locked
        client_inboxes[agent_id].append({"from": f"agent:{agent_id}", "ts": at,
                                         "msg_id": msg_id, "text": text})
        if msg_id:  # agents that answer over this channel never emit `result`, so tag the task
            r = RESULTS.get(str(msg_id))
            if r is not None:
                r["answered_via_inbox"] = True
                r["updated"] = at
                r["status"] = "answered_via_inbox"
                if r["state"] != "expired":
                    r["state"] = "answered"
    log_event("MSG_RCVD", agent_id, "Agent -> Server -> Client (queued)", eprint_summary(text))
    note_agent_traffic(agent_id)


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
    note_agent_traffic(sender)
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
              f"hub v{HUB_VERSION} started on {HUB_BIND}:{HUB_PORT} (threading mode), "
              f"task ttl {TASK_TTL_SECONDS}s")
    threading.Thread(target=reap_expired_tasks, daemon=True, name="task-reaper").start()
    print(f"[agent-hub] v{HUB_VERSION} listening on {HUB_BIND}:{HUB_PORT} | agents socket ns={NS} | "
          f"hub token len={len(AGENT_TOKEN)} | docs = /llms.txt")
    announce_log_urls()
    threading.Thread(target=open_tunnel, daemon=True).start()
    socketio.run(app, host=HUB_BIND, port=HUB_PORT, debug=False, allow_unsafe_werkzeug=True)
