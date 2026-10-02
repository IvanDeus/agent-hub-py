#!/usr/bin/env python3
"""Agent Hub - Flask + Flask-SocketIO orchestrator that routes messages and
files between HTTP clients and AI agents stuck behind NAT (persistent
outbound socket). Run:  NAUTH=... AGENT_AUTH_TOKEN=... LOG_SECRET_TOKEN=... python3 app.py
Optional: NGROK_AUTHTOKEN=... opens a public HTTPS tunnel from inside this app;
without it the hub warns and serves localhost only."""

import hashlib
import html as html_mod
import json
import os
import re
import secrets as pysecrets
import threading
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path

from flask import Flask, Response, abort, jsonify, request, send_file
from flask_socketio import SocketIO
from werkzeug.utils import secure_filename

# ----------------------------------------------------------------- config
BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "logs.html"
FILE_STORE = BASE_DIR / "file_store"
FILE_STORE.mkdir(exist_ok=True)
FAVICON_FILE = BASE_DIR / "favicon.ico"
FAVICON_ROUTE = "/favicon.ico"

NAUTH = os.environ.get("NAUTH", "")                      # master client token
AGENT_TOKEN = os.environ.get("AGENT_AUTH_TOKEN", "")     # agents' public token (6-50 chars)
LOG_SECRET = os.environ.get("LOG_SECRET_TOKEN", "")      # secret path segment for /logs/<token>
HUB_PORT = int(os.environ.get("HUB_PORT", "5000"))
ACK_TIMEOUT = float(os.environ.get("ACK_TIMEOUT", "10"))
NGROK_AUTHTOKEN = os.environ.get("NGROK_AUTHTOKEN", "")  # optional: public tunnel, else localhost only

ALLOWED_EXT = (".json", ".txt", ".html", ".htm", ".tar.gz", ".tgz")
MAX_UPLOAD = 25 * 1024 * 1024
AGENT_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,40}$")

if not NAUTH:
    raise SystemExit("[agent-hub] FATAL: NAUTH env var (master client token) is not set.")
if not (6 <= len(AGENT_TOKEN) <= 50):
    raise SystemExit("[agent-hub] FATAL: AGENT_AUTH_TOKEN env var must be a 6-50 char string.")
if not (4 <= len(LOG_SECRET) <= 64) or not re.fullmatch(r"[A-Za-z0-9_\-]+", LOG_SECRET):
    raise SystemExit("[agent-hub] FATAL: LOG_SECRET_TOKEN must be 4-64 URL-safe chars.")

NS = "/agents"

app = Flask(__name__)
app.config["SECRET_KEY"] = pysecrets.token_hex(16)
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

agents = {}                # agent_id -> sid
sid_to_agent = {}          # sid -> agent_id
reg_lock = threading.Lock()
client_inboxes = defaultdict(lambda: deque(maxlen=200))   # agent_id -> agent->client msgs
file_meta = {}             # file_id -> {name, path, type, size, sha256, by, created}
pending = {}               # msg_id -> {"event": Event, "replies": []}
meta_lock = threading.Lock()

# ----------------------------------------------------------------- logging
LOG_ROWS = deque(maxlen=3000)
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
.aid{font-weight:700}.dir{color:var(--muted);font-size:12px}.pl{flex:1;min-width:220px;word-break:break-word;color:var(--fg)}
"""


def _render_log() -> str:
    rows = "".join(LOG_ROWS)
    return ("<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<meta http-equiv='refresh' content='5'>"
            "<title>Agent Hub - Event Log</title>"
            f"<link rel='icon' href='{FAVICON_ROUTE}'>"
            f"<style>{LOG_CSS}</style></head><body>"
            "<header><h1>Agent Hub - Event Log</h1>"
            "<span class='sub'>auto-refresh 5s &middot; append-only &middot; "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span></header>"
            f"<div id='log'>{rows}</div></body></html>")


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
        LOG_FILE.write_text(_render_log(), encoding="utf-8")


def init_log_file() -> None:
    LOG_FILE.write_text(_render_log(), encoding="utf-8")


def eprint_summary(data) -> str:
    if isinstance(data, dict):
        text = data.get("text") or json.dumps(data, default=str)
    else:
        text = str(data)
    return text


# ----------------------------------------------------------------- auth helpers
def _bearer() -> str:
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return request.headers.get("X-Auth-Token", "").strip()


def client_authorized() -> bool:
    return bool(NAUTH) and pysecrets.compare_digest(_bearer(), NAUTH)


def agent_authorized() -> bool:
    return bool(AGENT_TOKEN) and pysecrets.compare_digest(request.headers.get("X-Agent-Token", "").strip(), AGENT_TOKEN)


def actor_label() -> str:
    aid = request.headers.get("X-Agent-Id", "").strip()
    if agent_authorized() and AGENT_ID_RE.fullmatch(aid or ""):
        return f"agent:{aid}"
    return "client"


# ----------------------------------------------------------------- onboarding page
ONBOARDING = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>Agent Hub - Orchestrator</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="/favicon.ico">
<style>
:root{--bg:#f5f5f3;--fg:#1c1c1e;--card:#fff;--line:#e3e3df;--muted:#6b6b6f;--acc:#1d4ed8}
@media(prefers-color-scheme:dark){:root{--bg:#111214;--fg:#e7e7ea;--card:#1a1c1f;--line:#2a2d31;--muted:#8b8f96;--acc:#60a5fa}}
body{margin:0;background:var(--bg);color:var(--fg);font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:14px;line-height:1.55}
main{max-width:780px;margin:0 auto;padding:28px 18px 60px}
h1{font-size:20px;margin:0 0 4px}
h2{font-size:15px;margin:26px 0 8px;border-bottom:1px solid var(--line);padding-bottom:6px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px;margin:10px 0}
code,pre{background:rgba(127,127,127,.12);border-radius:6px;padding:1px 5px;font-size:12.5px}
pre{padding:10px 12px;overflow:auto}
.k{color:var(--acc);font-weight:700}
.mut{color:var(--muted);font-size:12.5px}
table{width:100%;border-collapse:collapse;font-size:12.5px}
td,th{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);vertical-align:top}
</style></head><body><main>
<h1>Agent Hub <span class="mut">/ NAT orchestrator</span></h1>
<p>This central Flask server keeps
<span class="k">persistent outbound connections</span> open for AI agents trapped behind NAT.
Clients POST tasks here; the hub routes them down the agent's socket by <code>agent_id</code>
and streams replies and files back. No agent needs an inbound port. Set
<code>NGROK_AUTHTOKEN</code> and the hub opens its own public HTTPS tunnel; without it it
stays on localhost and just warns.</p>

<h2>How it works</h2>
<div class="card"><ol style="margin:0;padding-left:20px">
<li>Agent opens a Socket.IO connection to <code>/agents</code> with the shared <code>AGENT_AUTH_TOKEN</code>.</li>
<li>Client POSTs <code>/agent/&lt;agent_id&gt;/message</code> with the master token in <code>Authorization: Bearer $NAUTH</code>.</li>
<li>Hub pushes the task down the socket, waits for the agent's reply (bounded - never hangs), returns it to the client.</li>
<li>Agents talk to each other via <code>/relay</code> or socket <code>agent_to_agent</code>; files (JSON / TXT / TAR.GZ / HTML) via <code>/file</code>.</li>
</ol></div>

<h2>Required headers</h2>
<table>
<tr><th>Who</th><th>Header(s)</th><th>Used on</th></tr>
<tr><td>HTTP clients / operators</td><td><code>Authorization: Bearer &lt;NAUTH&gt;</code> (or <code>X-Auth-Token</code>)</td><td>all /agent*, /file*, /relay, /logs</td></tr>
<tr><td>Agents (HTTP fallback)</td><td><code>X-Agent-Token: &lt;AGENT_AUTH_TOKEN&gt;</code> + <code>X-Agent-Id: &lt;your id&gt;</code></td><td>/relay, /file, /agents</td></tr>
<tr><td>Everyone through ngrok free tier</td><td><code>ngrok-skip-browser-warning: true</code></td><td>every request - otherwise ngrok shows an interstitial page first</td></tr>
</table>

<h2>Quick start</h2>
<pre>export NAUTH=your-master-token          # 1+ chars, client auth
export AGENT_AUTH_TOKEN=agentpub12      # 6-50 chars, agent socket auth
export LOG_SECRET_TOKEN=oplogs77x       # secret path for the log viewer
export NGROK_AUTHTOKEN=&lt;your-authtoken&gt;   # optional: opens the public tunnel in-app
python3 app.py                          # listens on :5000 (+ tunnel when the token is set)

# no NGROK_AUTHTOKEN? the hub warns and stays on http://localhost:5000 only

# client sends a task:
curl -s -X POST $HUB/agent/scout/message \\
  -H "Authorization: Bearer $NAUTH" \\
  -H "ngrok-skip-browser-warning: true" \\
  -H "Content-Type: application/json" -d '{"text": "scan the dataset"}'

# agent joins (behind NAT, outbound only):
python3 mock_agent.py --server $HUB --agent-id scout --token "$AGENT_AUTH_TOKEN"</pre>

<h2>Endpoints</h2>
<table>
<tr><th>Method / Path</th><th>Auth</th><th>Purpose</th></tr>
<tr><td><code>GET /</code></td><td>none</td><td>this onboarding page</td></tr>
<tr><td><code>GET /agents</code></td><td>NAUTH or agent</td><td>connected agent registry</td></tr>
<tr><td><code>POST /agent/&lt;id&gt;/message</code></td><td>NAUTH</td><td>route a task to one agent, returns its reply</td></tr>
<tr><td><code>GET /agent/&lt;id&gt;/inbox</code></td><td>NAUTH</td><td>unsolicited agent-&gt;client messages (queued)</td></tr>
<tr><td><code>POST /relay</code></td><td>agent / NAUTH</td><td>body {"to": id, "text": ...} - hub-&gt;agent delivery</td></tr>
<tr><td><code>POST /file</code></td><td>agent / NAUTH</td><td>upload JSON/TXT/TAR.GZ/HTML; header <code>X-Target-Agent</code> pushes a notify</td></tr>
<tr><td><code>GET /file/&lt;file_id&gt;</code></td><td>agent / NAUTH</td><td>download a stored file</td></tr>
<tr><td><code>GET /files</code></td><td>agent / NAUTH</td><td>list stored file metadata</td></tr>
<tr><td><code>GET /logs/&lt;LOG_SECRET_TOKEN&gt;</code></td><td>secret path</td><td>auto-refreshing HTML event log (404 otherwise)</td></tr>
<tr><td><code>GET /favicon.ico</code></td><td>none</td><td>hub icon, linked from every HTML page</td></tr>
<tr><td><code>GET /health</code></td><td>none</td><td>liveness for ngrok / monitors</td></tr>
</table>
<p class="mut">Requests missing a valid token return this page with HTTP 401. Socket connections
without the agent token are rejected and logged as AUTH_FAIL.</p>
</main></body></html>"""


def onboarding_response(status: int) -> Response:
    return Response(ONBOARDING, status=status, content_type="text/html; charset=utf-8")


def require_client():
    """Gate an HTTP endpoint behind the master NAUTH token."""
    if not client_authorized():
        log_event("AUTH_FAIL", actor_label(), "Client -> Server (rejected)",
                  f"{request.method} {request.path} missing/invalid NAUTH")
        return onboarding_response(401)
    return None


def require_actor():
    """Gate an endpoint behind NAUTH *or* the agent token (agent operators)."""
    if client_authorized() or agent_authorized():
        return None
    log_event("AUTH_FAIL", actor_label(), "Client/Agent -> Server (rejected)",
              f"{request.method} {request.path} missing/invalid NAUTH or X-Agent-Token")
    return onboarding_response(401)


# ----------------------------------------------------------------- HTTP routes
@app.get("/")
def root():
    return onboarding_response(200)


@app.get(FAVICON_ROUTE)
def favicon():
    """Unauthenticated: browsers fetch it before any token could apply."""
    if not FAVICON_FILE.exists():
        abort(404)
    return send_file(FAVICON_FILE, mimetype="image/x-icon", max_age=86400)


@app.get("/health")
def health():
    with reg_lock:
        return jsonify(status="ok", agents_connected=len(agents), uptime="see logs")


@app.get("/agents")
def list_agents():
    if deny := require_actor():
        return deny
    with reg_lock:
        return jsonify({"agents": dict(agents)})


@app.post("/agent/<agent_id>/message")
def send_to_agent(agent_id):
    if deny := require_client():
        return deny
    if not AGENT_ID_RE.fullmatch(agent_id):
        return jsonify(error="invalid agent_id"), 400
    with reg_lock:
        sid = agents.get(agent_id)
    if sid is None:
        log_event("MSG_FAIL", agent_id, "Client -> Server (agent offline)",
                  "no persistent socket; request refused instantly")
        return jsonify(error=f"agent '{agent_id}' is offline", agent_id=agent_id), 404

    body = request.get_json(silent=True) or {}
    text = body.get("text") if isinstance(body, dict) else None
    if text is None:
        text = request.get_data(as_text=True) or json.dumps(body)
    msg_id = pysecrets.token_hex(6)
    pentry = {"event": threading.Event(), "replies": []}
    with meta_lock:
        pending[msg_id] = pentry

    socketio.emit("task", {"msg_id": msg_id, "from": actor_label(), "text": text},
                  to=sid, namespace=NS)
    log_event("MSG_SENT", agent_id, "Client -> Server -> Agent", eprint_summary(text))

    try:
        wait = min(max(float(request.args.get("wait", ACK_TIMEOUT)), 0), 60)
    except ValueError:
        wait = ACK_TIMEOUT
    if wait > 0 and pentry["event"].wait(timeout=wait):
        reply = pentry["replies"][-1] if pentry["replies"] else None
        log_event("MSG_RCVD", agent_id, "Agent -> Server -> Client", eprint_summary(reply))
        status = "replied"
    else:
        reply = None
        status = "delivered_no_ack"
    with meta_lock:
        pending.pop(msg_id, None)
    return jsonify(status=status, msg_id=msg_id, agent_id=agent_id, reply=reply)


@app.get("/agent/<agent_id>/inbox")
def agent_inbox(agent_id):
    if deny := require_client():
        return deny
    with meta_lock:
        q = client_inboxes.get(agent_id)
        msgs = list(q) if q else []
        if q:
            q.clear()
    return jsonify({"agent_id": agent_id, "messages": msgs, "count": len(msgs)})


@app.post("/relay")
def relay():
    if deny := require_actor():
        return deny
    data = request.get_json(silent=True) or {}
    to = str(data.get("to", ""))
    text = data.get("text", "")
    sender = actor_label()
    if not AGENT_ID_RE.fullmatch(to) or not text:
        return jsonify(error="body needs {to: agent_id, text: string}"), 400
    with reg_lock:
        sid = agents.get(to)
    if sid is None:
        log_event("MSG_FAIL", to, f"{sender} -> Server (offline)", "relay refused instantly")
        return jsonify(error=f"target agent '{to}' is offline", to=to), 404
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
        raw_name = request.headers.get("X-Filename", "file.bin")
        data = request.get_data()
        ctype = request.headers.get("Content-Type", "application/octet-stream")
    name = _safe_name(raw_name)
    if not _ext_ok(name):
        return jsonify(error="filetype not allowed", allowed=list(ALLOWED_EXT)), 415
    if not data:
        return jsonify(error="empty body - no file bytes"), 400
    if len(data) > MAX_UPLOAD:
        return jsonify(error=f"too large (max {MAX_UPLOAD} bytes)"), 413

    file_id = pysecrets.token_hex(8)
    path = FILE_STORE / f"{file_id}__{name}"
    path.write_bytes(data)
    meta = {"name": name, "type": ctype, "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(), "by": sender,
            "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    with meta_lock:
        file_meta[file_id] = meta

    target = request.headers.get("X-Target-Agent", "").strip()
    delivered = False
    if target and AGENT_ID_RE.fullmatch(target):
        with reg_lock:
            sid = agents.get(target)
        if sid:
            socketio.emit("file_ready", dict(file_id=file_id, url=f"/file/{file_id}", **meta),
                          to=sid, namespace=NS)
            delivered = True
        else:
            log_event("MSG_FAIL", target, f"{sender} -> Server (offline)",
                      f"file {name} stored; no socket to notify")
    log_event("FILE_SENT", target or "-", f"{sender} -> Server -> File store",
              f"{name} ({len(data)} B, sha={meta['sha256'][:12]}, pushed={delivered})")
    return jsonify(status="stored", file_id=file_id, name=name, size=len(data),
                   delivered_to=target if target else None, download_url=f"/file/{file_id}"), 201


@app.get("/files")
def list_files():
    if deny := require_actor():
        return deny
    with meta_lock:
        return jsonify({"files": dict(file_meta)})


@app.get("/file/<file_id>")
def download_file(file_id):
    if deny := require_actor():
        return deny
    with meta_lock:
        meta = file_meta.get(file_id)
    if not meta:
        abort(404)
    path = FILE_STORE / f"{file_id}__{meta['name']}"
    if not path.exists():
        abort(404)
    log_event("FILE_RCVD", actor_label(), "File store -> Server -> Client/Agent",
              f"downloaded {meta['name']} ({meta['size']} B)")
    return send_file(path, as_attachment=True, download_name=meta["name"],
                     mimetype=meta["type"])


# ----------------------------------------------------------------- log viewer
@app.get("/logs")
def logs_no_token():
    abort(404)


@app.get("/logs/<token>")
def logs_view(token):
    if not (len(token) <= 64 and pysecrets.compare_digest(token, LOG_SECRET)):
        log_event("AUTH_FAIL", "-", "Client -> Server (log 404)", f"invalid log token: {token[:16]!r}")
        abort(404)
    if not LOG_FILE.exists():
        init_log_file()
    return Response(LOG_FILE.read_text(encoding="utf-8"), content_type="text/html; charset=utf-8")


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
    with reg_lock:
        old = agents.get(agent_id)
        agents[agent_id] = request.sid
        sid_to_agent[request.sid] = agent_id
    if old:
        log_event("DISCONNECTED", agent_id, "Agent -> Server (replaced)",
                  "stale socket replaced by fresh connection")
    log_event("CONNECTED", agent_id, "Agent -> Server (socket open)",
              f"sid={request.sid[:12]} transport={getattr(request, 'transport', '?')}")


@socketio.on("disconnect", namespace=NS)
def on_disconnect():
    with reg_lock:
        agent_id = sid_to_agent.pop(request.sid, None)
        if agent_id and agents.get(agent_id) == request.sid:
            agents.pop(agent_id, None)
    if agent_id:
        log_event("DISCONNECTED", agent_id, "Agent -> Server (socket closed)",
                  "persistent connection dropped; future requests to this agent return 404")


@socketio.on("result", namespace=NS)
def on_result(data=None):
    agent_id = sid_to_agent.get(request.sid, "?")
    msg_id = str((data or {}).get("msg_id", ""))
    text = (data or {}).get("text", "")
    log_event("MSG_RCVD", agent_id, "Agent -> Server -> Client",
              f"[{msg_id}] {eprint_summary(text)}")
    with meta_lock:
        p = pending.get(msg_id)
        if p:
            p["replies"].append({"from": f"agent:{agent_id}", "text": text})
            p["event"].set()
        elif agent_id != "?":
            client_inboxes[agent_id].append({"from": f"agent:{agent_id}", "msg_id": msg_id, "text": text})


@socketio.on("agent_to_client", namespace=NS)
def on_agent_to_client(data=None):
    agent_id = sid_to_agent.get(request.sid, "?")
    text = (data or {}).get("text", "")
    client_inboxes[agent_id].append({"from": f"agent:{agent_id}",
                                     "ts": datetime.now().isoformat(timespec="seconds"),
                                     "text": text})
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
    wants_html = "text/html" in request.headers.get("Accept", "")
    if wants_html:
        return Response("<!DOCTYPE html><html><head><meta charset='utf-8'><title>404</title>"
                        "<link rel='icon' href='/favicon.ico'></head>"
                        "<body style='font-family:monospace;background:#111214;color:#e7e7ea;padding:40px'>"
                        "<h1>404 Not Found</h1><p>That path does not exist (bad log token? typo?). "
                        "Start at <a style='color:#60a5fa' href='/'>/ for onboarding</a>.</p>"
                        "</body></html>", status=404, content_type="text/html")
    return jsonify(error="not found"), 404


@app.errorhandler(413)
def too_large(_e):
    return jsonify(error="payload too large"), 413


@app.errorhandler(Exception)
def server_error(exc):  # never crash a client into a hang
    log_event("MSG_FAIL", "-", "Server -> Client (error)", f"{type(exc).__name__}: {exc}")
    return jsonify(error="internal server error", detail=str(exc)), 500


# ----------------------------------------------------------------- public tunnel (optional)
def _warn_localhost(reason: str) -> None:
    msg = f"{reason} - no tunnel, serving localhost only on http://localhost:{HUB_PORT}"
    print(f"[agent-hub] WARN: {msg}")
    log_event("SERVER", "-", "Server -> Server", msg)


def open_tunnel() -> None:
    """Best effort: expose this hub over an ngrok HTTPS edge. Any problem (no token,
    package missing, network down) only warns - the hub keeps serving on localhost."""
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
        listener = ngrok.forward(f"localhost:{HUB_PORT}", proto="http")
    except Exception as exc:  # noqa: BLE001 - tunnel failure must not kill the hub
        # ngrok echoes the authtoken back in its error text, keep it out of the log page
        detail = str(exc).replace(NGROK_AUTHTOKEN, "<redacted>")
        _warn_localhost(f"ngrok tunnel failed: {type(exc).__name__}: {detail}")
        return
    url = listener.url()
    print(f"[agent-hub] ngrok tunnel up: {url}")
    print(f"[agent-hub]   agents:  python3 mock_agent.py --server {url} --agent-id <id> --token \"$AGENT_AUTH_TOKEN\"")
    print(f"[agent-hub]   clients: send header  ngrok-skip-browser-warning: true  on every request")
    log_event("SERVER", "-", "Server -> ngrok edge", f"public {url} -> :{HUB_PORT}")


# ----------------------------------------------------------------- main
if __name__ == "__main__":
    init_log_file()
    log_event("SERVER", "-", "Server -> Server", f"hub started on :{HUB_PORT} (threading mode)")
    print(f"[agent-hub] listening on :{HUB_PORT} | agents socket ns={NS} | "
          f"NAUTH len={len(NAUTH)} | agent token len={len(AGENT_TOKEN)} | "
          f"log page = /logs/{LOG_SECRET[:3]}***")
    threading.Thread(target=open_tunnel, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=HUB_PORT, debug=False, allow_unsafe_werkzeug=True)
    
