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

import atexit
import hashlib
import hmac as hmac_mod
import html as html_mod
import json
import mimetypes
import os
import re
import secrets as pysecrets
import sys
import threading
import time
from collections import Counter, OrderedDict, defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from flask import Flask, Response, abort, jsonify, request, send_file
from flask_socketio import ConnectionRefusedError, SocketIO
from werkzeug.exceptions import HTTPException
from werkzeug.serving import BaseWSGIServer
from werkzeug.utils import secure_filename

import hubdocs

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
# An ACK is intent, not an answer. Rows whose only results are ACKs are reported as
# awaiting_answer at any value; >0 additionally dead-letters them once as `acked_silence` after
# that many seconds of silence, and the row is KEPT, so an answer that arrives late still lands.
ACKED_TTL_SECONDS = max(0, int(os.environ.get("HUB_ACKED_TTL_SECONDS", "0")))
TASK_LEDGER_MAX = 500          # msg_ids kept in the ledger before the oldest is evicted
DEAD_LETTER_MAX = 200
# Retention. Everything above is capped by COUNT, which is fine under load and useless for a
# quiet hub: a hub that sees three events a day keeps a 14-day-old deliverable, its ledger row
# and its log rows forever, because nothing was ever due for eviction. Age is the other axis.
# Default: prune anything older than 14 days. HUB_RETENTION_DAYS=0 keeps everything (count caps
# still apply), HUB_RETENTION_DRY_RUN=1 reports what would go without deleting a byte.
RETENTION_DAYS = max(0, int(os.environ.get("HUB_RETENTION_DAYS", "14")))
RETENTION_SWEEP_SECONDS = max(60, int(os.environ.get("HUB_RETENTION_SWEEP_SECONDS", "3600")))
RETENTION_DRY_RUN = os.environ.get("HUB_RETENTION_DRY_RUN", "") == "1"
# Unread nudge. A hub->agent push needs a live socket, so an agent that loses one misses what
# was addressed to it while it was gone. Every UNREAD_NUDGE_SECONDS - and once right at
# connect, which is when it matters most - the hub tells each connected agent what it still
# holds for that agent: tasks with no result frame back, stored files no socket ever heard
# about, and peer relays queued instead of dropped. 0 switches the sweep off but keeps telling
# an agent what it missed at connect: that moment is not a nag, and the relay queue would
# otherwise fill with text nobody ever receives. Anything under 15 is a nag, so clamp it.
UNREAD_NUDGE_SECONDS = int(os.environ.get("HUB_UNREAD_NUDGE_SECONDS", "45"))
if 0 < UNREAD_NUDGE_SECONDS < 15:
    UNREAD_NUDGE_SECONDS = 15
UNREAD_LIST_MAX = 10         # handles named per category in one notice
MAIL_FLUSH_MAX = 20          # queued relays released per agent per notice, the rest wait
MAIL_QUEUE_MAX = 200         # per agent, drop-oldest
MAIL_AGENTS_MAX = 200        # how many ids may hold mail at all; coldest id evicted first
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

HUB_VERSION = "1.11.2"
FEATURES = ["llms_txt", "api_manifest", "json_errors", "method_405", "inbox_peek",
            "agents_detail", "upload_sha256", "autojson_name", "file_index",
            "events_json", "ngrok_domain", "result_lookup", "events_mine",
            "client_source", "dedupe_uploads", "superseded_notice", "single_result_log_rows",
            "standby_takeover_chain", "result_inbox_status", "sid_scope_notes",
            "unique_agent_ids", "task_ledger", "dead_letter_queue", "agent_last_seen",
            "agent_credentials", "scoped_reads", "operator_principal", "result_by_caller",
            "log_write_resilience", "log_token_nonascii_404", "scoped_events_exact_tag",
            "events_payload_full", "atomic_index_write", "file_delete", "retention_sweep",
            "log_write_debounce", "result_ack_kind", "awaiting_answer_state",
            "events_mine_index", "events_mine_index_reclaim", "connect_client_hint",
            "unread_nudge", "relay_queue", "log_who_column", "events_from_to",
            "log_filters", "upload_100mb", "shared_file_store", "log_file_download"]
STARTED_AT = time.time()

ALLOWED_EXT = (".json", ".txt", ".html", ".htm", ".tar.gz", ".tgz")
MAX_UPLOAD = 100 * 1024 * 1024
AGENT_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,40}$")
_LEGAL_ID_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")

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
# What the unread nudge reads. agent_mail is the store-and-forward that replaced dropping a
# relay to an offline agent (memory-only, like client_inboxes - a hub restart loses it).
# files_unannounced remembers which stored bytes were addressed to an id that had no socket at
# upload time: the bytes are durable in file_meta, only the notice was lost, so this tracks the
# notice and nothing else. nudged is the never-twice ledger - without it one task that sits
# unanswered for a day would nag its agent every 45 seconds.
agent_mail = OrderedDict()   # agent_id -> deque(maxlen=MAIL_QUEUE_MAX) of {msg_id, from, text, ts}
files_unannounced = defaultdict(OrderedDict)   # agent_id -> {file_id: iso} no socket ever heard
nudged_tasks = OrderedDict()  # "<agent_id>|<msg_id>" -> iso, capped like id_rejects. Only tasks
                              # need it: a queued relay leaves the queue when flushed and an
                              # unannounced file leaves files_unannounced when announced, so both
                              # are once-only by construction, while a `delivered` row stays.
nudge_total = 0
_next_nudge_at = 0.0
_next_nudge_at = 0.0
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
LOG_MAX_ROWS = 3000
# ONE structured ring, rendered on read. It used to be two: LOG_ROWS held every event a second
# time, pre-baked into HTML, so the page could be a string join. That duplicate ring carried up to
# ~13 MB of the 52.5 MB a full log attributed to "the two rings", and it made the page
# unfilterable - a row that is already markup cannot be folded, grouped, matched against a query or
# re-columned without changing what a writer decided at event time. A 3000-row page build now costs
# ~1.9 ms (0.32 ms joined, 1.73 ms of it html.escape) and each log call gets ~9 us CHEAPER,
# because the escaping moved off the thread that logged and onto the reader that asked for it.
LOG_EVENTS = deque(maxlen=LOG_MAX_ROWS)    # the log: one dict per event
LOG_ROUTE_ROWS = 800                       # what one GET renders; the disk mirror keeps all 3000
log_lock = threading.Lock()
# GET /events/mine used to answer by regex-scanning EVERY row of a full ring while holding
# log_lock - 3000 rows x (direction + 160-char summary) = 0.55 MB of matching per poll, measured
# at a 14.9 ms lock hold (paired in-process, one ring). log_event and every other request queue
# behind that: on a loaded disposable the poll cost 12 ms alone, 371 ms at 50-way, 546 ms with
# traffic. The index below is built at write time from the row's own STRUCTURE - the agent it is
# about, plus the `from` and `to` ends - keyed by EXACT id, so a poll costs O(that agent's rows):
# 0.085 ms of lock for the same 375 rows. It used to read those ids out of `direction` prose
# instead, which meant an agent was in its own feed only if the code path that logged its event
# happened to spell "agent:" or "Agent:" in the sentence it authored: every FILE_RCVD row in a
# 198-row session (agent column "agent:qoder", direction a nameless literal) landed in no bucket at
# all, so that agent could never see its own downloads. Prose mentions inside a payload were never
# searched and still are not; ?mentions=1 keeps the whole-ring scan reachable.
LOG_SEQ = 0                        # rows ever appended - lets a bucket drop what the ring evicted
EVENT_INDEX: dict = {}             # agent_id -> deque[(seq, row dict)], oldest first
# An id in this index but not in the ring is dead weight; an id in the ring but not in this index
# reads an empty feed. So the bound tracks the ring: you cannot usefully index more ids than there
# are rows to hold them, and evicting an id that still has live rows is a wrong answer, not a slow
# one. Overflow past this still evicts the coldest bucket (see _evict_coldest_locked) and the
# payload says so with indexed=false. Measured: 500 ids cost ~0.4 MB; the worst case here is 3000
# near-empty deques, ~2.4 MB, against 52.5 MB for the rings themselves.
EVENT_INDEX_MAX_IDS = LOG_MAX_ROWS
_EVENT_TAG_RE = re.compile(r"(?:agent|Agent):([A-Za-z0-9_\-]{1,40})(?![A-Za-z0-9_\-])")

# ------------------------------------------------------- who a log row is speaking about/to
# The single place a principal becomes a token a human reads. Before this, each of the 34 log
# sites authored its own sentence, and 198 rows of one session carried 27 different grammars for
# the same fact ("agent:x", "Agent:x", "Mail:x", a bare "x", "Client -> Server -> Agent"). Two of
# those grammars were actively false: FILE_SENT put the notify RECIPIENT in the bold agent column
# where the uploader belonged (the row credited agent qoder2, offline all session, for hubmaster's
# upload), and actor_label()'s "agent:qoder"/"operator" leaked straight into a column that
# otherwise held bare ids - six spellings of one column.
#
# Words here name a hub-side END, not an agent. An agent that connects as literally "hub", "store",
# "mail", "web" or "operator" reads its own rows as the hub talking to itself; those names are
# reserved by this table.
_PSEUDO_END = {"": "hub", "-": "hub", "server": "hub", "hub": "hub",
               "file store": "store", "store": "store", "files": "store",
               "mail": "mail", "ngrok": "ngrok", "ngrok edge": "ngrok",
               "client": "web", "web": "web", "operator": "op"}
# Not real agent ids: indexing one spends an EVENT_INDEX bucket on something that can never poll,
# and would make GET /events/mine answer indexed=true for a caller that does not exist.
_RESERVED = frozenset(_PSEUDO_END.values()) | {"agent"}


def _tok(ident: str) -> tuple:
    """Bare id -> ('@id', 'id'). An id that does not match AGENT_ID_RE came from somewhere that
    never validated it (a REFUSED connect reports the agent_id the client asked for), so it must
    not be able to wear a real token's clothing: an agent_id of "@hubmaster" would otherwise
    render a rejection as something the master's agent did."""
    ident = (ident or "").strip()
    if ident == "?":                     # the sid_to_agent miss sentinel: unknown, not an attack
        return "?", ""
    if AGENT_ID_RE.fullmatch(ident):
        return f"@{ident}", ident
    return f"?{ident[:48]}", ""


def _who(label) -> tuple:
    """Resolve any principal label to (display token, real agent id or "").

      'agent:qoder'      -> ('@qoder', 'qoder')      credential-proven agent
      'operator:hubmaster' -> ('op:hubmaster', 'hubmaster')  master token + SELF-DECLARED label
      'operator'         -> ('op', '')               master token, no label
      'client'           -> ('web', '')              nothing presented
      'qoder'            -> ('@qoder', 'qoder')      validated bare id
      'Server'/'File store'/'Mail:qoder' -> hub-side end, id from the tail where there is one

    'op:' stays visibly different from '@' because an operator calling itself 'hubmaster' over the
    master token is a claim, not a credential - the footgun this repo already documents. And the
    display token embeds the raw id verbatim (@qoder, never Q(qoder)): GET /events/mine promises a
    caller its own id appears in agent+dir+payload, and a token that hid it would break that
    contract from the presentation layer.
    """
    raw = str(label or "").strip()
    low = raw.lower()
    if low in _PSEUDO_END:
        return _PSEUDO_END[low], ""
    head, sep, tail = raw.partition(":")
    if sep:
        h = head.lower()
        if h in ("agent", "mail", "store", "to", "from"):
            return _tok(tail)
        if h == "operator":
            disp, real = _tok(tail)
            return (f"op:{real}" if real else "op"), real
    return _tok(raw)


def _humansize(n) -> str:
    """17897384 -> '17.1 MB'. Byte counts were the single widest thing on the page."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return str(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


log_write_warned_at = 0.0          # throttle the WARN so a broken disk is not a print flood
log_mirror_writes = 0              # successful mirror writes since start - /health reports it
# The ring is the source of truth and /logs renders from it on read, so the file on disk is
# only a mirror: flag it dirty and flush on a cadence instead of rewriting a 13 MB page per event.
LOG_WRITE_INTERVAL_SECONDS = max(0, int(os.environ.get("HUB_LOG_WRITE_INTERVAL", "2")))
log_page_dirty = False
log_writer_started = False
log_written_rows = -1              # how many rows the on-disk mirror was built from

LOG_CSS = """
:root{--bg:#f5f5f3;--fg:#1c1c1e;--card:#ffffff;--line:#e3e3df;--muted:#6b6b6f;--acc:#1d4ed8}
@media(prefers-color-scheme:dark){:root{--bg:#111214;--fg:#e7e7ea;--card:#1a1c1f;--line:#2a2d31;--muted:#8b8f96;--acc:#60a5fa}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,"Courier New",monospace;font-size:13px}
header{padding:10px 14px;background:var(--card);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:5}
.hrow{display:flex;gap:12px;flex-wrap:wrap;align-items:baseline}
header h1{font-size:15px;margin:0}
header .sub{color:var(--muted);font-size:12px}
.legend{color:var(--muted);font-size:11px;margin-top:4px}
.legend b{color:var(--fg);font-weight:700}
#flt{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-top:8px}
#flt input,#flt select{background:var(--bg);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:3px 8px;font:inherit;font-size:12px}
#flt button{background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:3px 10px;font:inherit;font-size:12px;cursor:pointer}
#flt button:hover,#flt input:focus{border-color:var(--muted)}
nav.chips{display:flex;gap:5px;flex-wrap:wrap;margin-top:7px}
a.chip{text-decoration:none;color:var(--fg);background:var(--bg);border:1px solid var(--line);border-radius:999px;padding:1px 9px;font-size:11px;white-space:nowrap}
a.chip:hover{border-color:var(--muted)}
a.chip.on{background:var(--acc);color:#fff;border-color:var(--acc)}
#log{padding:6px 12px}
.row{display:grid;grid-template-columns:auto auto minmax(190px,max-content) minmax(220px,1fr);gap:10px;align-items:baseline;padding:4px 8px;border-bottom:1px solid var(--line);border-radius:6px}
.row:hover{background:var(--card)}
.ts{color:var(--muted);white-space:nowrap;font-variant-numeric:tabular-nums}
.badge{padding:1px 9px;border-radius:999px;font-size:11px;font-weight:700;color:#fff;letter-spacing:.4px;white-space:nowrap;background:#4b5563}
.b-CONNECTED{background:#15803d}.b-DISCONNECTED{background:#b91c1c}.b-MSG_SENT{background:#1d4ed8}
.b-MSG_RCVD{background:#7c3aed}.b-AUTH_FAIL{background:#c2410c}.b-FILE_SENT{background:#0e7490}
.b-FILE_RCVD{background:#0f766e}.b-FILE_DEL{background:#475569}.b-HOUSEKEEP{background:#3f3f46}.b-MSG_FAIL{background:#52525b}.b-SERVER{background:#334155}
.b-ID_REJECTED{background:#a16207}
.b-TASK_EXPIRED{background:#9f1239}.b-TASK_WEDGED{background:#be123c}.b-SCOPE_DENY{background:#7c2d12}
.b-CRED_MINTED{background:#4338ca}.b-CRED_FAIL{background:#be123c}
.b-UNREAD_NUDGE{background:#1e40af}.b-MSG_QUEUED{background:#0369a1}
.w{white-space:nowrap;font-size:13px;overflow:hidden;text-overflow:ellipsis}
.f{font-weight:700}.g{font-weight:700;color:var(--acc)}.a{color:var(--muted);font-weight:400;padding:0 3px}
.s{display:inline-block;color:var(--muted);font-size:11px;border:1px solid var(--line);border-radius:999px;padding:0 7px;margin-left:6px}
.m{display:inline-block;color:var(--muted);font-size:11px;border:1px solid var(--line);border-radius:999px;padding:0 7px;margin-left:6px;text-decoration:none}
.m:hover{border-color:var(--muted);color:var(--fg)}
a.dl{display:inline-block;color:var(--acc);font-size:11px;border:1px solid var(--acc);border-radius:999px;padding:0 7px;margin-left:6px;text-decoration:none;max-width:220px;overflow:hidden;text-overflow:ellipsis;vertical-align:bottom}
a.dl:hover{background:var(--acc);color:#fff}
.bad{color:#b91c1c;background:rgba(185,28,28,.14);border-radius:4px;padding:0 3px}
.x{color:var(--muted);font-size:11px;border:1px solid var(--line);border-radius:999px;padding:0 6px}
.day{color:var(--muted);font-size:11px;letter-spacing:.6px;text-transform:uppercase;padding:9px 8px 3px;border-bottom:1px solid var(--line)}
.empty{color:var(--muted);padding:18px 10px;text-align:center}
.aid{font-weight:700}.dir{color:var(--muted);font-size:12px}
.pl{min-width:0;word-break:break-word;color:var(--fg)}
#ar{background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:3px 10px;font:inherit;font-size:12px;cursor:pointer;margin-left:auto}
#ar:hover{border-color:var(--muted)}
.row.exp{cursor:pointer}
.row.exp .pl::after{content:" ▾";color:var(--muted);font-size:11px}
.row.exp.open .pl::after{content:" ▴";color:var(--muted);font-size:11px}
.full{display:none;grid-column:1/-1;white-space:pre-wrap;word-break:break-word;background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:8px 10px;margin:6px 0 2px;font-size:12px}
.row.open .full{display:block}
@media(max-width:760px){.row{grid-template-columns:auto auto;grid-auto-rows:auto}.w{white-space:normal}}
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
(function(){
  var K='hub-log-autoref', timer=null, btn=document.getElementById('ar');
  function armed(){try{return sessionStorage.getItem(K)!=='0'}catch(e){return true}}
  function paint(){if(btn){btn.textContent='auto-refresh: '+(timer?'on':'off');}}
  function start(){if(!timer){timer=setInterval(function(){location.reload()},5000);}paint();}
  function stop(){if(timer){clearInterval(timer);timer=null;}paint();}
  function set(v){try{sessionStorage.setItem(K,v?'1':'0')}catch(e){} if(v){start()}else{stop()}}
  if(btn){btn.addEventListener('click',function(){set(!timer)});}
  if(armed()){start()}else{stop()}
  document.addEventListener('click',function(ev){
    if(ev.target.closest('.full')){return}
    var row=ev.target.closest('.row.exp');
    if(!row){return}
    var opening=!row.classList.contains('open');
    row.classList.toggle('open');
    if(opening&&timer){set(false)}
  });
})();
"""


def _row_matches(row, agent, event, q) -> bool:
    """One row against the active filter. `agent` matches ANY of the three ids a row carries
    (subject, sender, recipient) rather than only the bold column, because that column is the
    subject and a caller asking for '@qoder' means every row that involves qoder."""
    if agent and agent not in (_who(row.get("agent", ""))[1], _who(row.get("frm", ""))[1],
                              _who(row.get("to", ""))[1]):
        return False
    if event and row.get("event") != event:
        return False
    if q:
        hay = f"{row.get('ref', '')} {row.get('payload', '')} {row.get('agent', '')}".lower()
        if q.lower() not in hay:
            return False
    return True


def _fold_key(row):
    """What makes two consecutive rows the same line said twice. The first 64 payload chars, not
    all of it: a file-push burst differs only in the msg_id, and the point of folding is to stop
    that filling the page."""
    return (row.get("event"), row.get("frm"), row.get("to"), row.get("ref"),
            (row.get("payload") or "")[:64])


def _log_row_html(row, repeat=0, dl=None) -> str:
    """One row of the page. `dl` is the operator's download context - (url prefix, {file_id:
    meta}) - or None, which is how the on-disk mirror renders: a static file on disk cannot
    stream bytes, so it carries no links."""
    esc = html_mod.escape
    d_f, id_f = _who(row.get("frm", ""))
    d_t, id_t = _who(row.get("to", ""))
    if row.get("frm") or row.get("to"):
        who = (f"<span class='f'>{esc(d_f)}</span>"
               f"<span class='a'>&#8594;</span><span class='g'>{esc(d_t)}</span>")
    else:
        # a row whose ends were never named: show its own words rather than invent an arrow
        who = f"<span class='dir'>{esc(row.get('dir') or '-')}</span>"
    subj = ""
    d_s, id_s = _who(row.get("agent", ""))
    if id_s and id_s not in (id_f, id_t):
        subj = f"<span class='s' title='who this row is about'>re: {esc(d_s)}</span>"
    ref = row.get("ref") or ""
    ref_html = f"<a class='m' href='?q={quote(ref, safe='')}' title='task / file id'>#{esc(ref[:16])}</a>" if ref else ""
    if dl and ref in dl[1]:
        base, files = dl
        meta = files[ref]
        label = meta.get("name") or ref
        ref_html += (f"<a class='dl' href='{esc(base)}{quote(ref, safe='')}'"
                     f" title='download {esc(label)} - {_humansize(meta.get('size') or 0)}"
                     f", uploaded by {esc(_who(meta.get('by') or '')[0])}'"
                     f">&#8595;&nbsp;{esc(label[:28])}</a>")
    text = row.get("payload") or ""
    if ref and text.startswith(f"[{ref}]"):      # the id has a column now; do not print it twice
        text = text[len(ref) + 3:].lstrip()
    full = row.get("payload_full") or ""
    expandable = full != (row.get("payload") or "")
    x = f"<span class='x' title='repeated'>×{repeat}</span> " if repeat > 1 else ""
    detail = f"<div class='full'>{esc(full[:4000])}</div>" if expandable else ""
    ev = row.get("event") or "?"
    return (f"<div class='row{' exp' if expandable else ''}'>"
            f"<span class='ts'>{esc((row.get('ts') or '')[11:])}</span>"
            f"<span class='badge b-{esc(ev)}'>{esc(ev)}</span>"
            f"<span class='w'>{who}{subj}{ref_html}</span>"
            f"<span class='pl'>{x}{esc(text)}</span>{detail}</div>")


def _render_log(rows, filt=None, dl=None) -> str:
    """Build the page from the structured ring. `filt` is None for the on-disk mirror, which must
    stay the complete archive; the route passes {agent, event, q, n, fold}. `dl` is the download
    context the route adds so a file row is clickable - the mirror renders without it."""
    filt = filt or {}
    agent, event, q = filt.get("agent", ""), filt.get("event", ""), filt.get("q", "")
    limit, fold = filt.get("n", 0), bool(filt.get("fold"))
    esc = html_mod.escape
    shown = [r for r in rows if _row_matches(r, agent, event, q)]
    if limit and len(shown) > limit:
        shown = shown[-limit:]
    # chips count the WHOLE snapshot, not the filtered view, so no chip is ever a link to nothing
    agents, events = Counter(), Counter()
    for r in rows:
        events[r.get("event") or "?"] += 1
        for aid in (_who(r.get("agent", ""))[1], _who(r.get("frm", ""))[1], _who(r.get("to", ""))[1]):
            if aid:
                agents[aid] += 1

    def chips(items, param):
        return "".join(f"<a class='chip{' on' if v == (event if param == 'event' else agent) else ''}'"
                       f" href='?{param}={quote(v, safe='')}'>{esc(v)} <span class='x'>{n}</span></a>"
                       for v, n in items)

    out = []
    prev_day = ""
    i = 0
    while i < len(shown):
        row = shown[i]
        j = i + 1
        repeat = 1
        if fold:
            key = _fold_key(row)
            while j < len(shown) and _fold_key(shown[j]) == key:
                j += 1
            repeat = j - i
            row = shown[j - 1]   # keep the NEWEST of the run - the ring is chronological, and a
                                 # reader (and the selftest) asks "did the hub do it yet", of the
                                 # last one, never the first
        day = (row.get("ts") or "")[:10]
        if day != prev_day:
            out.append(f"<div class='day'>{esc(day)}</div>")
            prev_day = day
        out.append(_log_row_html(row, repeat, dl))
        i = j
    body = "".join(out) or f"<div class='empty'>no rows match this filter ({len(rows)} in the ring)" \
                           f" &middot; <a href='?'>clear</a></div>"
    tz = datetime.now().astimezone().tzname() or "local"
    active = any((agent, event, q))
    scope = f" &middot; filtered to {esc(agent or event or q)}" if active else ""
    dl_hint = " &middot; <b>&#8595;</b> a stored file, click to download" if dl else ""
    return ("<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<title>Agent Hub - Event Log</title>"
            f"<link rel='icon' href='{FAVICON_ROUTE}'>"
            f"<style>{LOG_CSS}</style></head><body>"
            "<header><div class='hrow'><h1>Agent Hub - Event Log</h1>"
            f"<span class='sub'>{len(shown)} of {len(rows)} rows &middot; {len(agents)} agents"
            f"{scope} &middot; {datetime.now().strftime('%H:%M:%S')} {esc(tz)}</span>"
            "<button id='ar' title='tick every 5s; click to stop for uninterrupted reading'>auto-refresh: on</button></div>"
            "<div class='legend'>who column reads <b>sender &#8594; receiver</b> from structured "
            "fields, never from message text &middot; <b>@id</b> an agent (live socket, or addressed "
            "by id) &middot; "
            "<b>op:id</b> operator token naming itself (a claim, not a credential) &middot; "
            "<b>web</b> unauthenticated caller &middot; <b>hub / store / mail</b> the hub itself "
            f"&middot; <b>re:</b> who the row is about when that is neither end{dl_hint}"
            f" &middot; times in {esc(tz)}</div>"
            f"<form id='flt' method='get'>"
            f"<input name='agent' placeholder='agent id' value='{esc(agent)}'>"
            f"<select name='event'><option value=''>any event</option>"
            + "".join(f"<option value='{esc(e)}'{' selected' if e == event else ''}>{esc(e)} ({n})</option>"
                      for e, n in events.most_common())
            + f"</select>"
            f"<input name='q' placeholder='msg_id / text' value='{esc(q)}'>"
            + ("<input type='hidden' name='fold' value='1'>" if fold else "")
            + "<button>apply</button>"
            # href='?' = this path with an EMPTY query. href='' would resolve to the current URL
            # including its query string, so "clear" would reload the very filter it claims to drop.
            + ("<a class='chip' href='?'>clear</a>" if active or fold or limit else "")
            + "</form>"
            "<nav class='chips'>" + chips(agents.most_common(12), "agent") + chips(events.most_common(10), "event")
            + ("<a class='chip' href='?fold=1" + (f"&agent={quote(agent, safe='')}" if agent else "")
               + (f"&event={quote(event, safe='')}" if event else "") + "'>fold repeats</a>" if not fold
               else "<a class='chip on' href='?'>unfold</a>")
            + "</nav></header>"
            f"<div id='log'>{body}</div>"
            f"<script>{LOG_JS}</script></body></html>")


def _index_prune_locked() -> int:
    """Caller holds log_lock. A bucket must not outlive the rows it indexes: once the ring (or the
    retention sweep) has moved past every row for an id, the id goes. Without this the index is a
    record of every id the process ever saw, so EVENT_INDEX_MAX_IDS is spent on dead agents and a
    NEW agent's GET /events/mine answers an empty feed forever - measured: after 500 ids had ever
    polled, the 501st got count=0 while ?mentions=1 found its row. Cheap either way: one pass over
    the buckets, and the buckets only hold rows that are still in the ring."""
    floor = max(1, LOG_SEQ - len(LOG_EVENTS) + 1)
    dead = []
    for aid, bucket in EVENT_INDEX.items():
        while bucket and bucket[0][0] < floor:
            bucket.popleft()
        if not bucket:
            dead.append(aid)
    for aid in dead:
        EVENT_INDEX.pop(aid, None)
    return len(dead)


def prune_event_index() -> int:
    """Reclaim ids whose rows have all left the ring. Called from the reaper tick and after a
    retention sweep, so a long-running hub cannot fill the index with retired agents."""
    with log_lock:
        return _index_prune_locked()


def _evict_coldest_locked() -> None:
    """Caller holds log_lock. At the id cap, drop the bucket whose newest row is oldest, so a
    saturated index tracks the ids in the current ring instead of the first 500 the process met.
    The evicted id's indexed feed reads empty until its next row is written; ?mentions=1 (the
    whole-ring scan) still answers it correctly, and `indexed` in the payload says which case a
    caller is in."""
    if not EVENT_INDEX:
        return
    coldest = min(EVENT_INDEX,
                  key=lambda aid: EVENT_INDEX[aid][-1][0] if EVENT_INDEX[aid] else 0)
    EVENT_INDEX.pop(coldest, None)


def _index_event(row: dict, seq: int) -> None:
    """Caller holds log_lock. Read the ids out of the row's STRUCTURE, not out of a sentence: the
    agent the row is about plus both ends of who-spoke-to-whom. The regex pass over `dir` stays as
    a superset so a row cannot lose an index entry it had under the old scheme; the `_RESERVED`
    guard is what keeps 'hub'/'store'/'mail'/'web' from eating buckets, and `indexed=false` in
    GET /events/mine depends on it.
    """
    ids = {_who(row.get("agent", ""))[1], _who(row.get("frm", ""))[1], _who(row.get("to", ""))[1]}
    ids.update(_EVENT_TAG_RE.findall(row.get("dir", "") or ""))
    for aid in ids:
        if not aid or aid in _RESERVED or not AGENT_ID_RE.fullmatch(aid):
            continue
        bucket = EVENT_INDEX.get(aid)
        if bucket is None:
            if len(EVENT_INDEX) >= EVENT_INDEX_MAX_IDS:
                _index_prune_locked()              # ids the ring already forgot
            if len(EVENT_INDEX) >= EVENT_INDEX_MAX_IDS:
                _evict_coldest_locked()            # still full: prefer live ids over old ones
            bucket = EVENT_INDEX[aid] = deque(maxlen=LOG_MAX_ROWS)
        bucket.append((seq, row))


def log_event(event: str, agent_id: str = "", direction: str = "", payload: str = "",
              *, frm: str = "", to: str = "", ref: str = "", note: str = "") -> None:
    """Record one event.

    `agent_id` is the row's SUBJECT - who the event is ABOUT, not who acted: the recipient for
    MSG_SENT/MSG_QUEUED, the answering agent for MSG_RCVD, the downloader for FILE_RCVD. `frm`/`to`
    are who actually SPOKE and who received it. That split is the whole point of this signature -
    the page used to carry one bold name per row and let every call site write its own prose for
    the rest, which is how FILE_SENT came to print an offline recipient where the uploader belonged
    and a human reading the column was handed a false sentence. `agent_id` keeps its pre-v1.10
    meaning unchanged because GET /logs/<t>/events.json and GET /events/mine are agent-facing.

    `direction` is now a parenthetical QUALIFIER ("HTTP task", "socket open", "deduped"). The path
    itself is derived from frm/to, so the string a human reads and the fields a machine reads
    cannot disagree - structurally, not by discipline. Pass it positionally with no frm/to and the
    row keeps its pre-v1.10 shape.

    `ref` is a msg_id/file_id that belongs in its own column, not glued to the front of the payload.
    `note` carries the long explanation: `payload` stays terse for the page, `payload_full` gets
    both, so a structured consumer reads exactly what a human used to.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    d_frm, _ = _who(frm)
    d_to, _ = _who(to)
    if frm or to:
        path = f"{d_frm} -> {d_to}"
        dir_out = f"{path} ({direction})" if direction else path
    else:
        dir_out = direction
    full = payload or ""
    summary = full[:160].replace("\n", " ")
    expandable = full != summary or bool(note)
    if expandable:
        summary += " …"
    clipped = full[:4000] + (" …[truncated]" if len(full) > 4000 else "")
    if note:
        clipped = f"{clipped} - {note}" if clipped else note
    row = {"ts": ts, "event": event, "agent": agent_id or "-",
           "dir": dir_out, "payload": summary,
           # structured consumers (agents) used to get ONLY the 160-char
           # summary while the human page carried 4000 - task bodies were
           # unrecoverable from any JSON endpoint. Additive key.
           "payload_full": clipped,
           # v1.10 additive: the ends as given, so a consumer resolves ids itself instead of
           # parsing a sentence. The row dict becomes IMMUTABLE here - EVENT_INDEX and every JSON
           # response hand out this same object, so fold/filter/group must work on copies of the
           # list and never rewrite a row.
           "frm": frm, "to": to, "ref": ref}
    with log_lock:
        LOG_EVENTS.append(row)
        global log_page_dirty, LOG_SEQ
        LOG_SEQ += 1
        _index_event(row, LOG_SEQ)
        log_page_dirty = True
    if LOG_WRITE_INTERVAL_SECONDS == 0:
        flush_log_page()           # legacy write-per-event, but still off the lock


def flush_log_page() -> None:
    """Best-effort refresh of the on-disk mirror. Takes the rows under log_lock (a list of
    references, microseconds) and does the build + write WITHOUT it, because a 13 MB page render
    and a blocking write must not be what an event or request waits on. The ring is the source
    of truth and /logs renders from it, so a failed write (disk full, path replaced by a
    directory, perms) must NEVER fail the request or socket event that logged - an unguarded
    write_text here used to 500 every upload/relay/message the moment LOG_FILE went unwritable.
    """
    global log_page_dirty, log_write_warned_at, log_written_rows, log_mirror_writes
    with log_lock:
        if not log_page_dirty and len(LOG_EVENTS) == log_written_rows:
            return
        snapshot = list(LOG_EVENTS)
        log_page_dirty = False
    try:
        # filt=None: the mirror is the whole ring, unfiltered. The route caps what one GET renders;
        # this file must not, because it is the only durable copy an operator has.
        LOG_FILE.write_text(_render_log(snapshot), encoding="utf-8")
        log_written_rows = len(snapshot)
        log_mirror_writes += 1     # surfaced in /health: writes are bounded by cadence, not events
    except Exception as exc:  # noqa: BLE001 - a broken log page must not break the mesh
        log_page_dirty = True                      # retry on the next tick
        now = time.time()
        if now - log_write_warned_at >= 60:
            log_write_warned_at = now
            print(f"[agent-hub] WARN: cannot write log file {LOG_FILE} "
                  f"({type(exc).__name__}: {exc}) - events stay in memory, endpoints "
                  f"unaffected; /logs serves from memory until this clears "
                  f"(check disk / HUB_LOG_FILE)", file=sys.stderr)


def _log_page_writer_loop() -> None:
    while True:
        time.sleep(LOG_WRITE_INTERVAL_SECONDS)
        flush_log_page()


def start_log_page_writer() -> None:
    """Bounded cadence: at most one page rewrite per HUB_LOG_WRITE_INTERVAL seconds no matter
    how many events land between two ticks. 0 disables the thread and keeps the old
    write-per-event path (still rendered off the lock). The rings are the read path, so the
    freshest event is visible immediately either way - only the mirror lags by one interval."""
    global log_writer_started
    if log_writer_started or LOG_WRITE_INTERVAL_SECONDS <= 0:
        return
    log_writer_started = True
    threading.Thread(target=_log_page_writer_loop, daemon=True,
                     name="log-page-writer").start()
    atexit.register(flush_log_page)


def init_log_file() -> None:
    global log_page_dirty
    with log_lock:
        log_page_dirty = True
    flush_log_page()


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
    fail the upload that already succeeded. The write goes to a temp file and lands via
    os.replace (atomic on POSIX): write_text truncates in place, so a crash or ENOSPC
    mid-write used to leave index.json half-written - the whole uploader/content-type
    audit trail was then rebuilt as 'unknown (rehydrated from disk)' on restart."""
    try:
        tmp = INDEX_FILE.with_name(INDEX_FILE.name + ".tmp")
        # measured on a 500-row index: indent=1 writes 156,502 B per flush and every upload flushes
        # the WHOLE table (78 MB across 500 uploads), so this is O(n^2) in bytes by design. Compact
        # separators cut it to 138,001 B (-12%) for free; coalescing to one write per second was
        # REJECTED on purpose - a crash inside that window loses the uploader, content-type and
        # sha of the newest objects (disk re-adoption rebuilds them as "unknown (rehydrated from
        # disk)"), so a reader CAN observe the missing entry, which is exactly the bar this
        # function's atomic replace exists to protect.
        tmp.write_text(json.dumps(file_meta, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, INDEX_FILE)
    except Exception as exc:  # noqa: BLE001
        print(f"[agent-hub] WARN: file index persist failed: {exc}")


def drop_file_locked(file_id: str):
    """Caller holds meta_lock. Remove one stored object: its bytes, its index entry, and its
    sha advisory (re-pointed at the newest surviving duplicate so ?dedupe=1 stops handing out
    a deleted id). Returns the meta it removed, or None for an unknown id. An OSError from the
    unlink propagates with the index still intact, so the caller can retry."""
    meta = file_meta.get(file_id)
    if not meta:
        return None
    (FILE_STORE / f"{file_id}__{meta['name']}").unlink(missing_ok=True)
    file_meta.pop(file_id, None)
    sha = meta.get("sha256")
    if sha and sha_index.get(sha) == file_id:
        rest = [f for f, m in file_meta.items() if m.get("sha256") == sha]
        if rest:
            sha_index[sha] = max(rest, key=lambda f: file_meta[f].get("created_epoch") or 0)
        else:
            sha_index.pop(sha, None)
    return meta


def age_days(value: str, local: bool = False):
    """Days since a stored timestamp, or None if it cannot be read. Two formats live in this
    process: ledger/inbox/refusal rows are ISO-UTC, the log rings are naive local strings -
    the sweep has to age both, and a row it cannot parse is kept rather than deleted."""
    if not value:
        return None
    try:
        if local:
            seen = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return (datetime.now() - seen).total_seconds() / 86400.0
        seen = datetime.fromisoformat(value)
        if seen.tzinfo is None:
            seen = seen.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - seen).total_seconds() / 86400.0
    except (TypeError, ValueError):
        return None


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


def awaiting_answer(entry: dict) -> bool:
    """True when everything that came back was an ACK: the agent took the task and has not
    answered it. This is the distinction the ledger could not make before - a row with one
    result used to look identical whether that result was the work or just 'received'. Additive:
    `state` keeps its delivered|acked|answered|expired values, so no existing consumer changes."""
    res = entry.get("results") or []
    return bool(res) and all(str(r.get("kind") or "") == "ack" for r in res)


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
        wedged = []
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
            if ACKED_TTL_SECONDS:
                for entry in list(RESULTS.values()):
                    if entry.get("wedged") or entry["state"] == "expired":
                        continue
                    if not awaiting_answer(entry):
                        continue
                    try:
                        since = datetime.fromisoformat(entry["updated"] or "")
                    except (TypeError, ValueError):
                        continue
                    if (now - since).total_seconds() < ACKED_TTL_SECONDS:
                        continue
                    entry["wedged"] = True
                    entry["wedged_at"] = stamp()
                    wedged.append(dict(entry))
                    dead_letter_put(entry, "acked_silence")
        for entry in dead:
            log_event("TASK_EXPIRED", entry["agent"].partition(":")[2], "task abandoned",
                      f"[{entry['msg_id']}] delivered {entry['delivered_at']}, no result ever "
                      f"arrived within {TASK_TTL_SECONDS}s",
                      frm=entry.get("from", ""), to=entry["agent"].partition(":")[2],
                      ref=entry["msg_id"])
        for entry in wedged:
            log_event("TASK_WEDGED", entry["agent"].partition(":")[2], "acked, never answered",
                      f"[{entry['msg_id']}] ACKed and nothing but ACKs since: silent for "
                      f"{ACKED_TTL_SECONDS}s",
                      note="Row kept - a late answer still lands.",
                      frm=entry.get("from", ""), to=entry["agent"].partition(":")[2],
                      ref=entry["msg_id"])
        retention_tick()
        unread_tick()
        # buckets only hold rows that are still in the ring, so reclaim what the ring forgot:
        # otherwise retired agent ids occupy EVENT_INDEX_MAX_IDS slots and a new agent's
        # /events/mine reads empty forever
        prune_event_index()


LAST_SWEEP = {}              # what the most recent retention sweep removed (echoed on /health)
_next_sweep_at = 0.0


def retention_sweep(dry=None, actor="operator") -> dict:
    """Age out anything this hub has held longer than RETENTION_DAYS.

    The count caps (3000 log rows, 500 ledger rows, 200 dead-letter) bound a BUSY hub; a quiet
    one keeps a two-week-old deliverable, its ledger row and its log rows forever, and the file
    store had no age limit at all before this. Nothing here touches live state: connected
    agents, their queues, and agent_epoch (credential revocation) are never aged - a stale
    epoch is what makes a revoked credential stay revoked.
    """
    # the sweep is the one thing that shrinks the ring without a log_event, and the page now
    # renders from the ring, so without this the mirror keeps serving rows the ring has dropped
    # until the next event marks it dirty
    global log_page_dirty
    report = {"retention_days": RETENTION_DAYS, "enabled": RETENTION_DAYS > 0}
    if RETENTION_DAYS <= 0:
        report["note"] = "HUB_RETENTION_DAYS=0: nothing ages out, only the count caps apply"
        return report
    dry = RETENTION_DRY_RUN if dry is None else bool(dry)
    report["dry_run"] = dry
    now = time.time()
    with reg_lock:
        live = set(agents)

    aged_files, freed = [], 0
    with meta_lock:
        for fid, meta in list(file_meta.items()):
            age = age_days(meta.get("created_iso"))
            if age is None:
                age = (now - (meta.get("created_epoch") or now)) / 86400.0
            if age > RETENTION_DAYS:
                aged_files.append((fid, meta, round(age, 1)))
        removed_files = []
        for fid, meta, age in aged_files:
            entry = {"file_id": fid, "name": meta.get("name"), "size": meta.get("size"),
                     "age_days": age, "by": meta.get("by"),
                     "shared_with": meta.get("shared_with") or []}
            if dry:
                removed_files.append(entry)
                freed += meta.get("size") or 0
                continue
            try:
                drop_file_locked(fid)
            except OSError:
                continue          # stays indexed; the next sweep gets another shot at the bytes
            removed_files.append(entry)
            freed += meta.get("size") or 0
        if removed_files and not dry:
            persist_file_index()      # once for the whole sweep, not once per object

        aged_ledger = [m for m, r in RESULTS.items()
                       if (age_days(r.get("delivered_at")) or 0) > RETENTION_DAYS]
        if not dry:
            for m in aged_ledger:
                RESULTS.pop(m, None)

        aged_dl = [d for d in list(DEAD_LETTER)
                   if (age_days(d.get("died")) or 0) > RETENTION_DAYS]
        if not dry and aged_dl:
            for d in list(DEAD_LETTER):
                if (age_days(d.get("died")) or 0) > RETENTION_DAYS:
                    DEAD_LETTER.remove(d)

        aged_msgs, retired_queues = 0, []
        for aid, q in list(client_inboxes.items()):
            msgs = list(q)
            aged_msgs += sum(1 for m in msgs
                             if (age_days(m.get("ts")) or 0) > RETENTION_DAYS)
            if aid not in live and (age_days(agent_last_seen.get(aid)) or 0) > RETENTION_DAYS:
                retired_queues.append(aid)       # an agent nobody has seen for a fortnight
            elif not dry:
                fresh = [m for m in msgs if (age_days(m.get("ts")) or 0) <= RETENTION_DAYS]
                if len(fresh) != len(msgs):
                    client_inboxes[aid] = deque(fresh, maxlen=INBOX_QUEUE_MAX)
        if not dry:
            for aid in retired_queues:
                client_inboxes.pop(aid, None)

        # The relay queue and the unannounced-file markers are the unread nudge's memory. Left
        # alone they outlive every name anyone ever relayed to, so they age the same way.
        aged_mail, retired_ids = 0, []
        for aid in set(agent_mail) | set(files_unannounced):
            msgs = list(agent_mail.get(aid) or ())
            aged_mail += sum(1 for m in msgs
                             if (age_days(m.get("ts")) or 0) > RETENTION_DAYS)
            pend = files_unannounced.get(aid) or {}
            if aid not in live and (age_days(agent_last_seen.get(aid)) or 0) > RETENTION_DAYS:
                retired_ids.append(aid)
            elif not dry:
                fresh = [m for m in msgs if (age_days(m.get("ts")) or 0) <= RETENTION_DAYS]
                if len(fresh) != len(msgs):
                    agent_mail[aid] = deque(fresh, maxlen=MAIL_QUEUE_MAX)
                for fid, at in list(pend.items()):
                    if (age_days(at) or 0) > RETENTION_DAYS:
                        pend.pop(fid, None)
                if not pend:
                    files_unannounced.pop(aid, None)
        if not dry:
            for aid in retired_ids:
                agent_mail.pop(aid, None)
                files_unannounced.pop(aid, None)

        aged_seen = [a for a, s in agent_last_seen.items()
                     if a not in live and (age_days(s) or 0) > RETENTION_DAYS]
        if not dry:
            for a in aged_seen:
                agent_last_seen.pop(a, None)

        aged_rejects = [a for a, r in id_rejects.items()
                        if (age_days(str((r or {}).get("at") or "")) or 0) > RETENTION_DAYS]
        if not dry:
            for a in aged_rejects:
                id_rejects.pop(a, None)

    with log_lock:
        events = list(LOG_EVENTS)
    stale_rows = 0
    for e in events:
        if (age_days(e.get("ts"), local=True) or 0) > RETENTION_DAYS:
            stale_rows += 1
        else:
            break                      # the rings are chronological; the first fresh row ends it
    if not dry and stale_rows:
        with log_lock:
            for _ in range(stale_rows):
                if not LOG_EVENTS:
                    break
                LOG_EVENTS.popleft()
            log_page_dirty = True

    report["removed"] = {
        "files": len(removed_files), "bytes_freed": freed,
        "ledger_rows": len(aged_ledger), "dead_letter_rows": len(aged_dl),
        "log_rows": stale_rows, "inbox_messages": aged_msgs,
        "retired_agent_queues": len(retired_queues),
        "queued_relays": aged_mail, "retired_relay_queues": len(retired_ids),
        "last_seen_entries": len(aged_seen), "id_rejections": len(aged_rejects),
    }
    # The store is shared since v1.11, so there is no per-agent file slice to report: every
    # caller of GET /retention sees the same list of what ages out.
    report["files"] = removed_files[:25]
    report["files_listed"] = len(removed_files)
    report["files_truncated"] = len(removed_files) > 25
    total = sum(v for k, v in report["removed"].items() if k != "bytes_freed")
    if total and not dry:
        LAST_SWEEP.clear()
        LAST_SWEEP.update({"at": stamp(), "actor": actor, **report["removed"]})
        # `-` keeps the subject empty on purpose: `actor` here is a TRIGGER name from the sweep
        # call sites ("reaper", "startup", "operator"), not an agent, and passing it as the
        # subject would mint an EVENT_INDEX bucket for a caller that can never poll.
        log_event("HOUSEKEEP", "-", "retention sweep",
                  f"aged out > {RETENTION_DAYS}d (trigger {actor}): "
                  f"{report['removed']['files']} file(s) "
                  f"({_humansize(freed)} freed), {report['removed']['ledger_rows']} ledger row(s), "
                  f"{report['removed']['log_rows']} log row(s), "
                  f"{report['removed']['inbox_messages']} queued message(s), "
                  f"{report['removed']['retired_agent_queues']} retired queue(s)",
                  frm="hub", to="store")
    if not dry and report["removed"]["log_rows"]:
        prune_event_index()      # the sweep popped ring rows; the /events/mine index forgets them
    return report


def retention_tick() -> None:
    """Called from the reaper loop; performs the periodic sweep when it is due."""
    global _next_sweep_at
    if not RETENTION_DAYS:
        return
    now = time.time()
    if now < _next_sweep_at:
        return
    _next_sweep_at = now + RETENTION_SWEEP_SECONDS
    try:
        r = retention_sweep(actor="reaper")
    except Exception as exc:  # noqa: BLE001 - housekeeping must never kill the reaper
        print(f"[agent-hub] WARN: retention sweep failed: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return
    n = sum((r.get("removed") or {}).values())
    if n:
        print(f"[agent-hub] retention sweep removed {n} aged item(s) "
              f"(> {RETENTION_DAYS}d, dry_run={r.get('dry_run')})")


# ----------------------------------------------------------------- unread nudge (v1.9)
def mail_put(agent_id: str, sender: str, text: str) -> tuple:
    """Queue a relay addressed to an agent with no live socket, instead of destroying it.
    Returns (msg_id, queue_depth). Never call while holding a lock (it takes meta_lock itself).
    Bounded twice over - 200 messages per id drop-oldest, and MAIL_AGENTS_MAX ids with the least
    recently touched one evicted - because a caller may relay to names that never connect."""
    msg = {"msg_id": pysecrets.token_hex(6), "from": sender, "text": text, "ts": stamp()}
    overflow = []
    with meta_lock:
        q = agent_mail.get(agent_id)
        if q is None:
            while len(agent_mail) >= MAIL_AGENTS_MAX:
                cold_id, cold_q = agent_mail.popitem(last=False)
                files_unannounced.pop(cold_id, None)
                if cold_q:
                    overflow.append((cold_id, len(cold_q), "id cap"))
            q = agent_mail[agent_id] = deque(maxlen=MAIL_QUEUE_MAX)
        else:
            agent_mail.move_to_end(agent_id)
        full = len(q) == q.maxlen
        q.append(msg)
        depth = len(q)
    if full:
        overflow.append((agent_id, 1, "queue full"))
    for who, count, why in overflow:      # logged after meta_lock releases, like every other path
        log_event("MSG_FAIL", who, "queue dropped",
                  f"queued relay(s) for '{who}' dropped ({why}): {count} message(s) are gone "
                  f"and were never delivered",
                  note=f"caps are {MAIL_QUEUE_MAX} per agent, {MAIL_AGENTS_MAX} agents",
                  frm="mail", to=who)
    return msg["msg_id"], depth


def unread_state(agent_id: str) -> dict:
    """One meta_lock pass over everything this hub holds for one agent: the totals to report
    and the items that agent has not been told about yet. Takes meta_lock itself, so never call
    it while holding reg_lock - the hub does not nest those two. Tasks means rows still
    `delivered` with no result frame at all: 'the hub never heard back', NOT proof the agent
    never saw it."""
    prefix = f"agent:{agent_id}"
    with meta_lock:
        tasks = [mid for mid, row in RESULTS.items()
                 if row.get("agent") == prefix and row.get("state") == "delivered"
                 and not row.get("results") and row.get("hub_issued")]
        pending_files = list((files_unannounced.get(agent_id) or {}).keys())
        file_rows = [{"file_id": fid, "url": f"/file/{fid}",
                      **{k: v for k, v in (file_meta.get(fid) or {}).items()
                         if k in ("name", "size", "sha256")}}
                     for fid in pending_files[:UNREAD_LIST_MAX]]
        queued = list(agent_mail.get(agent_id) or ())
        new_tasks = [m for m in tasks if f"{agent_id}|{m}" not in nudged_tasks]
    return {"tasks": tasks, "new_tasks": new_tasks, "pending_files": pending_files,
            "file_rows": file_rows, "mail": queued[:MAIL_FLUSH_MAX], "queued": len(queued)}


def unread_nudge(agent_id: str, sid: str, reason: str = "tick") -> int:
    """Tell one connected agent what it has not been told, and release its queued relays to it
    as ordinary peer_msg. Items are announced once per agent and never twice: a file leaves
    `files_unannounced` when it is announced, a queued relay leaves its queue when it is
    flushed, and a task is remembered in `nudged_tasks` because its ledger row stays
    `delivered` whether or not anyone ever answers. So an agent with one abandoned task is
    told about it once, not every 45 seconds. Returns the number of items announced. The
    HUB_UNREAD_NUDGE_SECONDS=0 switch belongs to unread_tick, not here: an agent that just
    joined must still be told what it missed, or a queued relay would sit there unreadable."""
    global nudge_total
    st = unread_state(agent_id)
    new_tasks, mail, files = st["new_tasks"], st["mail"], st["file_rows"]
    if not new_tasks and not files and not mail:
        return 0
    listed = new_tasks[:UNREAD_LIST_MAX]
    payload = {"agent_id": agent_id, "reason": reason, "at": stamp(),
               "unread": {"tasks": len(st["tasks"]), "files": len(st["pending_files"]),
                          "relays": st["queued"]},
               "task_ids": listed,
               "tasks_truncated": len(new_tasks) > UNREAD_LIST_MAX,
               "files": files,
               "files_truncated": len(st["pending_files"]) > UNREAD_LIST_MAX,
               "relays_flushed": len(mail),
               "note": "counts are what this hub still holds for you; lists are what it has "
                       "not told you yet. A task reads `delivered` because no result frame ever "
                       "came back - that is the hub's blind spot, not proof you never saw it. "
                       "Expired tasks are not listed (see /tasks/dead-letter); pull a listed "
                       "file with GET /file/<file_id> and your X-Agent-Token."}
    if listed:
        payload["tasks_how"] = ("read the task text back with GET /result/<msg_id> - the ledger "
                                "keeps the first 200 chars of what was sent")
    try:
        socketio.emit("unread", payload, to=sid, namespace=NS)
        for msg in mail:
            socketio.emit("peer_msg", {"from": msg["from"], "text": msg["text"],
                                       "msg_id": msg["msg_id"], "queued_at": msg["ts"]},
                          to=sid, namespace=NS)
    except Exception as exc:  # noqa: BLE001 - a socket that died mid-notice is not a crash
        log_event("MSG_FAIL", agent_id, "unread notice failed",
                  f"{type(exc).__name__}: {exc}",
                  note="nothing was announced; this agent keeps its unread items and will be "
                       "told again next tick",
                  frm="hub", to=agent_id)
        print(f"[agent-hub] WARN: unread nudge to '{agent_id}' failed: "
              f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 0
    with meta_lock:
        pending_ids = files_unannounced.get(agent_id)
        for row in files:
            if pending_ids is not None:
                pending_ids.pop(row["file_id"], None)
        for mid in listed:
            nudged_tasks[f"{agent_id}|{mid}"] = stamp()
            while len(nudged_tasks) > 2000:
                nudged_tasks.popitem(last=False)
        if mail:
            q = agent_mail.get(agent_id)
            if q is not None:
                for _ in mail:      # `mail` is the oldest items, so drain from the left
                    q.popleft()
                # Only the queue goes. files_unannounced must NOT be dropped here: a notice names
                # at most UNREAD_LIST_MAX files, so an agent owed 15 of them still has 5 markers
                # live, and clearing the dict would lose those bytes' announcement for good.
                if not q:
                    agent_mail.pop(agent_id, None)
        nudge_total += 1
    log_event("UNREAD_NUDGE", agent_id, f"unread notice ({reason})",
              f"outstanding: {payload['unread']['tasks']} task(s), "
              f"{payload['unread']['files']} file(s), {payload['unread']['relays']} relay(s) - "
              f"announced now: {len(listed)} task(s), {len(files)} file(s), {len(mail)} relay(s)",
              frm="hub", to=agent_id)
    return len(listed) + len(files) + len(mail)


def unread_tick() -> None:
    """Called from the reaper loop; nudges every live agent when the cadence is due. Standby
    sockets are deliberately skipped - their credential was revoked by the take-over, so a
    notice would be an order they cannot fill."""
    global _next_nudge_at
    if not UNREAD_NUDGE_SECONDS:
        return
    now = time.time()
    if now < _next_nudge_at:
        return
    _next_nudge_at = now + UNREAD_NUDGE_SECONDS
    with reg_lock:
        live = dict(agents)
    for agent_id, sid in live.items():
        try:
            unread_nudge(agent_id, sid, reason="tick")
        except Exception as exc:  # noqa: BLE001 - one wedged agent must not stop the sweep
            log_event("MSG_FAIL", agent_id, "unread notice failed",
                      f"{type(exc).__name__}: {exc}",
                      note="this agent keeps its unread items and will be told again next tick",
                      frm="hub", to=agent_id)


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
    log_event("SCOPE_DENY", mine, "refused",
              f"asked for {what} belonging to '{tried}'",
              note="agent credentials are per-agent; the operator's AGENT_AUTH_TOKEN is the "
                   "all-seeing one",
              frm=mine, to="hub")
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
# The endpoint manifest, the onboarding page and /llms.txt are one table, rendered by hubdocs.py,
# so the three cannot disagree about what an endpoint returns. The hub version and the upload
# ceiling are passed in rather than copied into prose, because a ceiling written into a doc string
# goes on advertising 25 MB after the code moved to 100.
DOCS = hubdocs.build(HUB_VERSION, MAX_UPLOAD)


def onboarding_response(status: int) -> Response:
    return Response(DOCS.onboarding_html, status=status, content_type="text/html; charset=utf-8")


def require_actor():
    """Gate an HTTP endpoint behind a principal: an agent credential (minted at socket connect)
    or the operator's AGENT_AUTH_TOKEN. Returns None if allowed, else a content-negotiated 401
    (JSON for agents, onboarding HTML for browsers)."""
    if authorized():
        return None
    master_only = (not OPERATOR_HTTP
                   and token_eq(presented_token(), AGENT_TOKEN or ""))
    # There is no id to name here: an invalid credential authenticates as nobody, and the
    # master_only case is the OPERATOR being refused, not a client. actor_label() would have
    # answered "client" for both and put that word in the agent column.
    log_event("AUTH_FAIL", "-", "rejected",
              f"{request.method} {request.path} "
              + ("operator token not accepted over HTTP (HUB_OPERATOR_HTTP=0)"
                 if master_only else "missing/invalid credential or AGENT_AUTH_TOKEN"),
              frm="operator" if master_only else "client", to="hub")
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
                                      "is, scopes /result and inboxes to that agent_id (the file "
                                      "store is shared since v1.11), and "
                                      "is revoked when the socket dies or the id is taken over"},
                         "model": "since v1.5 identity comes from the credential that "
                                  "authenticated, never from X-Agent-Id; a master-token caller's "
                                  "X-Agent-Id is still a self-declared label"},
                   headers={"ngrok_free_tier": "send ngrok-skip-browser-warning: true on every request"},
                   endpoints=DOCS.endpoints, socket=DOCS.socket, footguns=DOCS.footguns)


@app.get("/llms.txt")
def llms_txt():
    return Response(DOCS.llms_md, content_type="text/markdown; charset=utf-8")


@app.get(FAVICON_ROUTE)
def favicon():
    """Unauthenticated: browsers fetch it before any token could apply."""
    if not FAVICON_FILE.exists():
        abort(404)
    return send_file(FAVICON_FILE, mimetype="image/x-icon", max_age=86400)


# Measured, not estimated (bench17/hubload.py over HTTP against a disposable filled to every cap,
# reading /proc/<pid>/status VmRSS; bench17/memprofile.py for the per-structure attribution):
#   65,440 KB cold  ->  139,012 KB with 3000 log rows x 4 KB payloads + 500 ledger rows
#                      + 500 stored files + 200 dead-letter rows + 8 live sockets
#               (a repeat run of the same build read 65,400 -> 139,464: the spread is ~0.5 MB)
# The two in-memory log rings were 52.5 MB of that 74 MB growth - filled linearly at ~17.5 KB per
# 4 KB-payload row (8.6 KB of it string content, the rest object overhead). Ledger 252 KB, file
# index 440 KB, dead-letter 48 KB, and the /events/mine index measured +0 KB because it stores
# references to the ring's own row dicts rather than copies.
# MEASURED PRE-v1.10, BEFORE the HTML ring was deleted: those numbers describe two rings and
# bench17/ is not in this repo, so they are quoted as history, not as this build's ceiling. One
# ring of the same cap holds strictly less than two did; re-measure before restating a figure.
MEMORY_CEILING_KB = 139012
MEMORY_COLD_KB = 65440
MEMORY_DOMINATES = ("the log ring (LOG_EVENTS, structured, rendered on read): it, plus the HTML "
                    "twin this build deleted, measured 52.5 MB of a 74 MB growth, ~17.5 KB per "
                    "4 KB-payload row. Ledger 252 KB, file index 440 KB, dead-letter 48 KB, "
                    "events/mine index +0 KB (shared row dicts)")
MEMORY_METHOD = ("bench17/hubload.py (live VmRSS at every cap) + bench17/memprofile.py "
                 "(per-structure deltas)")


def _memory_block() -> dict:
    """Additive /health key: where this process sits versus the measured ceiling."""
    rss = None
    try:                                    # Linux only; a hub elsewhere just reports no rss
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1])
                break
    except (OSError, ValueError):
        pass
    return {"rss_kb": rss, "measured_ceiling_kb": MEMORY_CEILING_KB,
            "measured_cold_kb": MEMORY_COLD_KB, "caps": {
                "log_rows": LOG_MAX_ROWS, "payload_chars": 4000,
                "ledger_rows": TASK_LEDGER_MAX, "dead_letter": DEAD_LETTER_MAX},
            "dominates": MEMORY_DOMINATES, "method": MEMORY_METHOD,
            "note": "ceiling is this build measured at every cap on a disposable, not a limit: "
                    "the caps are the actual bound and only the log rings are large"}


@app.get("/health")
def health():
    with reg_lock:
        n = len(agents)
    # separate lock pass, not nested: iterating these while mail_put inserts raises RuntimeError
    with meta_lock:
        queued = sum(len(q) for q in agent_mail.values())
        unannounced = sum(len(p) for p in files_unannounced.values())
        mail_ids = len(agent_mail)
    return jsonify(status="ok", agents_connected=n, uptime="see logs",
                   version=HUB_VERSION, features=FEATURES,
                   uptime_seconds=int(time.time() - STARTED_AT),
                   docs="/llms.txt", api="/api",
                   public_url=TUNNEL_URL or None,
                   memory=_memory_block(),
                   log_mirror={"writes": log_mirror_writes, "rows_written": log_written_rows,
                               "interval_seconds": LOG_WRITE_INTERVAL_SECONDS,
                               "dirty": log_page_dirty,
                               "note": "the on-disk page is a mirror: writes are bounded by the "
                                       "flush cadence, not by event count. /logs and events.json "
                                       "always render from the rings"},
                   retention={"days": RETENTION_DAYS, "enabled": RETENTION_DAYS > 0,
                              "dry_run_mode": RETENTION_DRY_RUN,
                              "sweep_every_seconds": RETENTION_SWEEP_SECONDS,
                              "next_sweep_in": max(0, int(_next_sweep_at - time.time())) or None,
                              "last_sweep": LAST_SWEEP or None},
                   unread={"every_seconds": UNREAD_NUDGE_SECONDS, "enabled": bool(UNREAD_NUDGE_SECONDS),
                           "next_nudge_in": (max(0, int(_next_nudge_at - time.time())) or None)
                                            if UNREAD_NUDGE_SECONDS else None,
                           "queued_relays": queued,
                           "queued_for_agents": mail_ids,
                           "files_unannounced": unannounced,
                           "notices_total": nudge_total, "list_max": UNREAD_LIST_MAX,
                           "mail_queue_max": MAIL_QUEUE_MAX,
                           "note": "`enabled` is the periodic sweep only - the connect and "
                                   "reactivation notices always fire, which is why next_nudge_in "
                                   "is null when it is off. Queued relays are memory-only: a hub "
                                   "restart loses them."})


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
        mail = {aid: len(q) for aid, q in agent_mail.items()}
        outstanding: dict = {}
        ack_only: dict = {}
        for entry in RESULTS.values():
            if entry["state"] in ("delivered", "acked"):
                aid = entry["agent"].partition(":")[2]
                outstanding[aid] = outstanding.get(aid, 0) + 1
                if awaiting_answer(entry):
                    ack_only[aid] = ack_only.get(aid, 0) + 1
        dead = len(DEAD_LETTER)
    return jsonify(agents=snap,                       # frozen shape: {id: sid}
                   count=len(snap), agent_ids=sorted(snap),
                   detail={aid: {"sid": sid, "connected_at": since.get(aid),
                                 "last_seen": seen.get(aid),
                                 "last_superseded_at": sup.get(aid),
                                 "standby_sockets": len(std.get(aid) or {}),
                                 "inbox_backlog": backlog.get(aid, 0),
                                 "mail_backlog": mail.get(aid, 0),
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
                                "awaiting_answer_by_agent": ack_only,
                                "stranded": {aid: n for aid, n in outstanding.items()
                                             if aid not in snap},
                                "dead_letter": dead, "ttl_seconds": TASK_TTL_SECONDS,
                                "acked_ttl_seconds": ACKED_TTL_SECONDS,
                                "expiry_note": "outstanding counts rows in state delivered or "
                                               "acked. awaiting_answer_by_agent counts rows whose "
                                               "only results are ACKs: the agent took the work and "
                                               "has not answered it. A delivered row with no result "
                                               "at all goes expired at ttl_seconds; an ack-only row "
                                               "goes to /tasks/dead-letter as acked_silence only "
                                               "when acked_ttl_seconds>0, and its row is kept. Read "
                                               "last_seen next to outstanding_tasks for the wedged "
                                               "case either way.",
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
        # The old hint blamed case-sensitivity for every refusal, which sent a caller off to
        # rename a perfectly legal id when the real fault was an illegal character (pain d).
        illegal = "".join(dict.fromkeys(c for c in agent_id if c not in _LEGAL_ID_CHARS))
        return _err(400, "invalid agent_id", pattern="[A-Za-z0-9_-]{1,40}",
                    rejected=agent_id[:60], illegal_chars=illegal[:16] or None,
                    too_long=(len(agent_id) > 40) or None,
                    hint=f"'{agent_id[:40]}' is not a legal agent_id: only [A-Za-z0-9_-], 1-40"
                         f" characters. Case is never the fault - 'Scout' and 'scout' are both"
                         f" legal (they are two different agents)")
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
        log_event("MSG_FAIL", agent_id, "agent offline",
                  "no persistent socket; request refused instantly",
                  frm=actor_label(), to=agent_id)
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

    try:
        wait = min(max(float(request.args.get("wait", ACK_TIMEOUT)), 0), 60)
    except ValueError:
        wait = ACK_TIMEOUT
    reply = None
    status = "delivered_no_ack"
    try:
        socketio.emit("task", {"msg_id": msg_id, "from": caller, "text": text},
                      to=sid, namespace=NS)
        # `caller` was already computed for the ledger two lines up and used to be thrown away:
        # this row said only "Client -> Server -> Agent", so 19 of the 50 MSG_SENT rows in a
        # session named nobody, and the only way to learn who had tasked an agent was the
        # "[hubmaster -> qoder]" prefix that senders had started typing into message bodies.
        log_event("MSG_SENT", agent_id, "HTTP task",
                  f"[{msg_id}] {eprint_summary(text)}",
                  frm=caller, to=agent_id, ref=msg_id)
        if wait > 0 and pentry["event"].wait(timeout=wait):
            reply = pentry["replies"][-1] if pentry["replies"] else None
            log_event("MSG_RCVD", agent_id, "HTTP reply",
                      f"[{msg_id}] {eprint_summary(reply)}",
                      frm=agent_id, to=caller, ref=msg_id)
            status = "replied"
    finally:
        # if emit or logging raises, the error handler still owes nobody a stuck pending
        # entry: pending.pop must run on every path or the dict grows per failed task.
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
    # the caller owns the row too: POST /agent/<id>/message hands back a
    # result_endpoint, and agent->agent tasking must be pollable by the asker.
    # `from` is credential-derived, so this cannot be forged.
    if (mine and mine not in (owner, (out.get("answered_by") or "").partition(":")[2])
            and out.get("from") != f"agent:{mine}"):
        return scope_violation(owner or "?", "a task ledger row")
    out["status"] = "done" if len(out.get("results", [])) > 1 else out.get("status", "first_result")
    out["count"] = len(out.get("results", []))
    out["awaiting_answer"] = awaiting_answer(out)
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
        rows = [{**r, "awaiting_answer": awaiting_answer(r)} for r in DEAD_LETTER]
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
    dead_note = (f"reason: expired = no result within ttl_seconds | acked_silence = ACKed but "
                 f"nothing but ACKs for {ACKED_TTL_SECONDS}s (row kept) | evicted = the row was pushed "
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
    mentions = request.args.get("mentions", "").lower() in ("1", "true", "yes")
    with log_lock:
        if mentions:
            # the pre-index behaviour, kept reachable: id anywhere in the row, prose included.
            # It must stay a SUPERSET of the index or the ?mentions=1 comparison inverts, so it
            # tests the same three structural ends the index buckets on, then adds prose.
            # whole-word matching only: a plain substring test let agent 'lnk' see every row
            # about 'lnk-x' (prefix-collision leak between ids that share a prefix).
            tag_re = re.compile(r"(?:agent|Agent):" + re.escape(aid) + r"(?![A-Za-z0-9_-])")
            tok_re = re.compile(r"(?<![A-Za-z0-9_-])@?" + re.escape(aid) + r"(?![A-Za-z0-9_-])")
            rows = [e for e in LOG_EVENTS
                    if aid in (_who(e.get("agent", ""))[1], _who(e.get("frm", ""))[1],
                               _who(e.get("to", ""))[1])
                    or tag_re.search(e.get("dir") or "") or tok_re.search(e.get("dir") or "")
                    or tag_re.search(e.get("payload") or "")
                    or tok_re.search(e.get("payload") or "")]
        else:
            bucket = EVENT_INDEX.get(aid)
            if not bucket:
                rows = []
            else:
                # a bucket can outlive the ring, so drop the pairs the ring already evicted;
                # the ring is still the only source of truth, this is just its index
                floor = LOG_SEQ - len(LOG_EVENTS) + 1
                while bucket and bucket[0][0] < floor:
                    bucket.popleft()
                rows = [row for _seq, row in bucket]
    # `indexed` tells a caller whether an empty answer means "no rows" or "your id is not in the
    # index" (over EVENT_INDEX_MAX_IDS the coldest bucket is dropped) - only the scan can see a
    # mention in payload prose, so the two cases are not the same and must not look the same.
    in_index = aid in EVENT_INDEX
    return jsonify(events=rows[-limit:], count=min(len(rows), limit), total_matching=len(rows),
                   caller=aid, indexed_ids=len(EVENT_INDEX),
                   indexed=in_index or mentions,
                   note="rows where your id is the subject (agent), the sender (from) or the "
                        "receiver (to) - since v1.10 those come from the row's own from/to "
                        "fields rather than from a sentence, so an event is no longer invisible "
                        "in your feed because the code path that wrote it worded the direction "
                        "without an 'agent:' tag; add ?mentions=1 to also scan prose, which is "
                        "the slow whole-ring path; indexed=false means your id is not in the "
                        "index at all, so re-read with ?mentions=1 before believing an empty "
                        "count; payload_full carries up to 4000 chars")


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
        mid, depth = mail_put(to, sender, text)
        log_event("MSG_QUEUED", to, "mail queued", eprint_summary(text),
                  frm=sender, to=to, ref=mid)
        return jsonify(status="queued", to=to, msg_id=mid, queue_depth=depth,
                       note=f"'{to}' has no live socket, so this text was queued in memory "
                            f"(drop-oldest at {MAIL_QUEUE_MAX}) instead of dropped. It arrives as "
                            "an ordinary peer_msg on that agent's next connect or on the next "
                            f"unread notice - whichever comes first"
                            + (". HUB_UNREAD_NUDGE_SECONDS=0 stops the periodic sweep, so this "
                               "queue only drains when the agent rejoins"
                               if not UNREAD_NUDGE_SECONDS else ""))
    socketio.emit("peer_msg", {"from": sender, "text": text}, to=sid, namespace=NS)
    log_event("MSG_SENT", to, "relay", eprint_summary(text), frm=sender, to=to)
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
    """Push a file_ready notice to `target`. Returns (delivered, target_error). A target that
    had no socket is recorded in `files_unannounced` so the unread nudge can tell it about the
    bytes when it rejoins - the file was always durable, only the notice was ever lost."""
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
        with meta_lock:
            heard = files_unannounced.get(target)
            if heard is not None:
                heard.pop(file_id, None)     # it heard about this file directly: nothing owed
        return True, ""
    with meta_lock:
        files_unannounced[target][file_id] = stamp()
    log_event("MSG_FAIL", target, "offline",
              f"file {meta['name']} stored; no socket to notify",
              note="the unread nudge will tell this agent about the bytes when it connects",
              frm=sender, to=target, ref=file_id)
    return False, (f"target agent '{target}' is offline; file stored, no notify sent - it is "
                   "queued for that agent's next unread notice")


def _record_shared(file_id: str, target: str) -> None:
    """Dedupe path: reusing older bytes must still record the new recipient on them."""
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
        # Addressing is still the sharing event worth recording, but since v1.11 it grants
        # nothing: every authenticated agent can already read the whole store.
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
        # FILE_SENT's subject used to be the NOTIFY RECIPIENT while the uploader lived only in the
        # prose - the row read "[FILE_SENT] qoder2 | agent:hubmaster -> Server -> File store" for a
        # file hubmaster uploaded and qoder2 never received, because qoder2 had no socket that
        # session. A human scanning the bold column read a false sentence. The uploader is now the
        # sender end and the recipient shows as the subject chip.
        log_event("FILE_SENT", target or "-", "deduped",
                  f"{name} ({_humansize(len(data))}, sha={sha[:12]}"
                  + (f", pushed={'yes' if delivered else 'no'}" if target else "") + ")",
                  note=f"bytes already stored, reused existing id {dup}",
                  frm=sender, to="store", ref=dup)
        out = {"status": "existing", "file_id": dup, "deduped": True, "name": name,
               "size": len(data), "sha256": sha, "delivered": delivered,
               "delivered_to": target if target else None, "download_url": f"/file/{dup}",
               "first_uploaded_by": reused.get("by"), "bytes_stored": False,
               "dedupe_note": "no new object written; this id is a byte-for-byte match"}
        if target_error:
            out["target_error"] = target_error
        return jsonify(out), 200

    delivered, target_error = _notify_target(file_id, meta, target, sender)
    log_event("FILE_SENT", target or "-", "stored",
              f"{name} ({_humansize(len(data))}, sha={sha[:12]}"
              + (f", pushed={'yes' if delivered else 'no'}" if target else "") + ")",
              frm=sender, to="store", ref=file_id)
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
    """Stored file metadata. `?ids=a,b` answers with just those rows and `?limit=N` with the N
    newest: a caller that wants one row should not have to copy and serialize the whole table
    (measured: 500 rows = 134 KB and 417 ms at 50-way under traffic). No params = legacy shape.
    Since v1.11 the whole store lists to any authenticated principal: agents share the work
    project, so hiding the table hid work, not secrets."""
    if deny := require_actor():
        return deny
    want = [f.strip() for f in request.args.get("ids", "").split(",") if f.strip()][:500]
    try:
        limit = max(0, min(int(request.args.get("limit", "0")), 5000))
    except ValueError:
        limit = 0
    with meta_lock:
        if want:
            rows = {fid: file_meta[fid] for fid in want if fid in file_meta}
        else:
            rows = dict(file_meta)
    total = len(rows)
    trimmed = total > limit > 0
    if trimmed:
        rows = dict(list(rows.items())[-limit:])       # insertion order: oldest first
    out = {"files": rows, "count": len(rows)}
    if want or trimmed:
        out["total_matching"] = total
        out["trimmed"] = bool(trimmed)
        out["trim_note"] = ("count is what this answer carries, total_matching what the store "
                            "holds for you; drop ids=/limit= for the whole table")
    out["scope_note"] = "the store is shared: every authenticated principal lists and downloads " \
                        "every object; only DELETE is owner-scoped (v1.11)"
    return jsonify(out)


def _serve_file(file_id: str, who: str):
    """Stream one stored object and log the pull. Shared by GET /file/<id> (agent credential or
    operator token) and the /logs page download link, so a download is the same row either way."""
    with meta_lock:
        meta = file_meta.get(file_id)
    if not meta:
        return _err(404, "unknown file_id", hint="list what exists: GET /files", file_id=file_id)
    path = FILE_STORE / f"{file_id}__{meta['name']}"
    if not path.exists():
        return _err(404, "file bytes missing from store",
                    hint=f"metadata exists but {meta['name']} is gone from disk", file_id=file_id)
    # The subject must be a BARE id, not actor_label()'s "agent:qoder": AGENT_ID_RE rejects the
    # colon, so every FILE_RCVD row in a session landed in no /events/mine bucket at all and an
    # agent could never see its own downloads.
    log_event("FILE_RCVD", _who(who)[1] or "-", "download",
              f"downloaded {meta['name']} ({_humansize(meta['size'])})",
              frm="store", to=who, ref=file_id)
    return send_file(path, as_attachment=True, download_name=meta["name"],
                     mimetype=meta["type"])


@app.get("/file/<file_id>")
def download_file(file_id):
    """Download stored bytes. Since v1.11 any authenticated principal may pull any object: the
    store is one shared work project, so scoping the bytes hid a teammate's output rather than
    protecting a secret."""
    if deny := require_actor():
        return deny
    return _serve_file(file_id, actor_label())


@app.delete("/file/<file_id>")
def delete_file(file_id):
    """Reclaim store space: bytes + index entry removed. The uploader (agent credential)
    or the operator token may delete; other agents get 403 - the store is shared to READ
    (v1.11) but a file parked there by someone else is not yours to destroy. The store had
    no shrink path before v1.6.0: a full-size upload was permanent."""
    if deny := require_actor():
        return deny
    mine = own_agent()
    with meta_lock:
        meta = file_meta.get(file_id)
        if not meta:
            return _err(404, "unknown file_id",
                        hint="list what exists: GET /files ; deleted ids are gone for good"
                             " (and aged out after HUB_RETENTION_DAYS, default 14)",
                        file_id=file_id)
        by = str(meta.get("by") or "")
        if mine and by != f"agent:{mine}":
            return scope_violation(by.partition(":")[2] or "?", "a stored file")
        path = FILE_STORE / f"{file_id}__{meta['name']}"
        try:
            drop_file_locked(file_id)
        except OSError as exc:
            return _err(500, "could not remove file bytes",
                        hint=f"index untouched; fix permissions on {path} and retry: {exc}",
                        file_id=file_id)
        persist_file_index()
    who = actor_label()
    log_event("FILE_DEL", _who(who)[1] or "-", "file deleted",
              f"{meta['name']} ({_humansize(meta.get('size'))}) freed",
              note="this id now 404s; other ids holding identical bytes are untouched",
              frm=who, to="store", ref=file_id)
    return jsonify(status="deleted", file_id=file_id, name=meta["name"],
                   freed_bytes=meta.get("size"),
                   note="this id now 404s; other ids holding identical bytes are untouched")


# ----------------------------------------------------------------- housekeeping
@app.get("/retention")
def retention_status():
    """What the sweep would remove right now, without removing anything - the operator's answer
    to 'how much of this is stale'. Every authenticated principal sees the whole store, since the
    store is shared (v1.11)."""
    if deny := require_actor():
        return deny
    report = retention_sweep(dry=True, actor="preview")
    report.update(config={"days": RETENTION_DAYS, "dry_run_mode": RETENTION_DRY_RUN,
                          "sweep_every_seconds": RETENTION_SWEEP_SECONDS},
                  run_endpoint="POST /retention/sweep", last_sweep=LAST_SWEEP or None)
    return jsonify(report)


@app.post("/retention/sweep")
def retention_run():
    """Sweep now instead of waiting for the hourly tick. Operator principal only: the sweep
    deletes files that belong to other agents, so a credential cannot ask for it."""
    if deny := require_actor():
        return deny
    if mine := own_agent():
        return _err(403, "retention sweeps are operator-only",
                    hint="GET /retention shows what ages out for you; DELETE /file/<id> removes "
                         "one of yours - the sweep removes everyone's",
                    your_agent_id=mine)
    dry = request.args.get("dry", "").lower() in ("1", "true", "yes")
    return jsonify(retention_sweep(dry=dry, actor="operator"))


# ----------------------------------------------------------------- log viewer
@app.get("/logs")
def logs_no_token():
    abort(404)


def _log_token_ok(token: str) -> bool:
    # token_eq, NOT pysecrets.compare_digest: a percent-encoded non-ASCII path segment
    # arrives as a unicode str and compare_digest raises TypeError on that - a free 500
    # for any anonymous GET /logs/<utf8-bytes>.
    return len(token) <= 64 and token_eq(token, LOG_SECRET)


@app.get("/logs/<token>")
def logs_view(token):
    if not _log_token_ok(token):
        log_event("AUTH_FAIL", "-", "log 404", f"invalid log token: {token[:16]!r}",
                  frm="client", to="hub")
        abort(404)
    # serve from the memory ring: fresher than the on-disk copy, and keeps the page up
    # even while LOG_FILE is unwritable (the page auto-refreshes, so it must never 500)
    try:
        n = max(1, min(int(request.args.get("n", LOG_ROUTE_ROWS)), LOG_MAX_ROWS))
    except ValueError:
        n = LOG_ROUTE_ROWS
    with log_lock:
        snapshot = list(LOG_EVENTS)
    # The page authenticates with the secret in its own URL and a browser cannot put a Bearer
    # token on an <a href>, so a file row below carries that same secret (see logs_download).
    # A ref that is no longer in the store renders no link: the bytes are gone, the row is not.
    with meta_lock:
        live = {fid: {"name": m.get("name"), "size": m.get("size"), "by": m.get("by")}
                for fid, m in file_meta.items()}
    dl = (f"/logs/{quote(LOG_SECRET, safe='')}/file/", live)
    return Response(_render_log(snapshot, {
        "agent": request.args.get("agent", "").strip()[:40],
        "event": request.args.get("event", "").strip().upper()[:24],
        "q": request.args.get("q", "").strip()[:64],
        "n": n,
        "fold": request.args.get("fold", "").lower() in ("1", "true", "yes")}, dl=dl),
        content_type="text/html; charset=utf-8")


@app.get("/logs/<token>/file/<file_id>")
def logs_download(token, file_id):
    """A stored file, pulled off the log page by the human sitting on it. Auth is the same secret
    path segment the page itself uses - the operator seat, which already reads the whole store -
    because an <a href> cannot carry an Authorization header."""
    if not _log_token_ok(token):
        log_event("AUTH_FAIL", "-", "log 404",
                  f"invalid log token on file download: {token[:16]!r}", frm="client", to="hub")
        abort(404)
    return _serve_file(file_id, "operator")


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
        log_event("AUTH_FAIL", _who(agent_id)[1] or "-", "socket rejected",
                  "missing/invalid AGENT_AUTH_TOKEN", frm=agent_id, to="hub")
        return False
    if not AGENT_ID_RE.fullmatch(agent_id):
        log_event("AUTH_FAIL", "-", "socket rejected", "missing/invalid agent_id",
                  frm=agent_id[:40], to="hub")
        return False
    force = (str(auth.get("force_takeover", "")).lower() in ("1", "true", "yes")
             or request.args.get("force", "").lower() in ("1", "true", "yes"))
    # v1.4's uniqueness check and the registration below used to sit in two separate
    # reg_lock sections: two simultaneous non-forced connects could both pass the check,
    # and the loser was silently demoted to standby instead of being refused. Check and
    # register are now one critical section, so "refuse duplicates" is atomic.
    with reg_lock:
        holder = agents.get(agent_id)
        holder_since = agent_since.get(agent_id)
        old = old_since = None
        refused = bool(holder and holder != request.sid and not force)
        if not refused:
            old = holder
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
    if refused:
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
        log_event("ID_REJECTED", agent_id, "socket refused",
                  f"duplicate agent_id; holder sid={holder[:12]}",
                  note="choose a unique id or set force_takeover",
                  frm=agent_id, to="hub")
        raise ConnectionRefusedError(reason)
    if old and old != request.sid:
        log_event("DISCONNECTED", agent_id, "replaced",
                  "stale socket replaced by fresh connection",
                  frm=agent_id, to="hub")
        try:  # tell the outgoing socket it lost routing (footgun 3 was invisible before v1.2)
            socketio.emit("superseded",
                          {"agent_id": agent_id, "reason": "duplicate agent_id re-registered",
                           "at": agent_superseded[agent_id], "sid": old,
                           "sid_note": "sid is the hub registry's /agents-namespace sid; "
                                       "compare with the client's sio.get_sid(namespace="
                                       "'/agents'), never sio.sid"}, to=old, namespace=NS)
        except Exception:  # noqa: BLE001 - old socket may already be dead
            pass
    log_event("CONNECTED", agent_id, "socket open",
              # `request.transport` does not exist on a Flask request, so this used to print
              # transport=? on every connect. The engine.io handshake carries it in the query
              # string (`?EIO=4&transport=websocket`), which is what an operator wants to see:
              # whether this agent got a real WebSocket or is falling back to polling.
              f"sid={request.sid[:12]} transport={request.args.get('transport', '?')}",
              frm=agent_id, to="hub")
    # v1.5: hand this socket its own credential. Minting here also revokes whatever the socket
    # it replaced (if any) was holding, because there is exactly one live epoch per agent_id.
    deliver_credential(agent_id, request.sid)
    # Credential first, notice second: the file handles a nudge carries are only downloadable
    # with X-Agent-Token, so an agent that hears about missed bytes before it has a key to open
    # them would be told about something it cannot act on yet.
    unread_nudge(agent_id, request.sid, reason="connect")


def _client_hint(agent_id: str, cred: str) -> dict:
    """One command a freshly connected agent can run to get the reference client. Keyed off
    the credential it was just handed, so the master token never re-enters the picture."""
    hub = (TUNNEL_URL or f"http://localhost:{HUB_PORT}").rstrip("/")
    return {
        "fetch": f'curl -fsS {hub}/client.py -H "Authorization: Bearer {cred}" '
                 f'-H "ngrok-skip-browser-warning: true" -o mock_agent.py',
        "auth_note": "that credential alone authorizes GET /client.py - you are already a "
                     "principal, so no AGENT_AUTH_TOKEN needed. -fsS so a dead credential (the "
                     "handshake hint only lives as long as your socket) fails loudly instead of "
                     "writing a 401 JSON body into mock_agent.py",
        "action_note": "a client that already has its own connect code does not need this; "
                       "it is for an agent that wants the reference implementation",
        "emit_without_a_socket": f"POST {hub}/relay, or append one JSON per line to "
                                 "state/<agent_id>/outbox.jsonl - "
                                 '{"action":"to_client|to_agent|reply|upload"}',
        "docs": f"{hub}/llms.txt",
    }


def deliver_credential(agent_id: str, sid: str) -> str:
    """Mint a fresh credential for this agent_id and push it down the given socket."""
    cred = mint_credential(agent_id)
    try:
        socketio.emit("agent_token",
                      {"agent_id": agent_id, "token": cred,
                       "client": _client_hint(agent_id, cred),
                       "note": "send this as X-Agent-Token on HTTP. It proves who you are, so "
                               "`from` is taken from it rather than from your X-Agent-Id header. "
                               "It dies when this socket disconnects or when another process "
                               "takes over this agent_id."},
                      to=sid, namespace=NS)
        log_event("CRED_MINTED", agent_id, "credential pushed", f"sid={sid[:8]}",
                  note="agent-scoped HTTP credential sent to this socket; the value is never "
                       "logged and it rotates with this socket",
                  frm="hub", to=agent_id)
    except Exception as exc:  # noqa: BLE001 - a socket that cannot hear it is not worth a 500
        log_event("CRED_FAIL", agent_id, "credential push failed",
                  f"{type(exc).__name__}: {exc}",
                  note="this agent has no credential and will keep using the operator token "
                       "for HTTP",
                  frm="hub", to=agent_id)
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
        log_event("CONNECTED", agent_id, "standby re-registered",
                  f"sid={promote[:12]} routing returned after the holder disconnected",
                  frm="hub", to=agent_id)
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
        # ...and re-tell it what it missed while it sat as a standby: items that arrived in
        # between were announced to the winner's socket, keyed on agent_id, not to this one.
        unread_nudge(agent_id, promote, reason="reactivated")
    elif agent_id:
        log_event("DISCONNECTED", agent_id, "socket closed", "persistent connection dropped",
                  note="future requests to this agent return 404 until it reconnects",
                  frm=agent_id, to="hub")


@socketio.on("result", namespace=NS)
def on_result(data=None):
    agent_id = sid_to_agent.get(request.sid, "?")
    msg_id = str((data or {}).get("msg_id", ""))
    text = (data or {}).get("text", "")
    # optional intent marker: "ack" means "received", anything else (or absent) is an answer.
    # Absent-by-default keeps every pre-v1.8 client behaving exactly as it does today.
    kind = str((data or {}).get("kind") or "")[:24].lower()
    answer_to = ""                       # who issued this task, for the log's sender/receiver ends
    row = {"from": f"agent:{agent_id}", "text": text}
    if kind:
        row["kind"] = kind
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
            r["results"].append(row)
            answer_to = r.get("from") or ""
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
            # the very dict the ledger records, so the `reply` an HTTP caller holds and the row it
            # reads back from GET /result/<msg_id> (including a kind marker) cannot drift apart
            p["replies"].append(row)
            p["event"].set()
        elif agent_id != "?":
            client_inboxes[agent_id].append({"from": f"agent:{agent_id}", "msg_id": msg_id,
                                             "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                             "text": text})
    # one MSG_RCVD row per result: the first reply to a waiting HTTP caller is logged by that route
    if not http_will_log:
        log_event("MSG_RCVD", agent_id, "recorded, no HTTP waiter",
                  f"[{msg_id}] {eprint_summary(text)}",
                  note=(f"kind={kind}" if kind else ""),
                  frm=agent_id, to=answer_to or "hub", ref=msg_id)
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
    log_event("MSG_RCVD", agent_id, "queued to client inbox", eprint_summary(text),
              frm=agent_id, to="client", ref=str(msg_id or ""))
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
        mid, depth = mail_put(to, f"agent:{sender}", text)
        log_event("MSG_QUEUED", to, "mail queued", eprint_summary(text),
                  frm=sender, to=to, ref=mid)
        return {"status": "queued", "to": to, "msg_id": mid, "queue_depth": depth,
                "note": f"agent '{to}' has no live socket; queued and will arrive as "
                        "peer_msg when it rejoins or on its next unread notice"}
    socketio.emit("peer_msg", {"from": f"agent:{sender}", "text": text}, to=sid, namespace=NS)
    log_event("MSG_SENT", to, "relay", eprint_summary(text), frm=sender, to=to)
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
    log_event("MSG_FAIL", "-", "internal error", f"{type(exc).__name__}: {exc}",
              frm="hub", to="client")
    out = {"error": "internal server error", "hint": "retry once; if persistent check /logs or /health",
           "docs": "/llms.txt", "api": "/api"}
    if HUB_DEBUG:
        out["detail"] = f"{type(exc).__name__}: {exc}"
    return jsonify(out), 500


# ----------------------------------------------------------------- public tunnel (optional)
def _warn_localhost(reason: str) -> None:
    msg = f"{reason} - no tunnel, serving localhost only on http://localhost:{HUB_PORT}"
    print(f"[agent-hub] WARN: {msg}")
    log_event("SERVER", "-", "", msg, frm="hub", to="hub")
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
    log_event("SERVER", "-", "ngrok edge", f"public {url} -> :{HUB_PORT}",
              frm="hub", to="ngrok")


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
    # Every agent is nudged at connect, so let the first tick come a full cadence after boot
    # instead of on the reaper's first 15s pass, which would address every socket at once.
    _next_nudge_at = STARTED_AT + UNREAD_NUDGE_SECONDS
    if RETENTION_DAYS:
        _next_sweep_at = time.time() + RETENTION_SWEEP_SECONDS   # don't sweep twice in one boot
        r = retention_sweep(actor="startup")
        n = sum(v for k, v in (r.get("removed") or {}).items() if k != "bytes_freed")
        print(f"[agent-hub] retention: objects and messages older than {RETENTION_DAYS}d age out "
              f"every {RETENTION_SWEEP_SECONDS}s"
              + (" (DRY RUN - reporting only)" if RETENTION_DRY_RUN else "")
              + f"; startup sweep removed {n} item(s)")
    else:
        print("[agent-hub] retention: disabled (HUB_RETENTION_DAYS=0) - only count caps apply")
    log_event("SERVER", "-", "startup",
              f"hub v{HUB_VERSION} started on {HUB_BIND}:{HUB_PORT} (threading mode), "
              f"task ttl {TASK_TTL_SECONDS}s", frm="hub", to="hub")
    threading.Thread(target=reap_expired_tasks, daemon=True, name="task-reaper").start()
    start_log_page_writer()
    print(f"[agent-hub] v{HUB_VERSION} listening on {HUB_BIND}:{HUB_PORT} | agents socket ns={NS} | "
          f"hub token len={len(AGENT_TOKEN)} | docs = /llms.txt")
    announce_log_urls()
    threading.Thread(target=open_tunnel, daemon=True).start()
    socketio.run(app, host=HUB_BIND, port=HUB_PORT, debug=False, allow_unsafe_werkzeug=True)
