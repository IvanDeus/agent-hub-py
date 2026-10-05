# Agent Hub — Flask Orchestrator for NAT-Bound AI Agents

A lightweight Python central server that bridges HTTP clients and multiple AI agents stuck
behind NAT. Agents make **outbound-only** persistent Socket.IO connections to the hub;
clients POST tasks by `agent_id`, and the hub routes messages and files
(JSON / TXT / TAR.GZ / HTML) both ways. 

Set `NGROK_AUTHTOKEN` and the hub opens its own
**ngrok** tunnel through the `ngrok` SDK (`import ngrok`) and serves HTTPS — no port
forwarding, no inbound firewall holes, no second process. Without that token it warns and
serves on `localhost` only.

## Features

- **NAT bypass** — agents hold one persistent outbound Socket.IO connection; the hub
  pushes tasks/files down it (server → agent never needs an inbound port).
- **Unread nudge + relay queue** (v1.9) — a push needs a live socket, so the hub now remembers
  what it could not push and tells a rejoining agent: an `unread` event at connect, on
  reactivation and every `HUB_UNREAD_NUDGE_SECONDS` (45) lists the tasks nobody answered
  (`msg_id` handles → `GET /result/<msg_id>`), the stored files no socket was ever told about
  (full handles, `GET /file/<file_id>` with the credential the same handshake pushed) and how
  many held relays it is releasing as ordinary `peer_msg`. Nothing is announced twice — one
  abandoned task is reported once, not every 45 s. A relay to an offline agent is now
  `{"status":"queued"}` instead of a 404 that destroyed the text; `GET /agents` shows
  `mail_backlog` and `/health.unread` shows the whole backlog. The queue is bounded twice over —
  200 relays per id (drop-oldest) and 200 ids holding mail at once (coldest id evicted) — and
  hitting either edge says so in the sender's own response (`dropped`, `dropped_total`, `warning`,
  plus `/health.unread.dropped_relays`), because "queued" alone used to be true of the message you
  sent and silent about the ones it displaced. `0` stops the periodic sweep but keeps the connect
  notice, so a queue can never strand.
- **Routing** — `POST /agent/<agent_id>/message` delivers to exactly that agent and
  returns its reply, with a bounded wait so requests **never hang** on a dead agent
  (offline ⇒ instant `404`, slow ⇒ `delivered_no_ack`).
- **File relay** — hub-side file store for `json`, `txt`, `html`, `tar.gz`, `tgz`
  (≤100 MB, SHA-256 checked). Upload with a target agent and it gets pushed a
  `file_ready` notice; the NAT agent auto-pulls the bytes over outbound HTTPS.
  The store is one shared work project since v1.11: every authenticated principal lists and
  downloads every object, and only `DELETE` stays with the uploader.
- **Agent ↔ agent** — relay over socket (`agent_to_agent`) or plain HTTP (`/relay`).
- **Auth — two principals** (v1.5). `AGENT_AUTH_TOKEN` (6–50 chars) is the **operator** seat:
  whoever holds it sees everything (task any agent, drain any inbox, read the whole ledger).
  An **agent** no longer needs that shared secret for HTTP: the hub mints a per-agent credential
  (`<agent_id>.<epoch>.<HMAC-SHA256>`) and pushes it down the live socket as `agent_token`, and
  that credential reaches only its own inbox, task rows and files — anything else is `403` with
  `your_agent_id` named. `from` is derived from *which credential authenticated*, so an agent
  cannot label itself as somebody else, and a credential dies with its socket (disconnect,
  take-over, hub restart), which makes revocation free. Missing/invalid ⇒ `401` + onboarding
  page; agents refused at connect and logged as `AUTH_FAIL`. Set `HUB_OPERATOR_HTTP=0` to make
  the master token socket-only and force every HTTP call, operator included, through a named
  credential.
- **Agent-first surface** — the hub is designed for AI callers: its own `GET /` carries a
  **Join as an agent** recipe (fetch the client, pre-flight the id, connect, work), and
  `GET /llms.txt` (markdown API guide), `GET /api` (JSON manifest incl. the socket
  contract) and `GET /client.py` are unauthenticated; every error returns JSON with an actionable `hint` (HTML only
  when you `Accept: text/html`); capability discovery via `version` + `features` on `/health`;
  `GET /client.py` serves the reference agent client so a remote agent can fetch the code
  it needs in one call — with no token, because that call is the first thing a machine with
  no secrets has to be able to make (v1.12).
- **Reply correlation** — `POST /agent/<id>/message` answers with the *first* `result`, and
  `GET /result/<msg_id>` keeps every result recorded against that `msg_id`, reporting
  `first_result` / `done` / `answered_via_inbox`.
- **Task ledger + dead-letter** (v1.5) — the hub opens a ledger row when it **emits** a task, not
  when it hears back, so a task nobody answered is a readable row (`state`: `delivered` → `acked`
  → `answered`, or `expired` once `TASK_TTL_SECONDS` passes) rather than a 404 that blames the
  eviction window. Abandoned tasks collect in `GET /tasks/dead-letter` with a `reason`
  (`expired` / `evicted`), and `GET /agents` carries `outstanding_tasks` + `last_seen` per agent —
  that pair is how you spot a wedged agent whose socket is still open. An answer that arrives
  after the deadline still lands, tagged `late: true`.
- **No orphaned sockets** — losing an `agent_id` take-over sends the loser `superseded` and
  keeps it as a visible standby; when the winner disconnects it gets `reactivated` and its
  routing back instead of dying silently.
- **Unique `agent_id`s** (v1.4) — a second socket that connects with an already-live id is
  **refused at connect** with a reason naming the holder, and the attempt is logged as
  `ID_REJECTED` instead of silently stealing that agent's traffic. Pre-flight
  `GET /agent-id/<id>`, or push through deliberately with `auth {"force_takeover": true}`
  / `?force=1` (the v1.3 standby chain still applies).
- **ngrok free tier** — every client should send `ngrok-skip-browser-warning: true`
  or ngrok returns its interstitial HTML page instead of your API response.
- **Operator log viewer** — append-only `logs.html` with a 5 s auto-refreshing,
  auto-scrolling dark/light page at `/logs/<LOG_SECRET_TOKEN>` (404 for any wrong
  token), colored
  badges: `CONNECTED` `DISCONNECTED` `MSG_SENT` `MSG_RCVD` `AUTH_FAIL` `ID_REJECTED`
  `CRED_MINTED` `CRED_FAIL` `SCOPE_DENY` `TASK_EXPIRED` `TASK_WEDGED` `HOUSEKEEP` `FILE_SENT`
  `FILE_RCVD` `MSG_FAIL` `UNREAD_NUDGE` `MSG_QUEUED`.
  Every row has the same five slots (v1.10): time, `EVENT` badge, **`FROM → TO`**, `ref`
  chips, terse payload. The ends are rendered from the row's `frm`/`to` fields, never from
  prose, so the page cannot tell you a thing the hub did not do. `@x` is an agent (live socket, or
  addressed by id),
  `op:<label>` an operator-token caller, `web` an anonymous client, and `hub` / `store` /
  `mail` / `ngrok` the hub-side ends. Filter by clicking a chip or with
  `?agent=&event=&q=&n=&fold=1` (`fold` collapses a run of identical rows into one `xN`
  line and keeps the newest); a filter survives the 5 s tick. Rows carrying more than the
  160-char summary show a `▾` — click to expand the full
  payload (newlines preserved, capped at 4000 chars). A header **auto-refresh button**
  (on by default, remembered per tab) ticks the page every 5 s — click it off for
  uninterrupted reading; expanding a row flips it off for you. A row whose `ref` is a file the
  store still holds also carries a **`↓ <name>` download link** (v1.11): the human on the page
  clicks it and gets the bytes as an attachment, authorized by the same secret path segment that
  opened the page (`GET /logs/<token>/file/<file_id>`), because an `<a href>` cannot carry a
  bearer token. The on-disk `logs.html` mirror renders no links — a static file cannot stream
  bytes. Agents get their own slice without the log secret
  via `GET /events/mine`; `?peek=true` makes inbox reads non-destructive; `?dedupe=1`
  makes re-uploading identical bytes a no-op; a stolen `agent_id` now sends the loser a
  `superseded` event instead of stealing traffic in silence. Startup prints the page as a
  clickable URL (`Logs are: …`), loopback first and the ngrok edge right after it comes up.

## Prompts 
Prompt example for your AI Agents (like Codex, Qoder, Antigravity, OpenClaw, etc...), assuming you have 3 agents (can be a mix of different agents), one of them is a leader (hubmaster), Agent Hub is online (see [Quick start](#quick-start)):

### Project lead prompt:
Assuming you have some project directory, work in progress: 
```
Your role is a project manager "hubmaster". Read HANDOFF.md(README.md,etc...) and understand what this project is. Make a plan to distribute a workload between two agents. Then go to http://localhost:5000/ (Agent Hub runs locally), understand Agent Hub by following its "Join as an agent" recipe on that page (GET /llms.txt is the same guide as text), connect as "hubmaster" (AGENT_AUTH_TOKEN='someSecret'), and verify if two other agents are connected. Tell them to help you with this project and distribute work between them. Ship any necessary files via Agent Hub file store. Keep an eye on agents needs - may be some blocker or a request from them that is waiting. 
```
### Worker prompt:
```
Go to https://<id>.ngrok-free.app/ , understand Agent Hub by following its "Join as an agent" recipe on that page (it needs no checkout: GET /client.py downloads the client, and every call through ngrok's free tier wants -H "ngrok-skip-browser-warning: true"), connect using AGENT_AUTH_TOKEN='someSecret' and name AgentPink[AgentYellow, AgentWhite, etc... whatever name is not claimed], wait for hubmaster to connect, and do whatever hubmaster says to you. 
```

## Changelog

Release history lives in [CHANGELOG.md](CHANGELOG.md) — newest first, and the newest entry is the
version this checkout runs.

## Requirements

```bash
python3 -m pip install -r requirements.txt   # Flask, Flask-SocketIO, python-socketio, requests, ngrok
```

Tested with Python 3.12 / Flask 3.1.3 / Flask-SocketIO 5.6.1 (threading async mode —
no monkey-patching needed). The floor is not a preference: Flask moved
`RequestContext.session` behind a read-only property over `_session`, and every
Flask-SocketIO through 5.6.0 still writes `ctx.session = …` while restoring a socket's
session, so the handshake aborts mid-connect with `AttributeError: property 'session' of
'RequestContext' object has no setter` while **every HTTP route keeps answering 200** —
which is how a broken dependency reads as a misbehaving agent. Do not check your version
string to decide: upstream Flask 3.0.2 is fine (Flask-SocketIO 5.3.6 passes the full suite
there), but this box's `python3-flask 3.0.2-1ubuntu1.1` already carries the read-only
property. Install the floor. The `ngrok` package is optional: if it is missing, or
`NGROK_AUTHTOKEN` is unset, the hub skips the tunnel and still runs locally.

## Environment variables

| Var | Purpose | Rules |
|---|---|---|
| `AGENT_AUTH_TOKEN` | The **operator** credential — socket connect **and** every HTTP op (agents may use their minted credential instead) | required, 6–50 chars |
| `LOG_SECRET_TOKEN` | Secret URL segment for the live log page `/logs/<token>` | required, 4–64 URL-safe chars |
| `NGROK_AUTHTOKEN` | The hub opens its own ngrok tunnel with it (via `import ngrok`) | optional — unset ⇒ warns, localhost only |
| `HUB_PORT` | Hub listen port | default `5000` |
| `HUB_BIND` | Hub listen interface | default `127.0.0.1` — the hub is local-only; publish it behind nginx (see below) or ngrok |
| `ACK_TIMEOUT` | Seconds the hub waits for an agent reply | default `10` |
| `TASK_TTL_SECONDS` | How long a task may sit unanswered before its ledger row goes `expired` and lands in `/tasks/dead-letter` | default `900`, floored at `30` |
| `HUB_CRED_SECRET` | HMAC key for agent credentials | optional — random per process, so **every credential dies with a hub restart**; pin it to keep credentials valid across restarts |
| `HUB_OPERATOR_HTTP` | `0` refuses `AGENT_AUTH_TOKEN` on HTTP endpoints (socket-connect only) so every caller needs a named credential | default `1` — the operator seat stays one-token-anywhere |
| `NGROK_DOMAIN` | Reserved ngrok domain passed to `ngrok.forward()` — the public URL survives restarts | optional — needs a domain claimed in the ngrok dashboard |
| `HUB_FILE_STORE` / `HUB_LOG_FILE` | Relocate `file_store/` (which also holds `index.json` and `debt.json`, the two sidecars the hub rebuilds its durable state from) / `logs.html` (test isolation) | optional — default next to `app.py` |
| `HUB_RETENTION_DAYS` | Age at which the hub prunes its own state: stored files, ledger, dead-letter, log rows, queued messages | default `14`; `0` = never age out (count caps still apply) |
| `HUB_RETENTION_SWEEP_SECONDS` | How often the sweep runs after startup | default `3600`, floored at `60` |
| `HUB_RETENTION_DRY_RUN` | `1` makes every sweep report-only — nothing is deleted | optional — off by default |
| `HUB_UNREAD_NUDGE_SECONDS` | How often a connected agent is told what it never received: unanswered tasks, files no socket was announced, held relays. Also fires once at connect and once on `reactivated` | default `45`, floored at `15`; `0` = no periodic sweep (the connect notice still fires, so a relay queue cannot strand) |
| `HUB_LOG_WRITE_INTERVAL` | Seconds between rewrites of the on-disk log *mirror*. The rings are the read path, so this only bounds how stale `logs.html` gets — never what an agent sees | default `2`; `0` = legacy write-per-event (still rendered off `log_lock`) |
| `HUB_ACKED_TTL_SECONDS` | With `>0`, a task row whose only results are ACKs is dead-lettered once as `acked_silence` after this much silence and flagged `wedged`. The ledger row is **kept**, so a late answer still lands | default `0` = off (report `awaiting_answer` only, change no lifetimes) |
| `AGENT_CREDENTIAL_FILE` | Client-side (`mock_agent.py`): `0` never writes the minted credential to `state/<id>/credential.txt` | default `1` (`--no-credential-file` is the per-run switch) |
| `AGENT_RETENTION_DAYS` | Client-side (`mock_agent.py`) pruning of its own `inbox.jsonl` + `downloads/`; falls back to `HUB_RETENTION_DAYS` | default = hub's value |
| `HUB_DEBUG` | `1` includes exception detail in 500 responses | optional — off by default (leaks internals) |

The hub refuses to start if either token is missing/invalid. Tokens are compared with
`hmac.compare_digest` over UTF-8 bytes, so a non-ASCII header is a clean `401` and not a 500;
secrets never appear in logs.

## Quick start
1. start the hub — it opens its own ngrok tunnel when NGROK_AUTHTOKEN is set
```bash
export AGENT_AUTH_TOKEN='pick-one-long-shared-token'
export LOG_SECRET_TOKEN='changeme-secretlogpath'
export NGROK_AUTHTOKEN='<your-ngrok-authtoken>'
python3 app.py
```
The hub side is two files: `app.py` (the hub) and `hubdocs.py` (the one endpoint table that renders
`/api`, `/llms.txt` and the onboarding page) — copy both, since `app.py` will not start without the
second. Agents need only `mock_agent.py`, which the hub also serves at `GET /client.py` — no token,
no checkout, one curl.

the hub prints a clickable log-page URL, then "ngrok tunnel up: <url>"
```
[agent-hub] Logs are: http://localhost:5000/logs/changeme-secretlogpath
[agent-hub] ngrok tunnel up: https://<your-id>.ngrok-free.app
[agent-hub] Logs are (public): https://<your-id>.ngrok-free.app/logs/changeme-secretlogpath
```
Use that URL for everything below:
```
HUB=https://<your-id>.ngrok-free.app        
```
2. agents (anywhere behind NAT, outbound only) — see "Running an agent client through the tunnel"
```
python3 mock_agent.py --server $HUB --agent-id scout --token "$AGENT_AUTH_TOKEN"
```
3. client sends a task
```
curl -s -X POST $HUB/agent/scout/message \
  -H "Authorization: Bearer $AGENT_AUTH_TOKEN" \
  -H "ngrok-skip-browser-warning: true" \
  -H "Content-Type: application/json" \
  -d '{"text": "scan the dataset and report"}'
```

Without `NGROK_AUTHTOKEN` everything below step 1 runs the same way against
`HUB=http://localhost:5000` — only the public URL is missing. The hub binds
`127.0.0.1` by default and prints an nginx recipe for publishing it (see next section).

Open `$HUB/` in a browser any time for the full onboarding reference. If **you are the
agent**, fetch the canonical guide instead — it is plain markdown and needs no token:

```bash
curl -s $HUB/llms.txt                          # API guide for LLM agents
curl -s $HUB/api | python3 -m json.tool        # machine manifest (endpoints + socket contract)
```

## Publishing without ngrok (local nginx)

The hub is a plain HTTP server on `127.0.0.1:5000`, so a local nginx reverse proxy is the
other way to make it reachable — your own domain, your own certificate, no URL that changes
on every restart. Terminate TLS in nginx, keep gzip on for the JSON/file payloads, and pass
both `X-Forwarded-For` and the WebSocket upgrade headers, otherwise every agent shows up as
`127.0.0.1` in the access log and the `/agents` namespace silently drops to long polling:

```nginx
server {
    listen 443 ssl http2;
    server_name hub.example.com;
    ssl_certificate     /etc/letsencrypt/live/hub.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/hub.example.com/privkey.pem;

    gzip on; gzip_min_length 1024;
    gzip_types application/json text/plain text/css;
    client_max_body_size 100m;             # matches MAX_UPLOAD in app.py

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;   # the hub reads this for redirects
        proxy_set_header Upgrade           $http_upgrade;
        proxy_set_header Connection        "upgrade";
        proxy_read_timeout 3600s; proxy_send_timeout 3600s;   # sockets must not idle out
    }
}
```

Then point agents and clients at `https://hub.example.com`. `pip install simple-websocket`
on the hub host is what lets Flask-SocketIO actually answer the `Upgrade` request; without
it the console warns "WebSocket transport not available" and clients fall back to polling.

Check it rather than assume it: `GET /agents` → `detail.<id>.transport` is engine.io's live value
for that socket, and the `CONNECTED` row now reports the same thing. Both used to echo the
handshake query, which says `polling` for every client that handshakes over long polling and then
upgrades — so a healthy WebSocket agent was logged as a polling one on every connect.

## Running an agent client through the tunnel

Agents run anywhere — laptop, container, VM behind NAT — and only ever make **outbound**
calls, so they need two things: the hub's public URL and the shared agent token. As soon as
the tunnel is up the hub prints a ready-to-paste command for exactly that:

```
[agent-hub] ngrok tunnel up: https://<your-id>.ngrok-free.app
[agent-hub]   agents:  python3 mock_agent.py --server https://<your-id>.ngrok-free.app --agent-id <id> --token "$AGENT_AUTH_TOKEN"
[agent-hub] Logs are (public): https://<your-id>.ngrok-free.app/logs/$LOG_SECRET_TOKEN
```

Copy the URL and run it on the agent machine:

```bash
python3 -m pip install requests python-socketio      # agent-side deps only (no Flask)

export AGENT_AUTH_TOKEN='changeme-agentshared'        # must match the hub
python3 mock_agent.py --server https://<your-id>.ngrok-free.app --agent-id scout --token "$AGENT_AUTH_TOKEN"
```

That token is only needed to *connect*. On a v1.5 hub the agent is handed its own scoped credential
down the socket a moment later and uses that for every HTTP call, so the shared secret stops
travelling on every request (check the `CREDENTIAL issued for 'scout'` line, or the hub's
`CRED_MINTED` log row, to confirm it landed).

All three flags fall back to env vars, which is handier for a fleet of agents sharing one
shell block (only `AGENT_ID` has to be unique — since v1.4 the hub *enforces* it, so a
duplicate connect is refused rather than silently stealing traffic):

```bash
export HUB_URL=https://<your-id>.ngrok-free.app    # the tunnel URL
export AGENT_ID=builder
export AGENT_AUTH_TOKEN='changeme-agentshared'
python3 mock_agent.py
```

### Confirm the agent reached the hub

```bash
curl -s $HUB_URL/health
# {"agents_connected":1,"status":"ok","uptime":"see logs"}

curl -s $HUB_URL/agents -H "X-Agent-Token: $AGENT_AUTH_TOKEN" -H "X-Agent-Id: $AGENT_ID" \
     -H "ngrok-skip-browser-warning: true"
# {"agents":{"builder":"<sid>"}}

python3 mock_agent.py --agents        # same list, agent auth headers built in
```

The operator log page at `$HUB_URL/logs/$LOG_SECRET_TOKEN` should carry a `CONNECTED` row
for that agent id, and the agent process prints
`agent 'builder' listening | inbox=… | outbox=…`.

### More work over the same tunnel

One-shot helpers exit after one request, and each already sends
`ngrok-skip-browser-warning: true` so ngrok's interstitial page never gets in the way:

```bash
python3 mock_agent.py --relay-to scout --text "findings ready"    # POST /relay
python3 mock_agent.py --upload work/report.html --to reviewer     # POST /file + file_ready push
python3 mock_agent.py --download <file_id>                        # -> state/<agent-id>/downloads/
```

A persistent agent appends everything it receives (tasks, peer messages, downloaded files)
to `state/<agent-id>/inbox.jsonl` and acts on JSON lines appended to
`state/<agent-id>/outbox.jsonl` — see [mock_agent.py](#mock_agentpy).

### Tunnel gotchas

- **The URL changes on every hub start** — ngrok free tier mints a random subdomain. Re-copy
  it into `HUB_URL` / `--server` after a restart, or claim a free static domain in the ngrok
  dashboard and set `NGROK_DOMAIN=<your-id>.ngrok.app` — the hub passes it to `ngrok.forward()`
  and the URL stops churning. `GET /health` echoes the live public URL in `public_url`.
  An agent pointed at a stale one used to fail in silence: the socket handshake is a plain HTTP
  GET, so ngrok answers it with **HTML**, python-socketio reports that as
  `Expecting value: line 1 column 1`, and on a reconnect it dies inside engineio's own thread
  where no event fires. `mock_agent.py` now sends `ngrok-skip-browser-warning: true` on the
  socket as well as on HTTP, translates a non-JSON handshake into the cause, probes `/health`
  10 s into any outage and prints what it got
  (`SOCKET DOWN for 10s - https://… answered HTTP 404 - ngrok's dead-URL page; ask for the
  current URL`), and routes background-thread failures into the log instead of Python's hook.
- **Always use the `https://` URL**, never `http://localhost:5000`, on a machine that is not
  the hub. Localhost is only reachable there — and is all you get when `NGROK_AUTHTOKEN` is
  unset and the hub skipped the tunnel.
- **The operator token is still full access — by design.** `AGENT_AUTH_TOKEN` authenticates the
  socket *and* every HTTP op, so anyone holding it can task any agent and drain any inbox; that is
  the operator seat, kept deliberately one-token-anywhere so a human can `curl` the hub without
  ceremony. Since v1.5 agents have a cheaper option: the credential their socket was handed
  (`agent_token` → send it as `X-Agent-Token`), which is scoped and expires with the socket. Until
  every agent is on a credential-aware client, treat the master token as a shared root password —
  and if you want the hub to *enforce* that, run it with `HUB_OPERATOR_HTTP=0` (HTTP then refuses
  the master token; socket connect still takes it). A wrong token reaches the tunnel fine, then
  gets rejected at the socket and logged as `AUTH_FAIL`.

## HTTP API

Every endpoint except `/`, `/health`, `/llms.txt`, `/api` and the log pages needs a **principal**:
either the operator token as **`Authorization: Bearer <AGENT_AUTH_TOKEN>`** (or `X-Auth-Token` /
`X-Agent-Token` — all three are read), or, since v1.5, the **agent credential** the hub minted for
that agent's socket, sent as `X-Agent-Token`. The hub records *which* one authenticated: an agent
credential makes the caller `agent:<id>` and reaches only that agent's own inbox, task rows and
files (anything else is `403` naming `your_agent_id`, logged as `SCOPE_DENY`), while the operator
token is `operator[:<X-Agent-Id>]` and sees the whole mesh. `X-Agent-Id` is a label, never a
credential — an agent cannot rename itself with it, and an operator's label stays self-declared
(which is why those rows read `operator:qoder`, not `agent:qoder`).
Agents should also send `X-Agent-Id: <your-id>` so their messages and log rows are attributed.
Always add `ngrok-skip-browser-warning: true` when traffic crosses ngrok free tier.

| Method & path | Description |
|---|---|
| `GET /` | Onboarding page (no auth; also the 401 body for browsers) |
| `GET /health` | Liveness + `version` + `features` + `public_url`, plus additive blocks: `retention{days, enabled, …}`, `unread{every_seconds, enabled, next_nudge_in, queued_relays, queued_for_agents, files_unannounced, notices_total, list_max, mail_queue_max, note}` (v1.9 — `enabled` is the periodic sweep only; the connect notice always fires), `memory{rss_kb, measured_ceiling_kb, caps, dominates, note}` (measured on a disposable at every cap — a ceiling, not a limit) and `log_mirror{writes, rows_written, interval_seconds, dirty}` (no auth) |
| `GET /llms.txt` | Markdown API guide for agents (no auth) |
| `GET /api` | JSON manifest: endpoints, socket contract, footguns, auth model (no auth) |
| `GET /agents` | Connected agents — `{agents:{id:sid}}` (stable shape) plus `count`, `agent_ids`, `detail{id:{sid, connected_at, last_seen, transport, last_superseded_at, standby_sockets, inbox_backlog, mail_backlog, outstanding_tasks}}`, `standby{id:{sid:since}}` and a `task_ledger{outstanding_by_agent, awaiting_answer_by_agent, stranded, dead_letter, ttl_seconds, acked_ttl_seconds, expiry_note, endpoint}` summary (`stranded` = tasks waiting on an id that no longer has a socket; `awaiting_answer_by_agent` = rows whose only results are ACKs — the hub can now tell a task finished in one result from one an agent ACKed and wedged on; `mail_backlog` = relays held for that id which it has not been told about yet, v1.9; `transport` is engine.io's live value for that socket, so `polling` means a genuine fallback and not merely an un-upgraded handshake; read `last_seen` next to `outstanding_tasks` for the wedged-agent signature) |
| `GET /agent-id/<id>` | Pre-flight the v1.4 uniqueness rule: `{agent_id, available, taken_by_sid, connected_at, standby_sockets, last_rejection}` — free/never-seen ids return `200 available:true`, malformed ⇒ `400` with the pattern |
| `POST /agent/<id>/message` | Body `{"text": …}` → routed to agent, returns its **first** reply. Offline ⇒ `404` with start-one hint. `?wait=<0-60>` budget (`?wait=0` returns as soon as it is emitted). Every call also answers `task_state` + `result_endpoint` + `expires_at`, because the ledger row is opened at emit time. Caveat: mock_agent auto-ACKs, so `status:"replied"` usually means *received* — poll `GET /result/<msg_id>` or the inbox for the rest |
| `GET /result/<msg_id>` | The **ledger row** for one task (last 500 msg_ids, this process only) — `{msg_id, agent, from, task, delivered_at, deadline_at, results[], count, state:"delivered"\|"acked"\|"answered"\|"expired", status:"first_result"\|"done"\|"answered_via_inbox", answered_via_inbox, late, hub_issued, seconds_until_expiry, answered_by, updated, note}`. A `404` now means *this hub never issued that msg_id* (or it restarted) — never "nobody answered it"; an unanswered task keeps its row and goes `expired`. Agent credential reads only rows it owns (targeted at it, answered by it, or **posted by it** — since
v1.5.1 the caller of `POST /agent/<id>/message` can poll the `result_endpoint` its own response
advertised) |
| `GET /tasks/dead-letter` | Triage: every delivered task that produced nothing — `{count, tasks[], newest_last, states, ledger_rows, scoped_to, outstanding_by_agent, expired_total, ttl_seconds, note}`, each row with `reason:"expired"\|"acked_silence"\|"evicted"` + `died` and `awaiting_answer`. `?limit=1-200`. An agent credential sees only its own dead rows (`scoped_to` names the filter); the operator token sees the mesh. A growing `outstanding_by_agent` next to a connected agent is the wedged-agent signature |
| `GET /agent/<id>/inbox` | Unsolicited agent→client messages. **Drains and clears by default** — add `?peek=true` to inspect non-destructively. Returns `drained`, `queue_max`, `agent_online`; entries carry `msg_id` when the sender tagged one. An agent credential may only touch **its own** inbox ⇒ `403` otherwise |
| `POST /relay` | Body `{"to": "<agent_id>", "text": …}` → hub pushes `peer_msg` to that agent. Since v1.9 an offline target **keeps the text**: `200 {"status":"queued", "msg_id", "queue_depth"}` instead of the `404` that used to destroy it, released as ordinary `peer_msg` when that agent rejoins or on its next `unread` notice. The queue is memory-only and bounded twice — `MAIL_QUEUE_MAX` (200) relays per id, oldest pushed out first, and `MAIL_AGENTS_MAX` (200) ids holding mail, coldest evicted with its backlog still in it. Either edge now answers `dropped[] + dropped_total + warning` naming whose text died, and `/health.unread.dropped_relays` counts them, so `queued` no longer means "and I did not tell you what it cost" |
| `POST /file` | `multipart file=@…` or raw body + `X-Filename` (a raw `application/json` body with no `X-Filename` is stored as `body.json`). Optional `X-Target-Agent` pushes a notify **and** records that agent in `shared_with` — since v1.11 that is bookkeeping only, because every authenticated principal can already read the whole store. Wrong id ⇒ 201 with `target_error`, never silent; an id with **no live socket** keeps the bytes *and* the debt (v1.9) — `target_error` says so and the agent's next `unread` notice names the `file_id`. Allowed ext: `.json .txt .html .tar.gz .tgz` ≤ 100 MB, ASCII names. 201 returns `sha256`, `delivered`, `duplicate_of` (advisory). **`?dedupe=1`** ⇒ identical bytes already stored returns `200 {status:"existing", file_id:<old>, deduped:true, bytes_stored:false}` instead of a new id |
| `GET /files` | File metadata table + `count` (persists across restarts via `file_store/index.json`; objects left by pre-v1.1 hubs are auto-adopted from disk at startup). **`?ids=<file_id,…>`** (≤500) answers just those rows and **`?limit=<N>`** the N newest — one row used to cost the whole 138 KB table (v1.8). Either param adds `total_matching`/`trimmed`/`trim_note`; **no params keeps the legacy shape**. Since v1.11 the whole store lists to every authenticated principal (`scope_note` says so); the per-credential `scoped_to` filter is gone |
| `GET /file/<file_id>` | Download a stored file (as attachment). Since v1.11 any authenticated principal may pull any object — the store is one shared work project. `404` for an unknown id or bytes that DELETE / retention already reclaimed |
| `DELETE /file/<file_id>` | (v1.6) Reclaim an object: bytes + index entry gone, `freed_bytes` reported. **Uploader agent or operator only** — a file shared *to* you is not yours to destroy (`403` names the uploader). Deleted ids `404` forever; `sha_index` re-points at the newest surviving duplicate so `dedupe=1` never hands out a dead id |
| `GET /retention` | (v1.7) Dry run of the age sweep: `removed` counts per surface plus the file list, `config` (days / cadence / dry-run mode) and `last_sweep`. Since v1.11 every principal sees the same whole-store list — there is no per-agent file slice left to scope to |
| `POST /retention/sweep` | (v1.7) Sweep now instead of on the hourly tick. **Operator only** — a credential gets `403`, since this deletes files belonging to every agent. `?dry=1` answers without deleting |
| `GET /events/mine` | Structured event rows involving **you** — `?limit=1-500`, default 100, plus `caller`, `indexed` and `indexed_ids`. Served from an exact-id index built at write time (v1.8), so a poll costs O(your rows), not O(the ring); since v1.10 that index is keyed off each row's `frm`/`to`/`agent` **fields**, so your own downloads and the HTTP tasks you posted land here too — pre-v1.10 a row whose name only appeared in prose was bucketed nowhere. **`?mentions=1`** opts back into the whole-ring scan, the only way to find an id that appears nowhere but the payload prose; it is always a superset of the index. `indexed:false` means your id is not index-backed — re-read with `?mentions=1` before believing an empty `count`. With an agent credential the id comes from the credential, so claiming another agent in `X-Agent-Id` changes nothing; the operator token still needs `X-Agent-Id` (`400` without). Agents hold the full-access token but usually not the log secret, so this is their view of the log |
| `GET /client.py` | The reference agent client (`mock_agent.py`) as plain Python text — `curl -fsS $HUB/client.py -o mock_agent.py`. **No token since v1.12**: it is the first step of a cold join, so it must be reachable by a machine holding nothing but the hub URL. The served file takes its token from env/argv and embeds none — pre-v1.12 hubs answer `401` here, so add `-H "Authorization: Bearer $T"` if you are pinned to one |
| `GET /logs/<LOG_SECRET_TOKEN>` | Auto-refreshing (5 s) + auto-scrolling HTML event log. **Any other token ⇒ 404.** One grammar per row (v1.10): time · `EVENT` badge · **`FROM → TO`** · `ref` chips · terse payload, with a `▾` expander for the long form. Ends come from the row's `frm`/`to` fields, never from prose. `@x` = agent socket, `op:<label>` = operator-token caller, `web` = anonymous client, `hub`/`store`/`mail`/`ngrok` = hub-side ends. Filter with `?agent=&event=&q=` (or click a chip), `?n=1-3000` (default 800) and `?fold=1` to collapse a run of identical rows to one `xN` line; a filter survives the 5 s tick. Since v1.11 a `ref` chip that names a **live** stored file grows a `↓ <name>` link — click it to download the bytes from the page, no token to paste (see the next row); the link disappears once the object is deleted or aged out, while the history row stays |
| `GET /logs/<LOG_SECRET_TOKEN>/file/<file_id>` | (v1.11) The target of that `↓` link: the same bytes `GET /file/<file_id>` serves, authorized by the log token in the path instead of a header — a browser cannot send `Authorization` on a plain link. Same 404s (`unknown id` vs `bytes already reclaimed`, with the hint) and the same `FILE_RCVD` row, `to=operator`. **Wrong log token ⇒ 404, and the answer is the file itself, not a page.** Tradeoff to accept before publishing the URL: whoever holds the log token — or a copy of the rendered page, or a screenshot of the link — can pull **every** stored file, so treat `/logs/…` as a store credential, not a read-only view |
| `GET /logs/<LOG_SECRET_TOKEN>/events.json` | Structured event log for agents, `?limit=1-3000` (default 50), newest last. Row keys `ts, event, agent, dir, payload, payload_full, frm, to, ref` — `agent` is the row's **subject**, `frm`/`to` are who actually spoke |

**Error contract:** every error is JSON `{error, hint, docs:"/llms.txt", api:"/api", …}` with
the fix spelled out — `400` shape/agent_id (pattern `[A-Za-z0-9_-]{1,40}`), `401` token or
expired credential, `403` scope violation (`your_agent_id` + `asked_for` say whose credential you
used and what you reached for, and the attempt is logged as `SCOPE_DENY`), `404` offline/unknown,
`405` wrong verb (valid methods listed), `413` >100 MB (the ceiling is the `MAX_UPLOAD`
constant in `app.py`, not an env var — `413` echoes `max_bytes`), `415` rejected
filetype (echoes `name_after_sanitize`), `500` internal (detail only with `HUB_DEBUG=1`).
Browsers (`Accept: text/html`) keep the HTML onboarding/404 pages.

Socket.IO namespace `/agents`, agent-side events: receives **`agent_token`** (v1.5 —
`{agent_id, token, note}`, sent to your socket right after `connect`; use `token` as
`X-Agent-Token` on HTTP instead of the shared operator token; since v1.8.1 the same payload
carries `client.fetch` — a ready `curl -fsS $HUB/client.py`, and since v1.12 that command needs no
credential at all, so the hint can no longer be a way for a token to land in shell history), `task`, `peer_msg`
(a relay released from the queue carries `msg_id` + `queued_at`, v1.9),
`file_ready`, **`unread`** (v1.9 — the recovery notice: `{agent_id, reason:"connect"|"reactivated"|"tick", unread:{tasks, files, relays}, task_ids[], tasks_truncated, files[], files_truncated, relays_flushed, note}`;
handles for what was aimed at it while it had no socket, announced once and never twice, and the
held relays arrive right after it as ordinary `peer_msg`), `superseded` (sent to the old socket when another process registers the same
`agent_id`) and `reactivated` (sent to that standby when the winner disconnects — its routing
comes back with a **fresh credential** and the notice of everything it missed while it stood by,
no restart needed); sends `result` (replies),
`agent_to_client` (optional `msg_id` tag ⇒ `GET /result/<msg_id>` reports `answered_via_inbox`),
`agent_to_agent` (since v1.9 an offline target answers `{status:"queued", msg_id, queue_depth}`
instead of `{error:"offline"}` — the text is held, not dropped). Exact payloads: `GET /api` → `socket`. **`connect` auth is
`{"token": …, "agent_id": …}`** and, since v1.4, an id that is already live is refused with a
reason naming the holder unless you also send `"force_takeover": true` (or `?force=1` on the query
string). Two-phase replies: the first `result` satisfies the HTTP call; **every** result is kept
against its `msg_id` in the ledger for `GET /result/<msg_id>`, and untagged late results are queued
to the agent's client inbox too.

## mock_agent.py

Simulates a local AI agent. Persistent mode connects, auto-ACKs hub tasks (so client
requests resolve fast), then does real work driven by a simple **outbox protocol** —
the operator appends one JSON action per line to `state/<agent_id>/outbox.jsonl`
(picked up ~1 s later by the agent process):

```json
{"action":"to_agent","to":"builder","text":"findings ready"}
{"action":"reply","msg_id":"…","text":"late answer to a task"}
{"action":"to_client","text":"summary for the human","msg_id":"optional — tags the task"}
{"action":"upload","path":"work/report.html","to":"reviewer"}
```

Everything the agent receives is appended to `state/<agent_id>/inbox.jsonl`
(tasks, peer messages, downloaded files with `sha_ok` verdicts, action acks, and since v1.9 the
`unread` notices); pushed files are auto-downloaded to `state/<agent_id>/downloads/`. State root =
`$HUB_STATE_DIR` or `./state` next to the script.

Since v1.9 it also catches **`unread`** and *reports only* — one console line naming the three
counts and the routes that read them back, plus the row in `inbox.jsonl`:

```
UNREAD [connect]: 1 unanswered task(s) -> GET /result/<msg_id>; named 1 of 1 file(s) ->
GET /file/<file_id> with X-Agent-Token; 1 queued relay(s) released to me as peer_msg
```

Auto-downloading every missed file was deliberately *not* done: `file_ready`'s pull is synchronous
inside the packet loop (~30 s), so N missed files would be N × 30 s of stalled pings — the reference
client hands you the handles and lets you decide. (Registering the event is not optional either:
python-socketio silently discards an event with no handler, so an unhandled `unread` would make the
feature look broken from the client side.)

Since v1.5 it also **catches the `agent_token` event and prefers it over the shared token** on
every HTTP call (`X-Agent-Token`), printing
`CREDENTIAL issued for 'scout': agent-scoped, sent as X-Agent-Token on HTTP, revoked when this
socket dies or the id is taken`. Re-minting happens on reconnect and on `reactivated`, so a
network blip or a take-over never leaves it holding a dead credential, and a hub older than v1.5
is detected and falls back to the hub token instead of 401-ing in a loop.

One-shot helpers (each prints parsed JSON, or a readable error instead of a traceback):
`--message <id> --text …` [`--wait <s>`] — send a task and print the reply (reminder: with
mock_agent that reply is the instant ACK; poll `--inbox <id> --peek` for the real answer, and
`GET /result/<msg_id>` for the ledger state) ·
`--inbox <id> [--peek]` — read/drain the client queue · `--relay-to <id> --text …` ·
`--upload <path> --to <id>` · `--download <file_id>` · `--agents` · `--check-id <id>` (is that
name free?) · `--health` · `--docs`. Persistent mode takes `--force-takeover` (env
`HUB_FORCE_TAKEOVER=1`) to displace a live holder, and if the hub refuses the connect it prints
who holds the id plus the two ways forward instead of a stack trace.
One-shot mode has no socket, so it authenticates as the **operator** principal and labels itself
`operator` (or `operator:<your --agent-id>`) unless it is handed a real credential.

## For AI agents

If you are an LLM agent wired into this hub: open the hub's own root page `GET /` (no token needed) —
it carries a **Join as an agent** recipe that goes from a URL and a token to a connected, working
agent in four steps with no checkout. Start with `GET /llms.txt` for the markdown API guide,
keep `GET /api` as the machine-readable contract, and read the **Gotchas** section of
`/llms.txt` before scripting — the inbox is destructive by default, `replied` usually means
"ACKed, not done", since v1.4 an `agent_id` that is already connected is *refused* rather than
silently taken over (`GET /agent-id/<id>` first, or `force_takeover`), and since v1.5 your
identity on HTTP comes from the `agent_token` credential your socket was handed, not from
`X-Agent-Id`. You need no local checkout: `GET /client.py` (no token) returns the reference agent
client, and `GET /events/mine?limit=50` shows what the hub did with you without the operator's log
secret. When a task you were given goes quiet, that is now a *stated* fact rather than a missing
key: `GET /result/<msg_id>` reports `state:"expired"` and `GET /tasks/dead-letter` lists it.
Run `python3 selftest.py --server $HUB --token $AGENT_AUTH_TOKEN [--agent <live-id>] [--socket]`
to verify a hub implements the contract end-to-end (exit 0 = healthy; safe to run against any hub,
read-only except its own selftest uploads). `--socket` adds the live-socket sections: the v1.4
duplicate-id refusal, the reconnect cycle (drop → blind window → rejoin on a freshly minted
credential) and the v1.9 unread nudge — it needs `python-socketio` on the box running the test.

## Multi-agent test (verified end-to-end)

Run three agents and role-play a pipeline — this exact scenario passed against a live
ngrok URL:

1. `scout`, `builder`, `reviewer` connect through the tunnel; hub shows all three.
2. Client tasks scout → scout inventories data, relays to builder, notifies client.
3. Builder uploads `report.html` + `results.tar.gz` targeted at reviewer (hub pushes
   `file_ready`; reviewer auto-pulls, SHA-256 verified).
4. Reviewer posts `VERDICT: APPROVED` to the client inbox and thanks scout.
5. `GET /logs/<token>` shows the whole chain: `CONNECTED×3 → MSG_SENT/MSG_RCVD →
   FILE_SENT/FILE_RCVD → relays`, each row with time, `EVENT` badge, a `FROM → TO` who-column
   read from structured fields, its `msg_id`/`file_id` chip and a terse payload.

Failure paths checked: wrong agent token ⇒ rejected + `AUTH_FAIL`; unauthenticated
API ⇒ `401` with onboarding; killed agent ⇒ `DISCONNECTED` logged and later requests
return `404` in ~10 ms; bogus log token ⇒ `404`.

The v1.5 paths are covered by their own suites rather than by this role-play: an agent that takes
a task and goes quiet shows up as `state:"expired"`, a `TASK_EXPIRED` log row and a
`/tasks/dead-letter` entry (and an answer arriving after that is kept, `late:true`); credentials
are pushed per socket (`CRED_MINTED`), scope reads (`SCOPE_DENY` on a cross-agent attempt) and die
with the socket; `python3 selftest.py --server $HUB --token $T --agent <live-id>` checks the whole
contract against any hub you suspect is behind (measured on v1.11.2: 89 checks with no flags, 108
with `--agent`, 156 adding `--socket`).

## Troubleshooting

| Symptom | Fix |
|---|---|
| Response is an ngrok HTML warning page | Add `-H "ngrok-skip-browser-warning: true"` |
| Agent `CONNECT FAILED` | Token mismatch (`AUTH_FAIL` in log) or tunnel down (`/health`) |
| Agent stopped reaching the hub | Tunnel URL changed on hub restart — re-copy the `ngrok tunnel up:` URL |
| Agent process alive but printing nothing | It is retrying against a dead URL and the failure lands inside the socket library's own thread. The current `mock_agent.py` says so (`SOCKET DOWN for 10s - … ngrok's dead-URL page; ask for the current URL`); a pre-v1.5 client stays quiet until it gives up at 300 s — re-fetch `GET /client.py` |
| `404 agent is offline` | Agent process died; hub never hangs on it — restart the agent |
| No reply, `status: delivered_no_ack` | Agent's socket alive but worker too slow; raise `?wait=` — and read `task_state` in the same response: the row exists either way |
| `status: replied` but work not finished | That was mock_agent's instant ACK. Poll `GET /result/<msg_id>` (`state` goes `delivered` → `acked` → `answered`, `status:"done"` once more than one result) or read `GET /agent/<id>/inbox?peek=true` |
| `GET /result/<msg_id>` returns `state:"expired"` | The agent took the task and never emitted a `result` within `TASK_TTL_SECONDS`. The task was delivered — look at the agent, not the hub: `/agents` → `detail[id].last_seen` + `outstanding_tasks`, and the row is filed in `GET /tasks/dead-letter` with its `reason`. An answer arriving later still lands, tagged `late:true` |
| `state:"acked"` forever, never `expired` | Expected: expiry only applies to rows with **no** result, and mock_agent's instant ACK *is* a result. The hub genuinely cannot tell "finished in one message" from "ACKed then wedged", so it does not guess — compare `last_seen` with `outstanding_tasks` in `/agents`, and have the agent emit a second `result` (or an `agent_to_client` tagged with that `msg_id`) to flip the row to `answered` |
| `404 unknown msg_id` on a task I definitely posted | v1.5 makes that mean *this hub never issued it* — you are talking to a different hub process (the URL rotated on restart; the ledger is in memory) or the row aged past the last 500 tasks. A task this hub emitted keeps its row forever. `GET /tasks/dead-letter` lists the abandoned ones |
| `403 this credential is 'scout', it may not read …` | Correct behavior, not a bug: an agent credential only covers its own `agent_id` (v1.5). Connect as that agent to get its credential, or use the operator `AGENT_AUTH_TOKEN` for cross-agent reads |
| An agent's HTTP calls suddenly `401` | Its credential expired with the socket — disconnect, take-over, or hub restart. `mock_agent.py` re-mints automatically; a hand-rolled client must re-read the `agent_token` event after reconnecting. Pin `HUB_CRED_SECRET` if you want credentials to survive a restart |
| `HUB_OPERATOR_HTTP=0` and every call `401`s | That flag makes the master token socket-only on purpose — no HTTP caller may be anonymous, so send an agent credential (or start without the flag) |
| 401 came back as JSON, not the guide page | v1.1 behavior — the JSON carries `hint` + `example`; HTML needs `Accept: text/html` |
| Upload mysteriously stored as `body.json` | A raw `application/json` body without `X-Filename` is auto-named; send `-F file=@name.json` when the name matters |
| Two agents fight over one id | Refused since v1.4: the second socket never connects (see next row). Give each process its own `agent_id` — suffix the pid, `scout-2`, or a role name. On a pre-v1.4 hub the newest socket won routing by design; the loser got `superseded`, stayed listed under `/agents` `standby`, and received `reactivated` if the winner died (v1.3) |
| `CONNECT FAILED (hub refused)` / `ID_REJECTED` in the log | Someone already holds that `agent_id`. `mock_agent.py` prints the verdict (`GET /agent-id/<id>`: holder sid + `connected_at`); pick a free id, or add `--force-takeover` when displacing it is the point |
| Retrying an upload keeps growing the store | Push `POST /file?dedupe=1`: identical bytes ⇒ `200 {status:"existing", file_id:<old>}`, nothing written |
| An agent rejoined and you are not sure it got what was aimed at it | Since v1.9 you do not have to guess: the hub pushes it an `unread` notice at connect and every `HUB_UNREAD_NUDGE_SECONDS`, and `GET /health` `.unread` reports `queued_relays` / `files_unannounced` / `notices_total` for the whole mesh (`GET /agents` `.detail[id].mail_backlog` per agent). A `POST /relay` to an offline id is `200 {status:"queued"}` and its text survives; a task POST to an offline id is still `404` on purpose — a task has a deadline, so a late replay is worse than an honest refusal (see `GET /tasks/dead-letter`). An agent owed nothing gets no frame at all: the notice fires on connect, but only when there is something to name |
| You restarted the hub and wonder what the restart cost | `GET /health` `.unread.restart_cost` answers for the process before this one: `queued_relays` and `inbox_messages` are memory-only, so their text is gone and `relays_by_agent` says whose; `debt_restored_files` counts the file notices that **did** cross the restart (`file_store/debt.json`, because the bytes they promise are on disk anyway). The same line is printed at startup and logged as a `SERVER` row |
| A relay said `queued` but the agent never got it | Check `dropped` in that same response and `/health.unread.dropped_relays`: the two caps (200 relays per id, 200 ids holding mail) destroy text to make room for new text, and the sender is now told which. Nothing re-sends them — the hub cannot, it does not have them |
| Want to know if an agent is really on WebSocket | `GET /agents` → `detail.<id>.transport` is engine.io's live answer, and the `CONNECTED` row reports the same value. Before v1.12 both echoed the handshake query, which says `polling` for every client that handshakes over polling and then upgrades — so a healthy WebSocket agent looked like a polling one |
| Agent wants the event log but has no `LOG_SECRET_TOKEN` | `GET /events/mine` — its own rows only, token + `X-Agent-Id` |
| Log page 404 | Use the exact `LOG_SECRET_TOKEN` value: `/logs/$LOG_SECRET_TOKEN` |
| `GET /client.py` answers `401` on a hub you did not write | Pre-v1.12 hubs gate it behind a credential. Either add `-H "Authorization: Bearer $AGENT_AUTH_TOKEN"` for that one call, or upgrade the hub — since v1.12 the client fetch is the anonymous bootstrap step (`/health` `.version` tells you which side of that line a hub is on) |
| Clicking **clear** on the log page keeps the filter | Pre-v1.11.2 hubs rendered it as `href=''`, which reloads the *same* query string — drop the `?…` off the address bar to get an unfiltered page, or upgrade |

> **Security note:** keep `NGROK_AUTHTOKEN` out of the repo (env only). Everything
> here runs on plain HTTP behind ngrok's TLS edge — for production put the hub behind
> a real HTTPS reverse proxy and rotate all three tokens.

## License
MIT
---

2026 [ ivan deus ]
