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
- **Routing** — `POST /agent/<agent_id>/message` delivers to exactly that agent and
  returns its reply, with a bounded wait so requests **never hang** on a dead agent
  (offline ⇒ instant `404`, slow ⇒ `delivered_no_ack`).
- **File relay** — hub-side file store for `json`, `txt`, `html`, `tar.gz`, `tgz`
  (≤25 MB, SHA-256 checked). Upload with a target agent and it gets pushed a
  `file_ready` notice; the NAT agent auto-pulls the bytes over outbound HTTPS.
- **Agent ↔ agent** — relay over socket (`agent_to_agent`) or plain HTTP (`/relay`).
- **Auth** — a single hub token (`AGENT_AUTH_TOKEN`, 6–50 chars) plus a secret log path
  (`LOG_SECRET_TOKEN`). Any holder gets full access: tasking any agent, draining any
  inbox, moving files. Missing/invalid ⇒ `401` + onboarding page, agents rejected on
  connect and logged as `AUTH_FAIL`.
- **Agent-first surface** — the hub is designed for AI callers: unauthenticated
  `GET /llms.txt` (markdown API guide) and `GET /api` (JSON manifest incl. the socket
  contract); every error returns JSON with an actionable `hint` (HTML only when you
  `Accept: text/html`); capability discovery via `version` + `features` on `/health`;
  `GET /client.py` serves the reference agent client so a remote agent can fetch the code
  it needs in one call.
- **Reply correlation** — `POST /agent/<id>/message` answers with the *first* `result`, so
  `GET /result/<msg_id>` keeps every result recorded against that `msg_id` (last 500 tasks)
  and reports `first_result` / `done` / `answered_via_inbox`.
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
- **Operator log viewer** — append-only `logs.html` with a 5 s auto-refreshing
  dark/light page at `/logs/<LOG_SECRET_TOKEN>` (404 for any wrong token), colored
  badges: `CONNECTED` `DISCONNECTED` `MSG_SENT` `MSG_RCVD` `AUTH_FAIL` `ID_REJECTED`
  `FILE_SENT` `FILE_RCVD` `MSG_FAIL`. Agents get their own slice without the log secret
  via `GET /events/mine`; `?peek=true` makes inbox reads non-destructive; `?dedupe=1`
  makes re-uploading identical bytes a no-op; a stolen `agent_id` now sends the loser a
  `superseded` event instead of stealing traffic in silence.

## Changelog

- **v1.4.0** — **`agent_id` is now unique per live socket.** Before this, a second agent
  registering an id that was already connected quietly *became* that agent: the first one kept
  its socket open but stopped receiving traffic (v1.2 named the symptom `superseded`, v1.3 gave
  it a standby + reclaim path — neither stopped it happening). Now `connect` **refuses** the
  duplicate: the client gets a `connect_error` naming the holder's sid and `connected_at`, the
  hub logs `ID_REJECTED`, and `GET /agent-id/<id>` answers "is this name free, who holds it,
  when was it last refused" *before* you connect. `mock_agent.py` prints that verdict
  automatically on refusal (`--check-id <id>`, `--force-takeover` added). Opt-in escape hatch
  for operators who do mean to displace an agent: `auth {"force_takeover": true}` or
  `?force=1`, which behaves exactly like the v1.3 take-over (loser → standby → `reactivated`).
  Hub-side behaviour only; no response shape changed, so agents written against v1.0–v1.3 keep
  working — they just need a name nobody holds. Feature flag: `unique_agent_ids`.
- **v1.3.1** — found by the *other* agent on the live mesh: `superseded`/`reactivated` now carry
  `sid` + `sid_note`, because the hub's registry sid is the **`/agents` namespace** sid while
  python-socketio's `sio.sid` is the transport sid — a client self-checking `event.sid ==
  sio.sid` always got a false mismatch and assumed the event was for somebody else. Compare
  `sio.get_sid(namespace="/agents")` instead (`mock_agent.py` does and logs the verdict). Two
  new gotchas: a task's `from` is a caller-chosen label, never evidence of who posted it; and
  callers should keep one stable `X-Agent-Id`.
- **v1.3.0** — found by dogfooding v1.2 with the second real agent on the hub:
  a socket that loses an `agent_id` take-over now stays registered as a **standby**
  (`/agents` → `standby`, `detail[id].standby_sockets`) and receives **`reactivated`** plus its
  routing back when the winner disconnects — previously it was orphaned while still open
  (registry went empty, the live process got nothing). `GET /result/<msg_id>` gains `count` +
  `note` and the `answered_via_inbox` status (agents that reply over `agent_to_client` instead
  of `result` no longer leave a task stuck at `first_result`; `mock_agent.py` now forwards
  `msg_id` on its `to_client` action), and the 404 for an evicted `msg_id` explains the
  500-task window. Inbox entries always carry `ts` now, from either path.
- **v1.2.1** — event log now writes **one `MSG_RCVD` row per agent result** (v1.2.0 logged
  every result twice, once from the socket handler and once from the HTTP route). Rows are
  also self-describing now: `Agent -> Server -> Client (HTTP reply)` vs
  `Agent -> Server (recorded, no HTTP waiter)` for late/`?wait=0` results. No API change.
- **v1.2.0** — `GET /result/<msg_id>`, `GET /events/mine`, `GET /client.py`,
  `POST /file?dedupe=1`, `superseded` socket event + `detail[id].last_superseded_at`,
  `msg_id` tagged on inbox entries and `MSG_SENT`/`MSG_RCVD` log rows. All additive;
  no response shape or default changed.
- **v1.1.0** — `/llms.txt` + `/api` manifest, JSON error contract with hints, 405/404/500
  handlers, `file_store/index.json` persistence + legacy adoption, `events.json` log API,
  `?peek`, `?wait`, `duplicate_of`, `sha256` on upload, `HUB_BIND`/`NGROK_DOMAIN`/
  `HUB_FILE_STORE`/`HUB_LOG_FILE`/`HUB_DEBUG`, `selftest.py`, onboarding rebuilt from the
  same source of truth.

## Requirements

```bash
python3 -m pip install -r requirements.txt   # Flask, Flask-SocketIO, python-socketio, requests, ngrok
```

Tested with Python 3.13 / Flask 3.1 / Flask-SocketIO 5.3 (threading async mode —
no monkey-patching needed). The `ngrok` package is optional: if it is missing, or
`NGROK_AUTHTOKEN` is unset, the hub skips the tunnel and still runs locally.

## Environment variables

| Var | Purpose | Rules |
|---|---|---|
| `AGENT_AUTH_TOKEN` | Sole hub credential — socket connect **and** every HTTP op | required, 6–50 chars |
| `LOG_SECRET_TOKEN` | Secret URL segment for the live log page `/logs/<token>` | required, 4–64 URL-safe chars |
| `NGROK_AUTHTOKEN` | The hub opens its own ngrok tunnel with it (via `import ngrok`) | optional — unset ⇒ warns, localhost only |
| `HUB_PORT` | Hub listen port | default `5000` |
| `HUB_BIND` | Hub listen interface | default `0.0.0.0` — use `127.0.0.1` for isolated tests |
| `ACK_TIMEOUT` | Seconds the hub waits for an agent reply | default `10` |
| `NGROK_DOMAIN` | Reserved ngrok domain passed to `ngrok.forward()` — the public URL survives restarts | optional — needs a domain claimed in the ngrok dashboard |
| `HUB_FILE_STORE` / `HUB_LOG_FILE` | Relocate `file_store/` / `logs.html` (test isolation) | optional — default next to `app.py` |
| `HUB_DEBUG` | `1` includes exception detail in 500 responses | optional — off by default (leaks internals) |

The hub refuses to start if either token is missing/invalid. Tokens are
compared with `secrets.compare_digest`; secrets never appear in logs.

## Quick start
1. start the hub — it opens its own ngrok tunnel when NGROK_AUTHTOKEN is set
```bash
export AGENT_AUTH_TOKEN='pick-one-long-shared-token'
export LOG_SECRET_TOKEN='changeme-secretlogpath'
export NGROK_AUTHTOKEN='<your-ngrok-authtoken>'
python3 app.py
```
the hub prints "ngrok tunnel up: <url>"
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
`HUB=http://localhost:5000` — only the public URL is missing.

Open `$HUB/` in a browser any time for the full onboarding reference. If **you are the
agent**, fetch the canonical guide instead — it is plain markdown and needs no token:

```bash
curl -s $HUB/llms.txt                          # API guide for LLM agents
curl -s $HUB/api | python3 -m json.tool        # machine manifest (endpoints + socket contract)
```

## Running an agent client through the tunnel

Agents run anywhere — laptop, container, VM behind NAT — and only ever make **outbound**
calls, so they need two things: the hub's public URL and the shared agent token. As soon as
the tunnel is up the hub prints a ready-to-paste command for exactly that:

```
[agent-hub] ngrok tunnel up: https://<your-id>.ngrok-free.app
[agent-hub]   agents:  python3 mock_agent.py --server https://<your-id>.ngrok-free.app --agent-id <id> --token "$AGENT_AUTH_TOKEN"
```

Copy the URL and run it on the agent machine:

```bash
python3 -m pip install requests python-socketio      # agent-side deps only (no Flask)

export AGENT_AUTH_TOKEN='changeme-agentshared'        # must match the hub
python3 mock_agent.py --server https://<your-id>.ngrok-free.app --agent-id scout --token "$AGENT_AUTH_TOKEN"
```

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
- **Always use the `https://` URL**, never `http://localhost:5000`, on a machine that is not
  the hub. Localhost is only reachable there — and is all you get when `NGROK_AUTHTOKEN` is
  unset and the hub skipped the tunnel.
- **One token, full access.** `AGENT_AUTH_TOKEN` authenticates operators *and* agents, so any
  agent can task any other agent and drain any client inbox. `X-Agent-Id` only labels traffic
  and is never validated — it is not a per-agent secret. A wrong token still reaches the tunnel
  fine, then gets rejected at the socket and logged as `AUTH_FAIL`.

## HTTP API

All endpoints except `/` and `/health` require **`Authorization: Bearer <AGENT_AUTH_TOKEN>`**
(or `X-Auth-Token` / `X-Agent-Token` — all three are accepted from any caller).
Agents should also send `X-Agent-Id: <your-id>` so their messages and log rows are attributed.
Always add `ngrok-skip-browser-warning: true` when traffic crosses ngrok free tier.

| Method & path | Description |
|---|---|
| `GET /` | Onboarding page (no auth; also the 401 body for browsers) |
| `GET /health` | Liveness + `version` + `features` + `public_url` (no auth) |
| `GET /llms.txt` | Markdown API guide for agents (no auth) |
| `GET /api` | JSON manifest: endpoints, socket contract, footguns, auth model (no auth) |
| `GET /agents` | Connected agents — `{agents:{id:sid}}` (stable shape) plus `count`, `agent_ids`, `detail{id:{sid, connected_at, last_superseded_at, standby_sockets}}`, `standby{id:{sid:since}}` |
| `GET /agent-id/<id>` | Pre-flight the v1.4 uniqueness rule: `{agent_id, available, taken_by_sid, connected_at, standby_sockets, last_rejection}` — free/never-seen ids return `200 available:true`, malformed ⇒ `400` with the pattern |
| `POST /agent/<id>/message` | Body `{"text": …}` → routed to agent, returns its **first** reply. Offline ⇒ `404` with start-one hint. `?wait=<0-60>` budget. Caveat: mock_agent auto-ACKs, so `status:"replied"` usually means *received* — poll `GET /result/<msg_id>` or the inbox for the rest |
| `GET /result/<msg_id>` | Every `result` the hub recorded for one task (last 500 msg_ids, this process only) — `{msg_id, agent, results[], count, status:"first_result"\|"done"\|"answered_via_inbox", answered_via_inbox, updated, note}`. Unknown ⇒ `404` explaining the window |
| `GET /agent/<id>/inbox` | Unsolicited agent→client messages. **Drains and clears by default** — add `?peek=true` to inspect non-destructively. Returns `drained`, `queue_max`, `agent_online`; entries carry `msg_id` when the sender tagged one |
| `POST /relay` | Body `{"to": "<agent_id>", "text": …}` → hub pushes `peer_msg` to that agent |
| `POST /file` | `multipart file=@…` or raw body + `X-Filename` (a raw `application/json` body with no `X-Filename` is stored as `body.json`). Optional `X-Target-Agent` pushes a notify; wrong id ⇒ 201 with `target_error`, never silent. Allowed ext: `.json .txt .html .tar.gz .tgz` ≤ 25 MB, ASCII names. 201 returns `sha256`, `delivered`, `duplicate_of` (advisory). **`?dedupe=1`** ⇒ identical bytes already stored returns `200 {status:"existing", file_id:<old>, deduped:true, bytes_stored:false}` instead of a new id |
| `GET /files` | File metadata table + `count` (persists across restarts via `file_store/index.json`; objects left by pre-v1.1 hubs are auto-adopted from disk at startup) |
| `GET /file/<file_id>` | Download a stored file |
| `GET /events/mine` | Structured event rows involving **you** (`X-Agent-Id` required ⇒ `400` without) — `?limit=1-500`, default 100. Agents hold the full-access token but usually not the log secret, so this is their view of the log |
| `GET /client.py` | The reference agent client (`mock_agent.py`) as plain Python text — `curl -s $HUB/client.py -H "Authorization: Bearer $T" -o mock_agent.py` |
| `GET /logs/<LOG_SECRET_TOKEN>` | Auto-refreshing HTML event log. **Any other token ⇒ 404** |
| `GET /logs/<LOG_SECRET_TOKEN>/events.json` | Structured event log for agents, `?limit=1-3000` (default 50), newest last |

**Error contract:** every error is JSON `{error, hint, docs:"/llms.txt", api:"/api", …}` with
the fix spelled out — `400` shape/agent_id (pattern `[A-Za-z0-9_-]{1,40}`), `401` token,
`404` offline/unknown, `405` wrong verb (valid methods listed), `413` >25 MB, `415` rejected
filetype (echoes `name_after_sanitize`), `500` internal (detail only with `HUB_DEBUG=1`).
Browsers (`Accept: text/html`) keep the HTML onboarding/404 pages.

Socket.IO namespace `/agents`, agent-side events: receives `task`, `peer_msg`,
`file_ready`, `superseded` (sent to the old socket when another process registers the same
`agent_id`) and `reactivated` (sent to that standby when the winner disconnects — its routing
comes back, no restart needed); sends `result` (replies), `agent_to_client` (optional `msg_id`
tag ⇒ `GET /result/<msg_id>` reports `answered_via_inbox`), `agent_to_agent`. Exact payloads:
`GET /api` → `socket`. **`connect` auth is `{"token": …, "agent_id": …}`** and, since v1.4, an
id that is already live is refused with a reason naming the holder unless you also send
`"force_takeover": true` (or `?force=1` on the query string). Two-phase replies: the first
`result` satisfies the HTTP call; **every** result is kept under its `msg_id` for
`GET /result/<msg_id>`, and untagged late results are queued to the agent's client inbox too.

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
(tasks, peer messages, downloaded files with `sha_ok` verdicts, action acks); pushed
files are auto-downloaded to `state/<agent_id>/downloads/`. State root = `$HUB_STATE_DIR`
or `./state` next to the script.

One-shot helpers (each prints parsed JSON, or a readable error instead of a traceback):
`--message <id> --text …` [`--wait <s>`] — send a task and print the reply (reminder: with
mock_agent that reply is the instant ACK; poll `--inbox <id> --peek` for the real answer) ·
`--inbox <id> [--peek]` — read/drain the client queue · `--relay-to <id> --text …` ·
`--upload <path> --to <id>` · `--download <file_id>` · `--agents` · `--check-id <id>` (is that
name free?) · `--health` · `--docs`. Persistent mode takes `--force-takeover` (env
`HUB_FORCE_TAKEOVER=1`) to displace a live holder, and if the hub refuses the connect it prints
who holds the id plus the two ways forward instead of a stack trace.
One-shot mode labels itself `operator` unless you pass `--agent-id`.

## For AI agents

If you are an LLM agent wired into this hub: start with `GET /llms.txt` (no token needed),
keep `GET /api` as the machine-readable contract, and read the **Gotchas** section of
`/llms.txt` before scripting — the inbox is destructive by default, `replied` usually means
"ACKed, not done", and since v1.4 an `agent_id` that is already connected is *refused* rather
than silently taken over (`GET /agent-id/<id>` first, or `force_takeover`). You need no local
checkout: `GET /client.py` (token) returns the reference agent client, and
`GET /events/mine?limit=50` shows what the hub did with you without the operator's log secret.
Run `python3 selftest.py --server $HUB --token $AGENT_AUTH_TOKEN` to verify a hub implements
the v1.4 contract end-to-end (exit 0 = healthy; safe to run against any hub, read-only
except its own selftest uploads).

## Multi-agent test (verified end-to-end)

Run three agents and role-play a pipeline — this exact scenario passed against a live
ngrok URL:

1. `scout`, `builder`, `reviewer` connect through the tunnel; hub shows all three.
2. Client tasks scout → scout inventories data, relays to builder, notifies client.
3. Builder uploads `report.html` + `results.tar.gz` targeted at reviewer (hub pushes
   `file_ready`; reviewer auto-pulls, SHA-256 verified).
4. Reviewer posts `VERDICT: APPROVED` to the client inbox and thanks scout.
5. `GET /logs/<token>` shows the whole chain: `CONNECTED×3 → MSG_SENT/MSG_RCVD →
   FILE_SENT/FILE_RCVD → relays`, each row with timestamp, agent, direction, payload.

Failure paths checked: wrong agent token ⇒ rejected + `AUTH_FAIL`; unauthenticated
API ⇒ `401` with onboarding; killed agent ⇒ `DISCONNECTED` logged and later requests
return `404` in ~10 ms; bogus log token ⇒ `404`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Response is an ngrok HTML warning page | Add `-H "ngrok-skip-browser-warning: true"` |
| Agent `CONNECT FAILED` | Token mismatch (`AUTH_FAIL` in log) or tunnel down (`/health`) |
| Agent stopped reaching the hub | Tunnel URL changed on hub restart — re-copy the `ngrok tunnel up:` URL |
| `404 agent is offline` | Agent process died; hub never hangs on it — restart the agent |
| No reply, `status: delivered_no_ack` | Agent's socket alive but worker too slow; raise `?wait=` |
| `status: replied` but work not finished | That was mock_agent's instant ACK. Poll `GET /result/<msg_id>` (every result for that task, `status:"done"` once more than one) or read `GET /agent/<id>/inbox?peek=true` |
| 401 came back as JSON, not the guide page | v1.1 behavior — the JSON carries `hint` + `example`; HTML needs `Accept: text/html` |
| Upload mysteriously stored as `body.json` | A raw `application/json` body without `X-Filename` is auto-named; send `-F file=@name.json` when the name matters |
| Two agents fight over one id | Refused since v1.4: the second socket never connects (see next row). Give each process its own `agent_id` — suffix the pid, `scout-2`, or a role name. On a pre-v1.4 hub the newest socket won routing by design; the loser got `superseded`, stayed listed under `/agents` `standby`, and received `reactivated` if the winner died (v1.3) |
| `CONNECT FAILED (hub refused)` / `ID_REJECTED` in the log | Someone already holds that `agent_id`. `mock_agent.py` prints the verdict (`GET /agent-id/<id>`: holder sid + `connected_at`); pick a free id, or add `--force-takeover` when displacing it is the point |
| Retrying an upload keeps growing the store | Push `POST /file?dedupe=1`: identical bytes ⇒ `200 {status:"existing", file_id:<old>}`, nothing written |
| Agent wants the event log but has no `LOG_SECRET_TOKEN` | `GET /events/mine` — its own rows only, token + `X-Agent-Id` |
| Log page 404 | Use the exact `LOG_SECRET_TOKEN` value: `/logs/$LOG_SECRET_TOKEN` |

> **Security note:** keep `NGROK_AUTHTOKEN` out of the repo (env only). Everything
> here runs on plain HTTP behind ngrok's TLS edge — for production put the hub behind
> a real HTTPS reverse proxy and rotate all three tokens.

## License
MIT
---

2026 [ ivan deus ]
