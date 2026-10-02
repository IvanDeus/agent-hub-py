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
- **Auth** — master client token (`NAUTH`), agent public token (`AGENT_AUTH_TOKEN`,
  6–50 chars), secret log path (`LOG_SECRET_TOKEN`). Missing/invalid ⇒ `401` +
  onboarding page, agents rejected on connect and logged as `AUTH_FAIL`.
- **ngrok free tier** — every client should send `ngrok-skip-browser-warning: true`
  or ngrok returns its interstitial HTML page instead of your API response.
- **Operator log viewer** — append-only `logs.html` with a 5 s auto-refreshing
  dark/light page at `/logs/<LOG_SECRET_TOKEN>` (404 for any wrong token), colored
  badges: `CONNECTED` `DISCONNECTED` `MSG_SENT` `MSG_RCVD` `AUTH_FAIL`
  `FILE_SENT` `FILE_RCVD` `MSG_FAIL`.

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
| `NAUTH` | Master token for HTTP clients (`Authorization: Bearer …`) | required |
| `AGENT_AUTH_TOKEN` | Token agents use for socket + HTTP ops (`X-Agent-Token`) | required, 6–50 chars |
| `LOG_SECRET_TOKEN` | Secret URL segment for the live log page `/logs/<token>` | required, 4–64 URL-safe chars |
| `NGROK_AUTHTOKEN` | The hub opens its own ngrok tunnel with it (via `import ngrok`) | optional — unset ⇒ warns, localhost only |
| `HUB_PORT` | Hub listen port | default `5000` |
| `ACK_TIMEOUT` | Seconds the hub waits for an agent reply | default `10` |

The hub refuses to start if any of the first three are missing/invalid. Tokens are
compared with `secrets.compare_digest`; secrets never appear in logs.

## Quick start
1. start the hub — it opens its own ngrok tunnel when NGROK_AUTHTOKEN is set
```bash
export NAUTH='pick-a-long-master-token'
export AGENT_AUTH_TOKEN='changeme-agentshared'
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
  -H "Authorization: Bearer $NAUTH" \
  -H "ngrok-skip-browser-warning: true" \
  -H "Content-Type: application/json" \
  -d '{"text": "scan the dataset and report"}'
```

Without `NGROK_AUTHTOKEN` everything below step 1 runs the same way against
`HUB=http://localhost:5000` — only the public URL is missing.

Open `$HUB/` in a browser any time for the full onboarding/auth reference; the hub
also serves it (with `401`) when a request arrives without a valid token.

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
shell block (only `AGENT_ID` has to be unique):

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
  it into `HUB_URL` / `--server` after a restart, or reserve a static domain in the ngrok
  dashboard and pass it in `open_tunnel()`:
  `ngrok.forward(f"localhost:{HUB_PORT}", proto="http", domain="<your>.ngrok.app")`.
- **Always use the `https://` URL**, never `http://localhost:5000`, on a machine that is not
  the hub. Localhost is only reachable there — and is all you get when `NGROK_AUTHTOKEN` is
  unset and the hub skipped the tunnel.
- **Agents authenticate with `AGENT_AUTH_TOKEN`, not `NAUTH`** (that is the client token). A
  wrong agent token still reaches the tunnel fine, then gets rejected at the socket and
  logged as `AUTH_FAIL`.

## HTTP API

All endpoints except `/` and `/health` require **`Authorization: Bearer <NAUTH>`**
(or `X-Auth-Token`); agent-operated calls may instead use
**`X-Agent-Token: <AGENT_AUTH_TOKEN>`** + `X-Agent-Id: <your-id>`.
Always add `ngrok-skip-browser-warning: true` when traffic crosses ngrok free tier.

| Method & path | Description |
|---|---|
| `GET /` | Onboarding page (no auth) |
| `GET /health` | Liveness (no auth) |
| `GET /agents` | Currently connected agents — `{"agents": {agent_id: sid}}` |
| `POST /agent/<id>/message` | Body `{"text": …}` → routed to agent, returns its reply. Offline ⇒ `404`. `?wait=<0-60>` reply budget |
| `GET /agent/<id>/inbox` | Drain queued unsolicited agent→client messages |
| `POST /relay` | Body `{"to": "<agent_id>", "text": …}` → hub pushes to that agent |
| `POST /file` | `multipart file=@…` or raw body + `X-Filename`. Optional `X-Target-Agent` pushes a notify. Allowed ext: `.json .txt .html .tar.gz .tgz` ≤ 25 MB |
| `GET /files` | File metadata table |
| `GET /file/<file_id>` | Download a stored file |
| `GET /logs/<LOG_SECRET_TOKEN>` | Auto-refreshing HTML event log. **Any other token ⇒ 404** |

Socket.IO namespace `/agents`, agent-side events: receives `task`, `peer_msg`,
`file_ready`; sends `result` (replies), `agent_to_client`, `agent_to_agent`.

## mock_agent.py

Simulates a local AI agent. Persistent mode connects, auto-ACKs hub tasks (so client
requests resolve fast), then does real work driven by a simple **outbox protocol** —
the operator appends one JSON action per line to `state/<agent_id>/outbox.jsonl`
(picked up ~1 s later by the agent process):

```json
{"action":"to_agent","to":"builder","text":"findings ready"}
{"action":"reply","msg_id":"…","text":"late answer to a task"}
{"action":"to_client","text":"summary for the human"}
{"action":"upload","path":"work/report.html","to":"reviewer"}
```

Everything the agent receives is appended to `state/<agent_id>/inbox.jsonl`
(tasks, peer messages, downloaded files with `sha_ok` verdicts, action acks); pushed
files are auto-downloaded to `state/<agent_id>/downloads/`. One-shot helpers:
`--relay-to <id> --text …`, `--upload <path> --to <id>`, `--download <file_id>`,
`--agents`.

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
| Log page 404 | Use the exact `LOG_SECRET_TOKEN` value: `/logs/$LOG_SECRET_TOKEN` |

> **Security note:** keep `NGROK_AUTHTOKEN` out of the repo (env only). Everything
> here runs on plain HTTP behind ngrok's TLS edge — for production put the hub behind
> a real HTTPS reverse proxy and rotate all three tokens.

## License
MIT
---

2026 [ ivan deus ]
