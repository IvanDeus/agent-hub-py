#!/usr/bin/env python3
"""Mock NAT agent for Agent Hub. Persistent socket mode (default) plus one-shot HTTP helpers.

# connect & listen (outbound only - works behind NAT):
python3 mock_agent.py --server https://<ngrok-host> --agent-id scout --token "$AGENT_AUTH_TOKEN"
#   since hub v1.4 an agent_id may only have one connected socket: pick a name nobody
#   holds (GET /agent-id/<id>, or --check-id <id>) or add --force-takeover.

# one-shot operator helpers (no socket needed; --token/$AGENT_AUTH_TOKEN required):
python3 mock_agent.py --agents                     # who is connected (hub version in --health)
python3 mock_agent.py --check-id scout             # is that agent_id free?
python3 mock_agent.py --message scout --text "scan the dataset" [--wait 30]
python3 mock_agent.py --inbox scout --peek         # read queue without draining it
python3 mock_agent.py --relay-to scout --text "findings ready"
python3 mock_agent.py --upload out/report.html --to reviewer
python3 mock_agent.py --download <file_id>
python3 mock_agent.py --health ; python3 mock_agent.py --docs   # /health, /llms.txt

# the operator (you / a sub-agent) communicates with the hub using files:
#   reads   -> <state>/<agent-id>/inbox.jsonl   (tasks, peer msgs, files)
#   writes  -> <state>/<agent-id>/outbox.jsonl  (one JSON action per line):
#     {"action":"to_agent","to":"builder","text":"..."}   relay via hub socket
#     {"action":"to_client","text":"...","msg_id":"..."}  note to client (msg_id optional:
#                                                         GET /result/<msg_id> -> answered_via_inbox)
#     {"action":"reply","msg_id":"...","text":"..."}      answer a pending task
#     {"action":"upload","path":"out/report.html","to":"reviewer"}  hub file relay
# state root = $HUB_STATE_DIR or ./state next to this script.
"""

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
import socketio

STATE_ROOT = Path(os.environ.get("HUB_STATE_DIR")
                  or Path(__file__).resolve().parent / "state")


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def http_base(server: str) -> str:
    return server.rstrip("/").split("/socket.io")[0]


def emit(r: "requests.Response") -> None:
    """Print a hub response without ever crashing on non-JSON bodies
    (ngrok interstitial, HTML 401 page, etc.)."""
    try:
        print(json.dumps(r.json(), indent=2))
    except ValueError:
        print(f"HTTP {r.status_code} - non-JSON body (first 200 chars):")
        print(r.text[:200])
        low = r.text.lower()
        if "ngrok" in low:
            print('hint: this looks like the ngrok free-tier interstitial - resend with header'
                  ' "ngrok-skip-browser-warning: true"')
        elif r.status_code == 401:
            print('hint: add -H "Authorization: Bearer <AGENT_AUTH_TOKEN>" ; docs: GET /llms.txt')


class Agent:
    def __init__(self, server: str, agent_id: str, token: str, quiet: bool = False,
                 force: bool = False):
        self.server, self.agent_id, self.token = server, agent_id, token
        self.force = force
        self.base = http_base(server)
        self.quiet = quiet
        self.state = STATE_ROOT / agent_id
        (self.state / "downloads").mkdir(parents=True, exist_ok=True)
        self.inbox = self.state / "inbox.jsonl"
        self.outbox = self.state / "outbox.jsonl"
        self.sio = socketio.Client(reconnection=True, reconnection_attempts=0,
                                   reconnection_delay=2, request_timeout=15)
        self.running = True
        for ev in ("task", "peer_msg", "file_ready", "superseded", "reactivated"):
            self.sio.on(ev, self._make_handler(ev), namespace="/agents")
        self.sio.on("connect", lambda: self._log(f"[{now()}] socket connected"))
        self.sio.on("disconnect", lambda: self._log(f"[{now()}] socket disconnected"))

    def _log(self, msg: str) -> None:
        if not self.quiet:
            print(msg, flush=True)

    def _write_inbox(self, entry: dict) -> None:
        entry = {"ts": now(), **entry}
        with self.inbox.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
        self._log(f"[{now()}] INBOX + {entry.get('type')}: "
                  f"{str(entry.get('text') or entry.get('name') or '')[:100]}")

    def _make_handler(self, ev: str):
        def handle(data=None, *_):
            data = data or {}
            if ev == "task":
                self._write_inbox({"type": "task", "from": data.get("from"),
                                   "msg_id": data.get("msg_id"), "text": data.get("text")})
                # instant ack so client requests never hang; real work goes to outbox later
                self.sio.emit("result", {"msg_id": data.get("msg_id"),
                                         "text": f"ACK[{self.agent_id}] task received {now()}"},
                              namespace="/agents")
            elif ev == "peer_msg":
                self._write_inbox({"type": "peer_msg", "from": data.get("from"),
                                   "text": data.get("text")})
            elif ev == "file_ready":
                self._download_file(data)
            elif ev in ("superseded", "reactivated"):
                mine = self.sio.get_sid(namespace="/agents")
                if ev == "superseded":
                    self._log(f"[{now()}] SUPERSEDED: agent_id '{self.agent_id}' was taken "
                              f"over by another socket ({data.get('reason', '-')})")
                else:
                    self._log(f"[{now()}] REACTIVATED: routing for '{self.agent_id}' is back "
                              f"with this process")
                # the hub's sid is the /agents-namespace sid - sio.sid would never match
                self._log(f"[{now()}]   sid check: hub={str(data.get('sid'))[:16]} "
                          f"mine={str(mine)[:16]} match={data.get('sid') == mine}")
                self._write_inbox({"type": ev, "reason": data.get("reason"),
                                   "at": data.get("at"), "hub_sid": data.get("sid"),
                                   "my_sid": mine, "sid_matches": data.get("sid") == mine,
                                   "text": f"agent_id '{self.agent_id}' "
                                           f"{'re-registered elsewhere' if ev == 'superseded' else 'routing restored'}"})
        return handle

    # ---------------- file download (agent pulls through ngrok, NAT-safe)
    def _download_file(self, meta: dict) -> None:
        url = f"{self.base}/file/{meta['file_id']}"
        r = requests.get(url, headers=self._auth_headers(), timeout=30)
        if r.status_code != 200:
            self._write_inbox({"type": "file_error", "file_id": meta["file_id"],
                               "status": r.status_code})
            return
        sha = hashlib.sha256(r.content).hexdigest()
        path = self.state / "downloads" / meta["name"]
        path.write_bytes(r.content)
        self._write_inbox({"type": "file", "file_id": meta["file_id"], "name": meta["name"],
                           "path": str(path), "size": len(r.content),
                           "sha_ok": sha == meta.get("sha256")})

    def _auth_headers(self) -> dict:
        return {"X-Agent-Token": self.token, "X-Agent-Id": self.agent_id,
                "Accept": "application/json",
                "ngrok-skip-browser-warning": "true"}

    # ---------------- outbox watcher: operator writes JSON actions here
    def _watch_outbox(self) -> None:
        pos = self.outbox.stat().st_size if self.outbox.exists() else 0
        while self.running:
            time.sleep(1.0)
            if not self.outbox.exists():
                continue
            with self.outbox.open("r", encoding="utf-8") as f:
                f.seek(pos)
                new = f.read()
                pos = f.tell()
            for line in new.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    act = json.loads(line)
                except json.JSONDecodeError:
                    self._log(f"[{now()}] OUTBOX bad json line: {line[:60]}")
                    continue
                self._run_action(act)

    def _run_action(self, act: dict) -> None:
        action = act.get("action")
        if action == "to_agent":
            ack = self.sio.call("agent_to_agent", {"to": act["to"], "text": act["text"]},
                                namespace="/agents", timeout=8)
            self._write_inbox({"type": "relay_ack", "to": act["to"], "ack": ack})
        elif action == "to_client":
            payload = {"text": act["text"]}
            if act.get("msg_id"):
                payload["msg_id"] = act["msg_id"]   # tags the task: GET /result/<msg_id> -> answered_via_inbox
            self.sio.emit("agent_to_client", payload, namespace="/agents")
            self._write_inbox({"type": "sent_to_client", "msg_id": act.get("msg_id"),
                               "text": act["text"]})
        elif action == "reply":
            self.sio.emit("result", {"msg_id": act.get("msg_id"), "text": act["text"]},
                          namespace="/agents")
            self._write_inbox({"type": "sent_reply", "msg_id": act.get("msg_id"),
                               "text": act["text"]})
        elif action == "upload":
            p = Path(act["path"])
            if not p.is_absolute():
                p = Path(__file__).resolve().parent / p
            data = p.read_bytes()
            headers = self._auth_headers()
            if act.get("to"):
                headers["X-Target-Agent"] = act["to"]
            r = requests.post(f"{self.base}/file", headers=headers, timeout=30,
                              files={"file": (p.name, data)})
            self._write_inbox({"type": "upload_ack", "name": p.name, "to": act.get("to"),
                               "status": r.status_code, "resp": r.json() if r.ok else r.text[:120]})
        else:
            self._log(f"[{now()}] OUTBOX unknown action: {action}")

    # ---------------- lifecycle
    def _why_refused(self) -> None:
        """The socket refusal is a string; confirm it over HTTP and print the fix."""
        try:
            probe = requests.get(f"{self.base}/agents", headers=self._auth_headers(), timeout=10)
        except Exception as exc:  # noqa: BLE001
            print(f"  - hub unreachable at {self.base}: {exc}", file=sys.stderr)
            return
        if probe.status_code == 401:
            print("  - VERDICT: the token is wrong (GET /agents returned 401). Fix "
                  "AGENT_AUTH_TOKEN; the hub logs this as AUTH_FAIL.", file=sys.stderr)
            return
        try:
            st = requests.get(f"{self.base}/agent-id/{self.agent_id}",
                              headers=self._auth_headers(), timeout=10).json()
        except Exception:  # noqa: BLE001 - hub older than v1.4, no such route
            print("  - could not check id availability (hub < v1.4 has no GET /agent-id/<id>)",
                  file=sys.stderr)
            return
        if not st.get("available"):
            print(f"  - VERDICT: agent_id '{self.agent_id}' is ALREADY CONNECTED "
                  f"(holder sid={str(st.get('taken_by_sid'))[:12]} since {st.get('connected_at')}, "
                  f"{st.get('standby_sockets')} standby socket(s)).\n"
                  f"    pick a unique one, e.g. --agent-id {self.agent_id}-{os.getpid()}\n"
                  f"    or displace it deliberately: --force-takeover\n"
                  f"    live ids: GET /agents", file=sys.stderr)
        else:
            print("  - id is free now, so the refusal was transient (a rival socket raced you)",
                  file=sys.stderr)

    def run_forever(self) -> int:
        auth = {"token": self.token, "agent_id": self.agent_id}
        if self.force:
            auth["force_takeover"] = True
        try:
            self.sio.connect(self.base, namespaces=["/agents"], auth=auth, wait_timeout=15)
        except Exception as exc:  # noqa: BLE001 - surface auth errors clearly
            print(f"[{now()}] CONNECT FAILED (hub refused): {exc}\n"
                  f"  - hub behind ngrok: send 'ngrok-skip-browser-warning' (clients here do)\n"
                  f"  - asking the hub why:", file=sys.stderr)
            self._why_refused()
            return 2
        self._log(f"[{now()}] agent '{self.agent_id}' listening | "
                  f"inbox={self.inbox} | outbox={self.outbox}")
        self._write_inbox({"type": "hello", "text": f"{self.agent_id} online via {self.base}"})
        threading.Thread(target=self._watch_outbox, daemon=True).start()
        try:
            while self.running and self.sio.connected:
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.running = False
            self.sio.disconnect()
        return 0


# ---------------- one-shot HTTP helpers (no persistent socket needed)
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Agent Hub mock agent (NAT client) + one-shot operator helpers. "
                    "Hub API guide: GET <hub>/llms.txt")
    ap.add_argument("--server", default=os.environ.get("HUB_URL", "http://localhost:5000"))
    ap.add_argument("--agent-id", default=os.environ.get("AGENT_ID", ""),
                    help="persistent mode: your id; one-shot: label only (default 'operator')")
    ap.add_argument("--token", default=os.environ.get("AGENT_AUTH_TOKEN", ""))
    ap.add_argument("--message", metavar="AGENT_ID",
                    help="POST /agent/<id>/message with --text and print the reply")
    ap.add_argument("--wait", type=float, default=None, metavar="SECONDS",
                    help="reply budget for --message (hub clamps 0-60)")
    ap.add_argument("--inbox", metavar="AGENT_ID", help="GET /agent/<id>/inbox (DRAINS by default)")
    ap.add_argument("--peek", action="store_true", help="with --inbox: read without clearing")
    ap.add_argument("--relay-to", metavar="AGENT_ID", help="POST /relay text and exit")
    ap.add_argument("--text", default="", help="text payload for --message/--relay-to")
    ap.add_argument("--upload", metavar="PATH", help="POST /file and exit")
    ap.add_argument("--to", metavar="AGENT_ID", help="target agent for --upload")
    ap.add_argument("--download", metavar="FILE_ID", help="GET /file/<id> into downloads/")
    ap.add_argument("--agents", action="store_true", help="GET /agents and exit")
    ap.add_argument("--force-takeover", action="store_true",
                    default=os.environ.get("HUB_FORCE_TAKEOVER", "").lower() in ("1", "true", "yes"),
                    help="take an agent_id that is already connected (hub refuses duplicates since v1.4)")
    ap.add_argument("--check-id", metavar="AGENT_ID", help="GET /agent-id/<id> and exit (is it free?)")
    ap.add_argument("--health", action="store_true", help="GET /health and exit")
    ap.add_argument("--docs", action="store_true", help="GET /llms.txt and exit")
    args = ap.parse_args()

    if not args.token:
        ap.error("--token (AGENT_AUTH_TOKEN) is required")
    one_shot = any([args.message, args.inbox, args.relay_to, args.upload, args.download,
                    args.agents, args.health, args.docs, args.check_id])
    if not args.agent_id:
        if not one_shot:
            ap.error("--agent-id is required for persistent mode (env: AGENT_ID)")
        args.agent_id = "operator"
    agent = Agent(args.server, args.agent_id, args.token, force=args.force_takeover)

    if args.docs:
        r = requests.get(f"{agent.base}/llms.txt", timeout=20)
        print(r.text)
        return
    if args.health:
        emit(requests.get(f"{agent.base}/health", headers=agent._auth_headers(), timeout=20))
        return
    if args.agents:
        emit(requests.get(f"{agent.base}/agents", headers=agent._auth_headers(), timeout=20))
        return
    if args.check_id:
        emit(requests.get(f"{agent.base}/agent-id/{args.check_id}",
                          headers=agent._auth_headers(), timeout=20))
        return
    if args.message:
        url = f"{agent.base}/agent/{args.message}/message"
        if args.wait is not None:
            url += f"?wait={args.wait}"
        r = requests.post(url, headers=agent._auth_headers(), timeout=75,
                          json={"text": args.text})
        emit(r)
        try:
            if r.json().get("status") == "replied":
                print("note: with mock_agent this is usually the instant ACK; the real answer "
                      f"lands later - check: python3 mock_agent.py --inbox {args.message} --peek")
        except ValueError:
            pass
        return
    if args.inbox:
        url = f"{agent.base}/agent/{args.inbox}/inbox"
        if args.peek:
            url += "?peek=true"
        emit(requests.get(url, headers=agent._auth_headers(), timeout=20))
        if not args.peek:
            print("note: GET /agent/<id>/inbox DRAINS AND CLEARS the queue (use --peek to inspect)")
        return
    if args.relay_to:
        emit(requests.post(f"{agent.base}/relay", headers=agent._auth_headers(), timeout=20,
                           json={"to": args.relay_to, "text": args.text}))
        return
    if args.upload:
        p = Path(args.upload)
        headers = agent._auth_headers()
        if args.to:
            headers["X-Target-Agent"] = args.to
        emit(requests.post(f"{agent.base}/file", headers=headers, timeout=60,
                           files={"file": (p.name, p.read_bytes())}))
        return
    if args.download:
        r = requests.get(f"{agent.base}/file/{args.download}", headers=agent._auth_headers(),
                         timeout=60)
        if r.ok:
            files = requests.get(f"{agent.base}/files", headers=agent._auth_headers(),
                                 timeout=20).json().get("files", {})
            meta = files.get(args.download, {})
            dest = agent.state / "downloads" / meta.get("name", f"{args.download}.bin")
            dest.write_bytes(r.content)
            print(f"saved -> {dest} ({len(r.content)} B, sha_ok="
                  f"{hashlib.sha256(r.content).hexdigest() == meta.get('sha256')})")
        else:
            emit(r)
        return
    sys.exit(agent.run_forever())


if __name__ == "__main__":
    main()
