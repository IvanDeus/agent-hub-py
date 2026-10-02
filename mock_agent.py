#!/usr/bin/env python3
"""Mock NAT agent for Agent Hub. Persistent socket mode (default) plus one-shot HTTP helpers.

# connect & listen (outbound only - works behind NAT):
python3 mock_agent.py --server https://<ngrok-host> --agent-id scout --token "$AGENT_AUTH_TOKEN"

# the operator (you / a sub-agent) communicates with the hub using files:
#   reads   -> ./state/<agent-id>/inbox.jsonl   (tasks, peer msgs, files)
#   writes  -> ./state/<agent-id>/outbox.jsonl  (one JSON action per line):
#     {"action":"to_agent","to":"builder","text":"..."}   relay via hub socket
#     {"action":"to_client","text":"..."}                 unsolicited note to client
#     {"action":"reply","msg_id":"...","text":"..."}      answer a pending task
#     {"action":"upload","path":"out/report.html","to":"reviewer"}  hub file relay
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


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def http_base(server: str) -> str:
    return server.rstrip("/").split("/socket.io")[0]


class Agent:
    def __init__(self, server: str, agent_id: str, token: str, quiet: bool = False):
        self.server, self.agent_id, self.token = server, agent_id, token
        self.base = http_base(server)
        self.quiet = quiet
        self.state = Path(__file__).resolve().parent / "state" / agent_id
        (self.state / "downloads").mkdir(parents=True, exist_ok=True)
        self.inbox = self.state / "inbox.jsonl"
        self.outbox = self.state / "outbox.jsonl"
        self.sio = socketio.Client(reconnection=True, reconnection_attempts=0,
                                   reconnection_delay=2, request_timeout=15)
        self.running = True
        for ev in ("task", "peer_msg", "file_ready"):
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
            self.sio.emit("agent_to_client", {"text": act["text"]}, namespace="/agents")
            self._write_inbox({"type": "sent_to_client", "text": act["text"]})
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
    def run_forever(self) -> int:
        try:
            self.sio.connect(self.base, namespaces=["/agents"],
                             auth={"token": self.token, "agent_id": self.agent_id},
                             wait_timeout=15)
        except Exception as exc:  # noqa: BLE001 - surface auth errors clearly
            print(f"[{now()}] CONNECT FAILED (hub rejected or unreachable): {exc}\n"
                  f"  - wrong AGENT_AUTH_TOKEN? hub logs it as AUTH_FAIL\n"
                  f"  - hub behind ngrok: first request may hit the browser-warning page",
                  file=sys.stderr)
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
    ap = argparse.ArgumentParser(description="Agent Hub mock agent (NAT client)")
    ap.add_argument("--server", default=os.environ.get("HUB_URL", "http://localhost:5000"))
    ap.add_argument("--agent-id", default=os.environ.get("AGENT_ID", ""))
    ap.add_argument("--token", default=os.environ.get("AGENT_AUTH_TOKEN", ""))
    ap.add_argument("--relay-to", metavar="AGENT_ID", help="POST /relay text and exit")
    ap.add_argument("--text", default="", help="text payload for --relay-to")
    ap.add_argument("--upload", metavar="PATH", help="POST /file and exit")
    ap.add_argument("--to", metavar="AGENT_ID", help="target agent for --upload")
    ap.add_argument("--download", metavar="FILE_ID", help="GET /file/<id> into downloads/")
    ap.add_argument("--agents", action="store_true", help="GET /agents and exit")
    args = ap.parse_args()

    if not args.agent_id:
        ap.error("--agent-id is required")
    if not args.token:
        ap.error("--token (AGENT_AUTH_TOKEN) is required")
    agent = Agent(args.server, args.agent_id, args.token)

    if args.agents:
        r = requests.get(f"{agent.base}/agents", headers=agent._auth_headers(), timeout=20)
        print(json.dumps(r.json(), indent=2))
        return
    if args.relay_to:
        r = requests.post(f"{agent.base}/relay", headers=agent._auth_headers(), timeout=20,
                          json={"to": args.relay_to, "text": args.text})
        print(r.status_code, r.text)
        return
    if args.upload:
        p = Path(args.upload)
        headers = agent._auth_headers()
        if args.to:
            headers["X-Target-Agent"] = args.to
        r = requests.post(f"{agent.base}/file", headers=headers, timeout=60,
                          files={"file": (p.name, p.read_bytes())})
        print(r.status_code, r.text)
        return
    if args.download:
        r = requests.get(f"{agent.base}/file/{args.download}", headers=agent._auth_headers(),
                         timeout=60)
        if r.ok:
            meta = requests.get(f"{agent.base}/files", headers=agent._auth_headers(),
                                timeout=20).json()["files"].get(args.download, {})
            dest = agent.state / "downloads" / meta.get("name", f"{args.download}.bin")
            dest.write_bytes(r.content)
            print(f"saved -> {dest} ({len(r.content)} B, sha_ok="
                  f"{hashlib.sha256(r.content).hexdigest() == meta.get('sha256')})")
        else:
            print(r.status_code, r.text)
        return
    sys.exit(agent.run_forever())


if __name__ == "__main__":
    main()
