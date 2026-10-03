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

# the minted credential, and how to call the hub WITHOUT the master token:
#   at connect the hub pushes a per-agent credential down your socket. The client writes it to
#   <state>/<agent-id>/credential.txt (0600) and deletes it when that socket dies, so a one-shot
#   on the same box can present it instead of AGENT_AUTH_TOKEN - and the hub then records the
#   call as agent:<id> rather than operator:<label>:
python3 mock_agent.py --agent-id scout --use-credential --inbox scout --peek
python3 mock_agent.py --agent-id scout --credential      # where mine lives + what the hub says
#   one proving command, no headers to interpret: it presents the file and prints the hub's answer
#     python3 mock_agent.py --server https://<host> --agent-id scout --credential
#   "scoped_to": "scout" is the id the hub read off the CREDENTIAL (the master token, which is
#   all-seeing, answers "all agents"). A credential is also scoped, so --inbox <peer> is 403.
#   keep the credential off the filesystem entirely: --no-credential-file (env AGENT_CREDENTIAL_FILE=0)

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
import mimetypes
import os
import signal
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path

import requests
import socketio

STATE_ROOT = Path(os.environ.get("HUB_STATE_DIR")
                  or Path(__file__).resolve().parent / "state")

# The credential the hub mints at connect is pushed down the socket and lives only in this
# process's memory, so the operator - who is the one person allowed to use it - cannot see it.
# Mirroring it into the state dir (already the shared IPC surface, already agent-scoped) makes it
# usable by a one-shot call; --no-credential-file / AGENT_CREDENTIAL_FILE=0 keeps it off disk.
CREDENTIAL_FILE_NAME = "credential.txt"
CREDENTIAL_FILE_ENABLED = os.environ.get("AGENT_CREDENTIAL_FILE", "1").lower() not in (
    "0", "false", "no")

# Age out my own copies on the hub's clock: the hub prunes its file store, ledger and log rings
# after HUB_RETENTION_DAYS (default 14), and without this the client side of the same
# conversation grows forever on disk. AGENT_RETENTION_DAYS overrides this side alone; 0 keeps
# everything.
RETENTION_DAYS = max(0, int(os.environ.get("AGENT_RETENTION_DAYS",
                                           os.environ.get("HUB_RETENTION_DAYS", "14"))))
RETENTION_SWEEP_SECONDS = max(60, int(os.environ.get("AGENT_RETENTION_SWEEP_SECONDS", "3600")))

# Keep retrying a dropped socket for this long before declaring the hub unreachable. Long
# enough to ride out an ngrok edge flap or a hub restart, short enough that a permanently
# wrong URL/superseded id surfaces as a dead process instead of an infinite quiet retry loop.
RECONNECT_GIVEUP_AFTER = 300


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def http_base(server: str) -> str:
    return server.rstrip("/").split("/socket.io")[0]


# The socket handshake is a plain HTTP GET, so ngrok's free tier intercepts it exactly like
# any other call - and python-socketio then reports the HTML body as a JSON parse error.
SOCKET_HEADERS = {"ngrok-skip-browser-warning": "true"}

_PARSE_TELLS = ("expecting value", "jsondecodeerror", "no json object could be decoded",
                "unexpected response from server", "unexpected status code", "char 0")


def diagnose(detail: str) -> str:
    """Turn a socket-failure string into the fix. python-socketio hands back 'Expecting
    value: line 1 column 1 (char 0)' or 'Unexpected response from server', which is true,
    complete and useless to whoever is staring at a dead agent."""
    low = (detail or "").lower()
    if any(t in low for t in _PARSE_TELLS):
        return ("the hub answered the socket handshake with HTML instead of a Socket.IO "
                "handshake. Two ways that happens: (a) ngrok's free-tier interstitial - this "
                "client now sends 'ngrok-skip-browser-warning: true' on the socket too, so if "
                "you are seeing it on an older client, upgrade it; (b) the tunnel URL is DEAD - "
                "ngrok mints a NEW subdomain every time the hub restarts, and the old one "
                "answers 404 with an HTML page forever. Get the current URL from the operator "
                "or from GET /health on the hub host.")
    if "401" in low or "unauthorized" in low:
        return ("the hub refused the socket at HTTP level (401) - the token in "
                "auth {'token': ...} is wrong for this hub.")
    return ""


def file_part(path: "Path") -> tuple:
    """A multipart part that carries a real Content-Type. `requests` sends none unless you
    hand it a 3-tuple, so every upload used to land in the hub's index as
    `application/octet-stream` and `GET /files` told you nothing about what you stored."""
    data = path.read_bytes()
    return path.name, data, (mimetypes.guess_type(path.name)[0]
                             or "application/octet-stream")


def hub_verdict(base: str) -> str:
    """One line on what the hub endpoint actually says right now, so an agent that lost its
    socket can tell 'hub down' from 'URL stale' from 'hub fine, my socket is the problem'."""
    try:
        r = requests.get(base.rstrip("/") + "/health", headers=SOCKET_HEADERS, timeout=8)
    except requests.exceptions.ConnectionError:
        return f"nothing is listening at {base} - the hub process is down (or the URL is wrong)"
    except Exception as exc:  # noqa: BLE001 - a probe never breaks the agent
        return f"probe failed: {type(exc).__name__}: {exc}"
    if r.status_code != 200:
        kind = "ngrok's dead-URL page" if "ngrok" in r.text.lower() else "a non-hub answer"
        return f"{base} answered HTTP {r.status_code} - {kind}; ask for the current URL"
    try:
        body = r.json()
    except ValueError:
        return f"{base} answered 200 with a non-JSON body - an intercepting proxy, not the hub"
    return (f"the hub at {base} is ALIVE (v{body.get('version')}, "
            f"{body.get('agents_connected')} agent(s) connected) - so the socket, not the hub, "
            f"is the problem here")


def emit(r: "requests.Response") -> bool:
    """Print a hub response without ever crashing on non-JSON bodies
    (ngrok interstitial, HTML 401 page, etc.). Returns True if 2xx."""
    ok = r.ok
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
    return ok


class Agent:
    def __init__(self, server: str, agent_id: str, token: str, quiet: bool = False,
                 force: bool = False, no_credential_file: bool = False,
                 on_task=None, on_peer_msg=None, on_file_ready=None,
                 auto_reply: bool = False, exec_cmd: str = ""):
        self.server, self.agent_id, self.token = server, agent_id, token
        self.force = force
        self.base = http_base(server)
        self.quiet = quiet
        self.on_task = on_task
        self.on_peer_msg = on_peer_msg
        self.on_file_ready = on_file_ready
        self.auto_reply = auto_reply
        self.exec_cmd = exec_cmd
        self.state = STATE_ROOT / agent_id
        (self.state / "downloads").mkdir(parents=True, exist_ok=True)
        self.inbox = self.state / "inbox.jsonl"
        self.outbox = self.state / "outbox.jsonl"
        self.cred_path = self.state / CREDENTIAL_FILE_NAME
        self.share_credential = CREDENTIAL_FILE_ENABLED and not no_credential_file
        self.sio = socketio.Client(reconnection=True, reconnection_attempts=0,
                                   reconnection_delay=2, request_timeout=15)
        self.running = True
        self.deliberate_stop = False
        self.last_refusal = ""
        self.agent_token = ""      # per-agent credential minted by the hub at connect (v1.5)
        for ev in ("task", "peer_msg", "file_ready", "superseded", "reactivated"):
            self.sio.on(ev, self._make_handler(ev), namespace="/agents")
        # The hub hands out a credential bound to this agent_id at connect. Every HTTP call this
        # process makes presents it, so `from` is derived from which credential authenticated
        # rather than from a header this process could type. A reconnect (or a take-over of the
        # id) mints a new one and the old one stops working - revocation for free.
        self.sio.on("agent_token", self._on_agent_token, namespace="/agents")
        self.sio.on("connect", lambda *a: self._log(f"[{now()}] socket connected"))
        # A drop is not an exit: python-socketio reconnects on its own thread, so say so out
        # loud instead of letting the operator guess from a quiet process.
        self.sio.on("disconnect", lambda *a: self._log(
            f"[{now()}] socket disconnected - process staying alive, reconnecting in the "
            f"background (a tunnel blip is not fatal)"))
        # Only a namespace refusal lands here. A hub that is down or a URL that changed fails
        # at the connection level and fires nothing at all, so run_forever times the outage
        # itself rather than trusting this event to report it.
        self.sio.on("connect_error", self._on_refused)
        # ...and a reconnect that dies inside engineio's own thread fires no event either.
        # Without this hook the exception goes to Python's default threading hook: a traceback
        # nobody reads (or silence), while the process looks healthy.
        self._install_thread_hook()

    def _install_thread_hook(self) -> None:
        me = self

        def _hook(args):
            text = f"{type(args.exc_value).__name__}: {args.exc_value}"
            me.last_refusal = text[:160]
            extra = diagnose(text)
            me._log(f"[{now()}] BACKGROUND SOCKET THREAD failed: {text}"
                    + (f"\n  - {extra}" if extra else ""))
            if os.environ.get("HUB_DEBUG") == "1" and args.exc_traceback is not None:
                traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback)

        threading.excepthook = _hook

    def _on_refused(self, err=None, *_a) -> None:
        self.last_refusal = str(err)[:160]
        extra = diagnose(self.last_refusal)
        self._log(f"[{now()}] RECONNECT REFUSED: {self.last_refusal}"
                  + (f" - {extra}" if extra else " - still retrying"))

    def _on_agent_token(self, data=None) -> None:
        self.agent_token = str((data or {}).get("token", ""))
        if self.agent_token:
            self._log(f"[{now()}] CREDENTIAL issued for '{self.agent_id}': agent-scoped, sent as "
                      f"X-Agent-Token on HTTP, revoked when this socket dies or the id is taken")
        else:
            self._log(f"[{now()}] no credential in the connect handshake - this hub predates "
                      f"v1.5, using the hub token for HTTP")
        self._publish_credential()

    # ---------------- credential handoff: the socket's credential, on disk for the operator
    def _publish_credential(self) -> None:
        if not self.share_credential:
            return
        if not self.agent_token:
            self._revoke_credential("this hub mints none")
            return
        try:
            # O_TRUNC + 0600 at create: the file is a credential, so it must never be group- or
            # world-readable even briefly, and a leftover from another hub must not survive.
            fd = os.open(self.cred_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(self.agent_token + "\n")
            self._log(f"[{now()}]   credential mirrored to {self.cred_path} (0600) - one-shots on "
                      f"this box can present it: --agent-id {self.agent_id} --use-credential")
        except OSError as exc:
            self._log(f"[{now()}]   credential NOT mirrored: {exc} (this process still holds it)")

    def _revoke_credential(self, why: str) -> None:
        if not self.share_credential or not self.cred_path.exists():
            return
        try:
            current = self.cred_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            self._log(f"[{now()}]   credential file unreadable, leaving it: {exc}")
            return
        # A process that took over this agent_id has already published ITS credential to the same
        # path. Only our own copy is dead with our socket; deleting the newer one would knock out
        # a live credential that belongs to somebody else.
        if current and current != self.agent_token:
            self._log(f"[{now()}]   {self.cred_path} left in place - it holds a newer credential "
                      f"than this socket's ({why})")
            return
        try:
            self.cred_path.unlink()
        except OSError as exc:
            self._log(f"[{now()}]   credential file still on disk: {exc} - treat it as dead")
            return
        self.agent_token = ""
        self._log(f"[{now()}]   {self.cred_path} deleted ({why}) - the hub revoked this "
                  f"credential with its socket, so leaving the file would look like a live secret")

    def read_credential(self) -> str:
        """The credential this agent_id last minted, if one is on disk and still looks usable."""
        try:
            txt = self.cred_path.read_text(encoding="utf-8").strip()
        except OSError:
            return ""
        # "<agent_id>.<epoch>.<hmac>", and minted for THIS id: the state dir can outlive the run
        # that wrote it, and presenting another agent's credential would be a scoping bug.
        parts = txt.split(".")
        if len(parts) != 3 or not all(parts) or parts[0] != self.agent_id:
            return ""
        return txt

    def _log(self, msg: str) -> None:
        if not self.quiet:
            print(msg, flush=True)

    def prune_state(self) -> dict:
        """Remove my own history older than RETENTION_DAYS: inbox rows and downloaded files.

        outbox.jsonl is deliberately NOT touched - the watcher tracks its consumed prefix by
        content, so rewriting that file makes already-run actions run a second time."""
        report = {"inbox_removed": 0, "downloads_removed": 0, "bytes": 0}
        if RETENTION_DAYS <= 0:
            return report
        cutoff = time.time() - RETENTION_DAYS * 86400
        if self.inbox.exists():
            keep, dropped = [], 0
            try:
                lines = self.inbox.read_text(encoding="utf-8").splitlines()
            except OSError:
                lines = []
            for line in lines:
                ts = ""
                try:
                    ts = str(json.loads(line).get("ts") or "")
                except (ValueError, TypeError):
                    pass
                try:
                    age = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp()
                except (ValueError, OSError):
                    age = time.time()          # unparseable: keep it, never delete on a guess
                if age < cutoff:
                    dropped += 1
                else:
                    keep.append(line)
            if dropped:
                tmp = self.inbox.with_name(self.inbox.name + ".tmp")
                try:
                    tmp.write_text("\n".join(keep) + ("\n" if keep else ""), encoding="utf-8")
                    os.replace(tmp, self.inbox)      # atomic: a torn inbox is a lost history
                    report["inbox_removed"] = dropped
                except OSError:
                    pass
        dl = self.state / "downloads"
        if dl.is_dir():
            for f in dl.iterdir():
                try:
                    if f.is_file() and f.stat().st_mtime < cutoff:
                        report["bytes"] += f.stat().st_size
                        f.unlink()
                        report["downloads_removed"] += 1
                except OSError:
                    continue
        return report

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
                # instant ack so client requests never hang; real work goes to outbox later.
                # kind="ack" is what tells the hub this is intent and not the answer: without it a
                # task the agent finished in one result and a task it ACKed then wedged on are the
                # same row, which is the trap awaiting_answer exists for.
                self.sio.emit("result", {"msg_id": data.get("msg_id"), "kind": "ack",
                                         "text": f"ACK[{self.agent_id}] task received {now()}"},
                              namespace="/agents")
                self._dispatch_task_response(data)
            elif ev == "peer_msg":
                self._write_inbox({"type": "peer_msg", "from": data.get("from"),
                                   "text": data.get("text")})
                if self.on_peer_msg:
                    try:
                        self.on_peer_msg(data, self)
                    except Exception as exc:
                        self._log(f"[{now()}] on_peer_msg error: {exc}")
            elif ev == "file_ready":
                file_info = self._download_file(data)
                if self.on_file_ready and file_info:
                    try:
                        self.on_file_ready(file_info, self)
                    except Exception as exc:
                        self._log(f"[{now()}] on_file_ready error: {exc}")
            elif ev in ("superseded", "reactivated"):
                mine = self.sio.get_sid(namespace="/agents")
                if ev == "superseded":
                    self._log(f"[{now()}] SUPERSEDED: agent_id '{self.agent_id}' was taken "
                              f"over by another socket ({data.get('reason', '-')})")
                    self._revoke_credential("superseded - the hub revoked it")
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

    def _dispatch_task_response(self, data: dict) -> None:
        msg_id = data.get("msg_id")
        text = str(data.get("text") or "")
        if self.on_task:
            def _run():
                try:
                    res = self.on_task(data, self)
                    if res is not None:
                        self.reply(msg_id, str(res))
                except Exception as exc:
                    self._log(f"[{now()}] on_task error: {exc}")
                    self.reply(msg_id, f"ERROR[{self.agent_id}]: {exc}")
            threading.Thread(target=_run, daemon=True).start()
            return
        if self.exec_cmd:
            def _run_cmd():
                try:
                    import subprocess
                    proc = subprocess.run(
                        self.exec_cmd, shell=True, input=json.dumps(data),
                        text=True, capture_output=True, timeout=60
                    )
                    out = proc.stdout.strip() if proc.returncode == 0 else f"EXEC_FAIL (rc={proc.returncode}): {proc.stderr.strip()}"
                    self.reply(msg_id, out or "OK")
                except Exception as exc:
                    self._log(f"[{now()}] exec_cmd error: {exc}")
                    self.reply(msg_id, f"EXEC_ERROR: {exc}")
            threading.Thread(target=_run_cmd, daemon=True).start()
            return
        if self.auto_reply:
            def _run_auto():
                reply_text = self._compute_auto_reply(text, data)
                self.reply(msg_id, reply_text)
            threading.Thread(target=_run_auto, daemon=True).start()

    def _compute_auto_reply(self, text: str, data: dict) -> str:
        try:
            val = json.loads(text)
            if isinstance(val, dict):
                action = val.get("action")
                if action == "ping":
                    return json.dumps({"status": "pong", "agent": self.agent_id, "time": now()})
                if action == "echo":
                    return str(val.get("message") or "")
                if action == "compute":
                    expr = str(val.get("expr") or "0")
                    safe_dict = {"__builtins__": None, "abs": abs, "min": min, "max": max, "sum": sum, "round": round}
                    return str(eval(expr, safe_dict, {}))  # noqa: S307
        except Exception:
            pass
        return f"Completed task from {data.get('from')}: '{text}' (agent {self.agent_id})"

    # ---------------- file download (agent pulls through ngrok, NAT-safe)
    def _download_file(self, meta: dict) -> dict:
        file_id = meta.get("file_id", "")
        url = f"{self.base}/file/{file_id}"
        try:
            r = requests.get(url, headers=self._auth_headers(), timeout=30)
        except requests.exceptions.RequestException as exc:
            self._write_inbox({"type": "file_error", "file_id": file_id, "error": str(exc)})
            return {}
        if r.status_code != 200:
            self._write_inbox({"type": "file_error", "file_id": file_id,
                               "status": r.status_code})
            return {}
        sha = hashlib.sha256(r.content).hexdigest()
        raw_name = meta.get("name") or f"{file_id}.bin"
        safe_name = os.path.basename(raw_name) or f"{file_id}.bin"
        path = self.state / "downloads" / safe_name
        try:
            path.write_bytes(r.content)
        except OSError as exc:
            self._write_inbox({"type": "file_error", "file_id": file_id, "name": safe_name,
                               "error": f"write failed: {exc}"})
            return {}
        info = {"type": "file", "file_id": file_id, "name": safe_name,
                "path": str(path), "size": len(r.content),
                "sha_ok": sha == meta.get("sha256")}
        self._write_inbox(info)
        return info

    def _auth_headers(self) -> dict:
        # X-Agent-Id goes along as a label, but once the hub has minted a credential the hub
        # takes the caller's identity from that credential and ignores this header.
        return {"X-Agent-Token": self.agent_token or self.token,
                "X-Agent-Id": self.agent_id,
                "Accept": "application/json",
                "ngrok-skip-browser-warning": "true"}

    # ---------------- programmatic API helpers
    def reply(self, msg_id: str, text: str, kind: str = "") -> None:
        """Answer a pending task directly via Socket.IO."""
        payload = {"msg_id": msg_id, "text": text}
        if kind:
            payload["kind"] = kind
        self.sio.emit("result", payload, namespace="/agents")
        self._write_inbox({"type": "sent_reply", "msg_id": msg_id, "text": text})

    def send_to_agent(self, to: str, text: str, timeout: float = 8.0) -> dict:
        """Relay a message to another connected agent over Socket.IO."""
        try:
            ack = self.sio.call("agent_to_agent", {"to": to, "text": text},
                                namespace="/agents", timeout=timeout)
            self._write_inbox({"type": "relay_ack", "to": to, "ack": ack})
            return ack if isinstance(ack, dict) else {"status": "relayed", "to": to}
        except Exception as exc:
            self._write_inbox({"type": "relay_error", "to": to, "error": str(exc)})
            return {"error": str(exc), "to": to}

    def send_to_client(self, text: str, msg_id: str = "") -> None:
        """Send a note to client (queued on GET /agent/<id>/inbox)."""
        payload = {"text": text}
        if msg_id:
            payload["msg_id"] = msg_id
        self.sio.emit("agent_to_client", payload, namespace="/agents")
        self._write_inbox({"type": "sent_to_client", "msg_id": msg_id, "text": text})

    def upload_file(self, path: str | Path, to: str = "") -> dict:
        """Upload a file to the hub and optionally notify a target agent."""
        p = Path(path)
        if not p.is_absolute():
            p = Path(__file__).resolve().parent / p
        if not p.is_file():
            err = f"file not found: {p}"
            self._write_inbox({"type": "upload_error", "path": str(p), "error": err})
            return {"error": err, "status": 404}
        headers = self._auth_headers()
        if to:
            headers["X-Target-Agent"] = to
        try:
            r = requests.post(f"{self.base}/file", headers=headers, timeout=30,
                              files={"file": file_part(p)})
            resp = r.json() if r.ok else {"error": r.text[:120], "status_code": r.status_code}
            self._write_inbox({"type": "upload_ack", "name": p.name, "to": to,
                               "status": r.status_code, "resp": resp})
            return resp
        except Exception as exc:
            self._write_inbox({"type": "upload_error", "name": p.name, "error": str(exc)})
            return {"error": str(exc), "status": 500}

    def start_background(self) -> threading.Thread:
        """Connect and start listening in a background daemon thread."""
        t = threading.Thread(target=self.run_forever, daemon=True)
        t.start()
        t0 = time.time()
        while time.time() - t0 < 5.0:
            if self.sio.connected or not self.running:
                break
            time.sleep(0.1)
        return t

    def stop(self) -> None:
        """Cleanly stop the agent and disconnect socket."""
        self.deliberate_stop = True
        self.running = False
        try:
            self.sio.disconnect()
        except Exception:
            pass
        self._revoke_credential("agent stopped programmatically")

    # ---------------- outbox watcher: operator writes JSON actions here
    def _watch_outbox(self) -> None:
        # Byte-offset tailing lost actions: a '>' redirect or an editor save makes the file
        # shorter than the stored offset, so seek() landed past EOF (read '' - nothing ran) or
        # mid-line (garbage json). Track the consumed *prefix* instead and notice rewrites.
        consumed = b""
        if self.outbox.exists():
            try:
                pre = self.outbox.read_bytes()
            except OSError:
                pre = b""
            cut = pre.rfind(b"\n")
            consumed = pre[:cut + 1] if cut >= 0 else b""
            if consumed:
                n = consumed.count(b"\n")
                self._log(f"[{now()}] OUTBOX: ignoring {n} action line(s) queued before this "
                          f"process started (they are not replayed)")
        while self.running:
            time.sleep(1.0)
            try:
                data = self.outbox.read_bytes()
            except OSError:
                continue
            if consumed and not data.startswith(consumed):
                self._log(f"[{now()}] OUTBOX was rewritten rather than appended - re-reading it "
                          f"from the top; any action it already ran may run a second time")
                consumed = b""
            cut = data.rfind(b"\n")
            if cut < 0:
                continue                       # nothing complete to act on yet
            body = data[:cut + 1]              # only consume whole lines
            if len(body) <= len(consumed):
                continue
            new = body[len(consumed):]
            consumed = body
            for line in new.decode("utf-8", "replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    act = json.loads(line)
                except json.JSONDecodeError:
                    self._log(f"[{now()}] OUTBOX bad json line: {line[:60]}")
                    continue
                try:
                    self._run_action(act)
                except Exception as exc:
                    self._log(f"[{now()}] OUTBOX action '{act.get('action')}' failed: {exc}")
                    self._write_inbox({"type": "outbox_error", "action": act.get("action"),
                                       "error": str(exc), "act": act})

    def _run_action(self, act: dict) -> None:
        action = act.get("action")
        if action == "to_agent":
            to = str(act.get("to") or "")
            text = str(act.get("text") or "")
            if not to or not text:
                self._write_inbox({"type": "outbox_error", "action": action,
                                   "error": "missing 'to' or 'text'"})
                return
            self.send_to_agent(to, text)
        elif action == "to_client":
            text = str(act.get("text") or "")
            msg_id = act.get("msg_id", "")
            self.send_to_client(text, msg_id=msg_id)
        elif action == "reply":
            msg_id = str(act.get("msg_id") or "")
            text = str(act.get("text") or "")
            kind = str(act.get("kind") or "")
            if not msg_id:
                self._write_inbox({"type": "outbox_error", "action": action,
                                   "error": "missing 'msg_id'"})
                return
            self.reply(msg_id, text, kind=kind)
        elif action == "upload":
            raw_path = act.get("path")
            if not raw_path:
                self._write_inbox({"type": "outbox_error", "action": action,
                                   "error": "missing 'path'"})
                return
            self.upload_file(raw_path, to=act.get("to", ""))
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

        # A supervisor's SIGTERM used to kill the process mid-socket, leaving the mirrored
        # credential on disk after the hub had already revoked it. Stop through the same exit path
        # Ctrl+C uses, so the file goes with the socket.
        def _stopped(signum, _frame):
            self._log(f"[{now()}] SIGNAL {signum}: stopping - socket closes and its credential "
                      f"is revoked with it")
            self.deliberate_stop = True      # a supervisor's stop is not a crash: exit 0
            self.running = False

        for sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGHUP", None)):
            if sig is None:
                continue
            try:
                signal.signal(sig, _stopped)
            except (OSError, ValueError):   # not this platform, or not the main thread
                pass
        try:
            self.sio.connect(self.base, namespaces=["/agents"], auth=auth, wait_timeout=15,
                             headers=SOCKET_HEADERS)
        except Exception as exc:  # noqa: BLE001 - surface auth errors clearly
            print(f"[{now()}] CONNECT FAILED (hub refused): {exc}\n"
                  f"  - asking the hub why:", file=sys.stderr)
            why = diagnose(str(exc))
            if why:
                print(f"  - {why}", file=sys.stderr)
            verdict = hub_verdict(self.base)
            print(f"  - right now: {verdict}", file=sys.stderr)
            if "ALIVE" in verdict:      # only now is "who holds this id" a meaningful question
                self._why_refused()
            self._revoke_credential("no socket in this process ever opened")
            return 2
        self._log(f"[{now()}] agent '{self.agent_id}' listening | "
                  f"inbox={self.inbox} | outbox={self.outbox}")
        self._write_inbox({"type": "hello", "text": f"{self.agent_id} online via {self.base}"})
        pr = self.prune_state()
        if RETENTION_DAYS:
            extra = (f" | startup pruned {pr['inbox_removed']} row(s), "
                     f"{pr['downloads_removed']} download(s)") if any(pr.values()) else ""
            self._log(f"[{now()}] retention: inbox rows and downloads older than "
                      f"{RETENTION_DAYS}d age out every {RETENTION_SWEEP_SECONDS}s{extra}")
        threading.Thread(target=self._watch_outbox, daemon=True).start()
        next_prune = time.time() + RETENTION_SWEEP_SECONDS
        try:
            down_since = None
            told = ""
            next_tell = 0.0
            while self.running:
                if time.time() >= next_prune:
                    next_prune = time.time() + RETENTION_SWEEP_SECONDS
                    pr = self.prune_state()
                    if any(pr.values()):
                        self._log(f"[{now()}] RETAIN: pruned {pr['inbox_removed']} inbox row(s), "
                                  f"{pr['downloads_removed']} download(s) "
                                  f"({pr['bytes']} B) older than {RETENTION_DAYS}d")
                if self.sio.connected:
                    down_since = None
                    told = ""
                else:
                    down_since = down_since or time.time()
                    # A reconnect that fails inside engineio's own thread fires no event at all,
                    # so the outage would be silent for the whole give-up window. Ask the hub
                    # directly and say what came back - once per verdict, not every tick.
                    if time.time() - down_since >= 10 and time.time() >= next_tell:
                        verdict = hub_verdict(self.base)
                        next_tell = time.time() + 30
                        if verdict != told:
                            told = verdict
                            self._log(f"[{now()}] SOCKET DOWN for "
                                      f"{int(time.time() - down_since)}s - {verdict} "
                                      f"(still retrying; giving up at "
                                      f"{RECONNECT_GIVEUP_AFTER}s without it)")
                    if time.time() - down_since >= RECONNECT_GIVEUP_AFTER:
                        self._log(f"[{now()}] GIVING UP: no usable socket for "
                                  f"{RECONNECT_GIVEUP_AFTER}s (last hub answer: "
                                  f"{self.last_refusal or 'nothing - connection never opened'})"
                                  f" - exiting nonzero so a supervisor restarts this agent")
                        self.running = False
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.deliberate_stop = True
            self.running = False
            try:
                self.sio.disconnect()
            except Exception:  # noqa: BLE001 - already gone is fine here
                pass
        # The socket is gone on every path that reaches here, so the hub has revoked our
        # credential - take the on-disk copy with it rather than leaving a dead one behind.
        self._revoke_credential("this process is exiting")
        # Only a Ctrl+C is a clean exit. Ending any other way means the socket was lost for
        # good, and exit 0 would tell a supervisor "nothing to restart".
        return 0 if self.deliberate_stop else 1


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
    ap.add_argument("--credential", action="store_true",
                    help="print where this agent_id's minted credential lives, and exit")
    ap.add_argument("--use-credential", action="store_true",
                    default=os.environ.get("AGENT_USE_CREDENTIAL", "").lower() in ("1", "true", "yes"),
                    help="one-shot: present the credential minted for --agent-id (from its file) "
                         "instead of the master token, so the hub labels the call agent:<id>")
    ap.add_argument("--no-credential-file", action="store_true",
                    help="never mirror the minted credential into the state dir (env AGENT_CREDENTIAL_FILE=0)")
    ap.add_argument("--check-id", metavar="AGENT_ID", help="GET /agent-id/<id> and exit (is it free?)")
    ap.add_argument("--health", action="store_true", help="GET /health and exit")
    ap.add_argument("--docs", action="store_true", help="GET /llms.txt and exit")
    ap.add_argument("--auto-reply", action="store_true",
                    help="persistent mode: auto-reply to tasks with computed results instead of only ACKing")
    ap.add_argument("--exec-cmd", metavar="COMMAND",
                    help="persistent mode: execute shell command on task (receives task JSON on stdin, stdout is reply)")
    ap.add_argument("--result", metavar="MSG_ID",
                    help="GET /result/<msg_id> and exit (query task state & replies)")
    ap.add_argument("--await-reply", action="store_true",
                    help="with --message: poll GET /result/<msg_id> until answered or timeout")
    ap.add_argument("--dead-letter", action="store_true",
                    help="GET /tasks/dead-letter and exit")
    ap.add_argument("--events", action="store_true",
                    help="GET /events/mine and exit")
    ap.add_argument("--delete-file", metavar="FILE_ID",
                    help="DELETE /file/<file_id> and exit")
    args = ap.parse_args()

    one_shot = any([args.message, args.inbox, args.relay_to, args.upload, args.download,
                    args.agents, args.health, args.docs, args.check_id, args.credential,
                    args.result, args.dead_letter, args.events, args.delete_file])
    if args.use_credential and not (args.agent_id and args.agent_id != "operator"):
        ap.error("--use-credential needs the --agent-id whose credential to present "
                 "(env AGENT_ID) - a credential belongs to one named agent")
    if not args.token and not (args.use_credential or args.credential):
        ap.error("--token (AGENT_AUTH_TOKEN) is required - or --use-credential to present an "
                 "already-minted agent credential instead")
    if not args.agent_id:
        if not one_shot:
            ap.error("--agent-id is required for persistent mode (env: AGENT_ID)")
        args.agent_id = "operator"
    agent = Agent(args.server, args.agent_id, args.token, force=args.force_takeover,
                  no_credential_file=args.no_credential_file,
                  auto_reply=args.auto_reply, exec_cmd=args.exec_cmd or "")

    if args.credential:
        minted = agent.read_credential()
        out = {
            "agent_id": args.agent_id,
            "hub": agent.base,
            "credential_file": str(agent.cred_path),
            "present_on_disk": bool(minted),
            "written_by": "the persistent client, at connect; deleted when that socket dies",
            "read_the_value": f"cat {agent.cred_path}",
            "use_it": f"python3 mock_agent.py --server {args.server} --agent-id {args.agent_id} "
                      f"--use-credential --inbox {args.agent_id} --peek",
        }
        if not minted:
            out["proof"] = {"verdict": "nothing on disk to present - start the persistent client "
                                       "and the hub mints one down that socket"}
        else:
            agent.agent_token = minted
            # /tasks/dead-letter?limit=1 is the smallest call whose answer differs by principal:
            # `scoped_to` is the id the hub read off the credential, while the master token is
            # all-seeing and answers "all agents". Nothing about it needs a header to believe.
            v = requests.get(f"{agent.base}/tasks/dead-letter", headers=agent._auth_headers(),
                             params={"limit": 1}, timeout=20)
            got = ""
            try:
                got = str(v.json().get("scoped_to") or "") if v.ok else ""
            except ValueError:
                got = ""
            if got == args.agent_id:
                verdict = (f"LIVE - the hub identified this caller as '{got}' from the credential "
                           f"alone (the master token answers scoped_to='all agents')")
            elif not v.ok:
                verdict = (f"REFUSED ({v.status_code}) - the socket that minted it is gone and the "
                           f"hub revoked the credential; have the agent reconnect")
            else:
                verdict = f"answered scoped_to='{got or '-'}', not '{args.agent_id}'"
            out["proof"] = {"call": "GET /tasks/dead-letter?limit=1", "http": v.status_code,
                            "scoped_to": got or None, "verdict": verdict}
        print(json.dumps(out, indent=2, default=str))
        return

    if args.use_credential:
        minted = agent.read_credential()
        if not minted:
            print(f"--use-credential: no usable credential for '{args.agent_id}' at "
                  f"{agent.cred_path}\n"
                  f"  - it is written when the agent connects: python3 mock_agent.py "
                  f"--server {args.server} --agent-id {args.agent_id} --token \"$AGENT_AUTH_TOKEN\"\n"
                  f"  - and deleted the moment that socket dies (the hub revokes it then too), "
                  f"so a hub restart means the agent must reconnect first", file=sys.stderr)
            sys.exit(3)
        agent.agent_token = minted

    def actor_note() -> None:
        # A one-shot with the master token is recorded as operator:<label> even when this very box
        # holds a credential that would record it as agent:<id> - say so instead of leaving the
        # log full of unattributable rows.
        if not args.use_credential and agent.share_credential and agent.cred_path.exists():
            print(f"note: this call used the master token, so the hub recorded it as "
                  f"'operator:{args.agent_id}'. A credential for '{args.agent_id}' is on disk at "
                  f"{agent.cred_path} - --use-credential makes the same call land as "
                  f"'agent:{args.agent_id}'")

    if args.docs:
        r = requests.get(f"{agent.base}/llms.txt", timeout=20)
        print(r.text)
        return
    if args.health:
        ok = emit(requests.get(f"{agent.base}/health", headers=agent._auth_headers(), timeout=20))
        if not ok:
            sys.exit(1)
        return
    if args.agents:
        ok = emit(requests.get(f"{agent.base}/agents", headers=agent._auth_headers(), timeout=20))
        if not ok:
            sys.exit(1)
        return
    if args.check_id:
        ok = emit(requests.get(f"{agent.base}/agent-id/{args.check_id}",
                               headers=agent._auth_headers(), timeout=20))
        if not ok:
            sys.exit(1)
        return
    if args.result:
        ok = emit(requests.get(f"{agent.base}/result/{args.result}",
                               headers=agent._auth_headers(), timeout=20))
        actor_note()
        if not ok:
            sys.exit(1)
        return
    if args.dead_letter:
        ok = emit(requests.get(f"{agent.base}/tasks/dead-letter",
                               headers=agent._auth_headers(), timeout=20))
        actor_note()
        if not ok:
            sys.exit(1)
        return
    if args.events:
        ok = emit(requests.get(f"{agent.base}/events/mine",
                               headers=agent._auth_headers(), timeout=20))
        actor_note()
        if not ok:
            sys.exit(1)
        return
    if args.delete_file:
        ok = emit(requests.delete(f"{agent.base}/file/{args.delete_file}",
                                  headers=agent._auth_headers(), timeout=20))
        actor_note()
        if not ok:
            sys.exit(1)
        return
    if args.message:
        url = f"{agent.base}/agent/{args.message}/message"
        if args.wait is not None:
            url += f"?wait={args.wait}"
        r = requests.post(url, headers=agent._auth_headers(), timeout=75,
                          json={"text": args.text})
        ok = emit(r)
        msg_id = ""
        try:
            data = r.json()
            msg_id = data.get("msg_id", "")
            if data.get("status") == "replied" and not args.await_reply:
                print("note: with mock_agent this is usually the instant ACK; the real answer "
                      f"lands later - check: python3 mock_agent.py --inbox {args.message} --peek "
                      f"or add --await-reply to wait for the answer")
        except ValueError:
            pass
        actor_note()
        if args.await_reply and msg_id:
            print(f"[{now()}] waiting for final answer on /result/{msg_id} ...")
            deadline = time.time() + (args.wait or 30.0)
            answered = False
            while time.time() < deadline:
                time.sleep(1.0)
                res = requests.get(f"{agent.base}/result/{msg_id}",
                                   headers=agent._auth_headers(), timeout=10)
                if res.ok:
                    rj = res.json()
                    if rj.get("state") == "answered" or len(rj.get("results", [])) > 1:
                        print(f"[{now()}] task completed! Final result:")
                        emit(res)
                        answered = True
                        break
            if not answered:
                print(f"[{now()}] timed out waiting for final answer on {msg_id}", file=sys.stderr)
                sys.exit(1)
        if not ok:
            sys.exit(1)
        return
    if args.inbox:
        url = f"{agent.base}/agent/{args.inbox}/inbox"
        if args.peek:
            url += "?peek=true"
        ok = emit(requests.get(url, headers=agent._auth_headers(), timeout=20))
        if not args.peek:
            print("note: GET /agent/<id>/inbox DRAINS AND CLEARS the queue (use --peek to inspect)")
        if not ok:
            sys.exit(1)
        return
    if args.relay_to:
        ok = emit(requests.post(f"{agent.base}/relay", headers=agent._auth_headers(), timeout=20,
                                json={"to": args.relay_to, "text": args.text}))
        actor_note()
        if not ok:
            sys.exit(1)
        return
    if args.upload:
        p = Path(args.upload)
        if not p.is_file():
            print(json.dumps({"error": f"local file '{args.upload}' not found",
                              "path": str(p)}, indent=2), file=sys.stderr)
            sys.exit(1)
        headers = agent._auth_headers()
        if args.to:
            headers["X-Target-Agent"] = args.to
        ok = emit(requests.post(f"{agent.base}/file", headers=headers, timeout=60,
                                files={"file": file_part(p)}))
        actor_note()
        if not ok:
            sys.exit(1)
        return
    if args.download:
        r = requests.get(f"{agent.base}/file/{args.download}", headers=agent._auth_headers(),
                         timeout=60)
        if r.ok:
            # one row, not the whole store: an older hub ignores ids= and answers with the full
            # table, which this still reads correctly - it just pays 134 KB to name one file.
            files = requests.get(f"{agent.base}/files?ids={args.download}",
                                 headers=agent._auth_headers(), timeout=20).json().get("files", {})
            meta = files.get(args.download, {})
            name = os.path.basename(meta.get("name") or f"{args.download}.bin")
            dest = agent.state / "downloads" / name
            dest.write_bytes(r.content)
            print(f"saved -> {dest} ({len(r.content)} B, sha_ok="
                  f"{hashlib.sha256(r.content).hexdigest() == meta.get('sha256')})")
        else:
            emit(r)
            actor_note()
            sys.exit(1)
        actor_note()
        return
    sys.exit(agent.run_forever())


if __name__ == "__main__":
    main()
