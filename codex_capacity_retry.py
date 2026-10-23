#!/usr/bin/env python3
"""Independent macOS Codex Desktop capacity retry monitor.

This single-file tool uses only the Python standard library plus macOS
``ps``/``lsof``. It discovers CODEX_HOME paths from live ChatGPT/Codex
processes, reads live App IPC snapshots, and retries model-capacity failures.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import sqlite3
import stat
import socket
import struct
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse
import uuid


class BridgeError(Exception):
    code = "bridge_error"


class ValidationError(BridgeError):
    code = "validation"


class Unavailable(BridgeError):
    code = "unavailable"


class ProtocolError(BridgeError):
    code = "protocol"


class DeliveryUnknown(BridgeError):
    code = "delivery_unknown"


class Rejected(BridgeError):
    code = "rejected"


def thread_id(value):
    if value.startswith("codex://"):
        parsed = urlparse(value)
        if parsed.netloc != "threads" or parsed.query or parsed.fragment:
            raise ValidationError("Expected codex://threads/<UUID> without query or fragment")
        value = parsed.path.removeprefix("/")
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
        raise ValidationError("Invalid thread ID") from None


def address(home, tid):
    instance = hashlib.sha256(str(Path(home).resolve()).encode()).hexdigest()[:16]
    return f"agent-bridge://{instance}/{thread_id(tid)}"


MAX_FRAME = 256 * 1024 * 1024
VERSIONS = {"initialize": 0, "thread-owner-discovery": 1,
            "thread-follower-start-turn": 2}


def send_frame(sock, message):
    raw = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode()
    if not raw or len(raw) > MAX_FRAME:
        raise ProtocolError("Outgoing IPC frame exceeds limit")
    sock.sendall(struct.pack("<I", len(raw)) + raw)


def read_exact(sock, count, deadline=None):
    chunks = bytearray()
    while len(chunks) < count:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("IPC frame deadline expired")
            sock.settimeout(remaining)
        try:
            chunk = sock.recv(count - len(chunks))
        except socket.timeout as exc:
            raise TimeoutError("IPC receive timed out") from exc
        if not chunk:
            raise EOFError("IPC disconnected")
        chunks.extend(chunk)
    return bytes(chunks)


def receive_frame(sock, deadline=None):
    size = struct.unpack("<I", read_exact(sock, 4, deadline))[0]
    if not 0 < size <= MAX_FRAME:
        raise ProtocolError("Invalid IPC frame size")
    try:
        value = json.loads(read_exact(sock, size, deadline))
    except (ValueError, UnicodeError) as exc:
        raise ProtocolError("Invalid IPC JSON") from exc
    if not isinstance(value, dict):
        raise ProtocolError("IPC frame must contain an object")
    return value


def is_busy(state):
    if state.get("unconfirmedTurnSubmissions"):
        return True
    def active(obj):
        if isinstance(obj, dict):
            if obj.get("status") in ("inProgress", "in_progress"):
                return True
            return any(active(v) for k, v in obj.items()
                       if k not in ("items", "itemsByKey", "input", "content"))
        if isinstance(obj, list):
            return any(active(v) for v in obj)
        return False
    return active(state.get("turnHistory", {}))


# Preserve only the fields needed for capacity detection and native retry.
# Item placeholders retain array positions for subsequent Immer patches.
_KEEP = object()
_ERROR_FIELDS = {key: _KEEP for key in ("message", "codexErrorInfo", "errorInfo")}
_TURN_FIELDS = {key: _KEEP for key in ("turnId", "turnStartedAtMs", "status")}
_TURN_FIELDS.update(error=_ERROR_FIELDS, items={"*": {"type": _KEEP, **_ERROR_FIELDS}})
_STATE_FIELDS = {
    "latestModel": _KEEP, "latestCollaborationMode": _KEEP,
    "unconfirmedTurnSubmissions": {"*": {}},
    "turns": {"*": _TURN_FIELDS},
    "turnHistory": {"history": {"entitiesByKey": {"*": _TURN_FIELDS}}},
}


def _project(value, fields):
    if fields is _KEEP or value is None:
        return value
    if not fields:
        return None
    if isinstance(value, list):
        if "*" not in fields:
            raise ProtocolError("Invalid projected array schema")
        return [_project(item, fields["*"]) for item in value]
    if isinstance(value, dict):
        return {key: _project(item, fields.get(key, fields.get("*")))
                for key, item in value.items() if key in fields or "*" in fields}
    # Error codes can be scalars; preserve their native representation.
    return value


def _patch_fields(path):
    fields = _STATE_FIELDS
    for part in path:
        if fields is _KEEP:
            return _KEEP
        if part == "length" and "*" in fields:
            return _KEEP
        fields = fields.get(part, fields.get("*"))
        if fields is None:
            return None
    return fields


def _apply_status_patch(state, patch):
    if not isinstance(patch, dict):
        raise ProtocolError("Invalid stream patch")
    path, op = patch.get("path"), patch.get("op")
    if (not isinstance(path, list) or op not in ("add", "remove", "replace")
            or any(not isinstance(part, (str, int)) or isinstance(part, bool) for part in path)):
        raise ProtocolError("Unsupported stream patch")
    fields = _patch_fields(path)
    if fields is None:
        return state
    if not path:
        if op == "remove" or not isinstance(patch.get("value"), dict):
            raise ProtocolError("Invalid stream root replacement")
        return _project(patch["value"], _STATE_FIELDS)
    parent = state
    try:
        for key in path[:-1]:
            parent = parent[key]
        key = path[-1]
        value = _project(patch.get("value"), fields)
        if isinstance(parent, list):
            if key == "length" and op == "replace":
                if not isinstance(value, int) or not 0 <= value <= len(parent):
                    raise ProtocolError("Invalid projected array length")
                del parent[value:]
            elif not isinstance(key, int) or not 0 <= key <= len(parent):
                raise ProtocolError("Invalid projected array index")
            elif op == "add":
                parent.insert(key, value)
            elif op == "remove":
                del parent[key]
            else:
                parent[key] = value
        elif isinstance(parent, dict):
            if op == "remove":
                del parent[key]
            else:
                parent[key] = value
        else:
            raise ProtocolError("Invalid projected patch parent")
    except (KeyError, IndexError, TypeError) as exc:
        raise ProtocolError("Stream patch does not match cached state") from exc
    return state


class Client:
    def __init__(self, home, timeout=0.75):
        self.home, self.timeout = Path(home), timeout
        self.sock = None
        self.client_id = "initializing-client"
        self.following = None
        self.state = None
        self.revision = None

    def __enter__(self):
        path = self.home / "ipc/ipc.sock"
        try:
            parent, entry = path.parent.lstat(), path.lstat()
            if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or parent.st_mode & 0o022
                    or not stat.S_ISSOCK(entry.st_mode) or entry.st_uid != os.getuid() or entry.st_mode & 0o077):
                raise ProtocolError("IPC endpoint ownership or permissions are unsafe")
            self.sock = socket.socket(socket.AF_UNIX)
            self.sock.settimeout(self.timeout)
            self.sock.connect(str(path))
            response = self.request("initialize", {"clientType": "codex-capacity-retry"})
            result = response.get("result")
            if not isinstance(result, dict) or not isinstance(result.get("clientId"), str):
                raise ProtocolError("Invalid initialize response")
            self.client_id = result["clientId"]
            return self
        except (OSError, EOFError) as exc:
            self.close()
            raise Unavailable(f"App IPC unavailable: {type(exc).__name__}") from exc
        except BaseException:
            self.close()
            raise

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.sock:
            if self.following:
                try:
                    self.follow(*self.following, value=False)
                except (OSError, EOFError, ProtocolError):
                    pass
            self.sock.close()
            self.sock = None

    def read(self, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("IPC deadline expired")
        self.sock.settimeout(remaining)
        message = receive_frame(self.sock, deadline)
        if message.get("type") == "client-discovery-request":
            send_frame(self.sock, {"type": "client-discovery-response", "requestId": message["requestId"],
                                    "response": {"canHandle": False}})
        self._consume_change(message)
        return message

    def _consume_change(self, message):
        if not self.following:
            return
        tid, owner = self.following
        method, params = message.get("method"), message.get("params", {})
        if method == "ipc-connection-reset" or (
            method == "client-status-changed" and params.get("clientId") == owner
            and params.get("status") == "disconnected"
        ):
            raise ProtocolError("Thread stream owner disconnected; resync required")
        if method == "thread-stream-following-status-requested" and params.get("conversationId") == tid:
            raise ProtocolError("Thread stream subscription reset; resync required")
        if (method != "thread-stream-state-changed" or params.get("conversationId") != tid
                or message.get("sourceClientId") != owner):
            return
        if message.get("version") != 11:
            raise ProtocolError(f"Unsupported stream snapshot version: {message.get('version')} (expected 11)")
        change = params.get("change", {})
        revision = change.get("revision")
        if not isinstance(revision, int):
            raise ProtocolError("Stream revision missing")
        if change.get("type") == "snapshot":
            state = change.get("conversationState")
            if not isinstance(state, dict):
                raise ProtocolError("Invalid conversation snapshot")
            self.state = _project(state, _STATE_FIELDS)
        elif change.get("type") == "patches":
            if self.state is None or change.get("baseRevision") != self.revision or revision <= self.revision:
                raise ProtocolError("Thread stream revision gap; resync required")
            patches = change.get("patches")
            if not isinstance(patches, list):
                raise ProtocolError("Invalid stream patches")
            for patch in patches:
                self.state = _apply_status_patch(self.state, patch)
        else:
            raise ProtocolError("Unsupported stream change; resync required")
        self.revision = revision

    def request(self, method, params, target=None, mutation=False):
        rid = str(uuid.uuid4())
        packet = {"type": "request", "requestId": rid, "sourceClientId": self.client_id,
                  "version": VERSIONS[method], "method": method, "params": params,
                  "timeoutMs": int(self.timeout * 1000)}
        if target:
            packet["targetClientId"] = target
        try:
            send_frame(self.sock, packet)
            deadline = time.monotonic() + self.timeout
            while True:
                response = self.read(deadline)
                if response.get("type") == "response" and response.get("requestId") == rid:
                    break
        except (OSError, EOFError, ProtocolError) as exc:
            if mutation:
                raise DeliveryUnknown(f"IPC result unknown: {type(exc).__name__}") from exc
            if isinstance(exc, ProtocolError):
                raise
            raise Unavailable(f"IPC read failed: {type(exc).__name__}") from exc
        if response.get("resultType") != "success":
            error = response.get("error", "invalid-response")
            if mutation and error in ("request-timeout", "client-disconnected", "server-closed"):
                raise DeliveryUnknown(error)
            if error == "no-client-found":
                raise Unavailable(error)
            raise Rejected(str(error))
        if response.get("method") != method:
            if mutation:
                raise DeliveryUnknown("IPC response method mismatch")
            raise ProtocolError("IPC response method mismatch")
        return response

    def owner(self, tid):
        response = self.request("thread-owner-discovery", {"hostId": "local", "conversationId": tid})
        owner = response.get("handledByClientId")
        if not owner:
            raise ProtocolError("Owner response has no client ID")
        return owner

    def follow(self, tid, owner, value=True):
        self.state = self.revision = None
        send_frame(self.sock, {"type": "broadcast", "sourceClientId": self.client_id, "version": 1,
                               "method": "thread-stream-following-changed", "targetClientIds": [owner],
                               "params": {"hostId": "local", "conversationId": tid, "following": value}})
        self.following = (tid, owner) if value else None

    def snapshot(self, tid, owner):
        if self.following == (tid, owner) and self.state is not None:
            # The owner lookup has already consumed queued changes. Drain any
            # remaining frames without sending another following=true, which
            # would ask the App to publish the full history again.
            deadline = time.monotonic() + self.timeout
            while select.select([self.sock], [], [], 0)[0]:
                self.read(deadline)
            return self.state
        if self.following:
            self.follow(*self.following, value=False)
        self.follow(tid, owner)
        deadline = time.monotonic() + self.timeout
        try:
            while self.state is None:
                self.read(deadline)
            return self.state
        except (OSError, EOFError) as exc:
            raise Unavailable(f"Snapshot unavailable: {type(exc).__name__}") from exc


CAPACITY_ERROR = "Selected model is at capacity. Please try a different model."
CAPACITY_ERROR_INFO = "serverOverloaded"
AUTOMATIC_TRIGGER = "capacity_retry_automatic"
FULL_ROUTE_SWEEP_SECONDS = 30.0


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result else default


def _turns(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    entities = state.get("turnHistory", {}).get("history", {}).get("entitiesByKey", {})
    values = list(entities.values()) if isinstance(entities, dict) else []
    if not values and isinstance(state.get("turns"), list):
        values = [item for item in state["turns"] if isinstance(item, dict)]
    return values


def _turn_sort_key(turn: Dict[str, Any], position: int) -> Tuple[float, int]:
    # A missing timestamp is unusual in a live snapshot.  Preserve the App's
    # list order as a deterministic fallback rather than guessing from IDs.
    return (_number(turn.get("turnStartedAtMs"), 0.0), position)


def _error_matches(error: Any) -> bool:
    if not isinstance(error, dict):
        return False
    if error.get("message") != CAPACITY_ERROR:
        return False
    info = error.get("codexErrorInfo") or error.get("errorInfo")
    if isinstance(info, dict):
        info = info.get("type") or info.get("code") or info.get("name")
    # The UI text is the primary discriminator.  The structured code is an
    # additional guard when it is present, while older snapshots may omit it.
    return info in (None, CAPACITY_ERROR_INFO)


def _turn_has_capacity_error(turn: Dict[str, Any]) -> bool:
    if _error_matches(turn.get("error")):
        return True
    for item in turn.get("items", []) if isinstance(turn.get("items"), list) else []:
        if isinstance(item, dict) and item.get("type") == "error" and _error_matches(item):
            return True
    return False


def find_capacity_failure(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the newest failed capacity turn, but only if it is still latest.

    Retrying an old failed turn after the user has already continued the
    conversation would create a second, out-of-order task.  The latest-turn
    check is therefore intentional and is part of the safety contract.
    """

    turns = _turns(state)
    if not turns:
        return None
    latest = max(enumerate(turns), key=lambda pair: _turn_sort_key(pair[1], pair[0]))[1]
    if latest.get("status") != "failed" or not _turn_has_capacity_error(latest):
        return None
    if is_busy(state):
        return None
    turn_id = latest.get("turnId")
    if not isinstance(turn_id, str) or not turn_id:
        return None
    return {
        "turn_id": turn_id,
        "status": latest.get("status"),
        "error": CAPACITY_ERROR,
        "model": state.get("latestModel"),
        "collaboration_mode": state.get("latestCollaborationMode"),
        "turn": latest,
    }


def send_capacity_retry(
    client: Client,
    conversation_id: str,
    owner: str,
    collaboration_mode: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Submit the same empty-turn shape used by the desktop retry control."""

    request: Dict[str, Any] = {
        "threadId": conversation_id,
        "turnTrigger": AUTOMATIC_TRIGGER,
        "clientUserMessageId": str(uuid.uuid4()),
        "input": [],
    }
    if isinstance(collaboration_mode, dict):
        # Keep only the native shape.  This avoids forwarding arbitrary state
        # keys from a future snapshot into an internal mutation request.
        mode = collaboration_mode.get("mode")
        settings = collaboration_mode.get("settings")
        if isinstance(mode, str) and isinstance(settings, dict):
            request["collaborationMode"] = {
                "mode": mode,
                "settings": dict(settings),
            }
    params = {
        "conversationId": conversation_id,
        "turnStart": {
            "request": request,
            "context": {"inheritThreadSettings": True},
        },
    }
    result = client.request(
        "thread-follower-start-turn",
        params,
        target=owner,
        mutation=True,
    ).get("result", {})
    # Desktop's follower router wraps the owner result once more; keep the
    # public helper stable for both that shape and test/future adapters.
    if isinstance(result, dict) and isinstance(result.get("result"), dict):
        return result["result"]
    return result


def _latest_state_db(home: Path) -> Optional[Path]:
    dbs = [p for p in home.glob("state_*.sqlite") if p.is_file()]
    if not dbs:
        return None
    numbered: List[Tuple[int, Path]] = []
    for path in dbs:
        try:
            numbered.append((int(path.stem.split("_", 1)[1]), path))
        except (IndexError, ValueError):
            continue
    return max(numbered, default=(0, dbs[-1]), key=lambda item: item[0])[1]


def _state_index_signature(home: Path) -> Tuple[Tuple[str, int, int, int], ...]:
    """Return a cheap change signature for SQLite state and sidecar files."""

    signature: List[Tuple[str, int, int, int]] = []
    for database in sorted(home.glob("state_*.sqlite")):
        for path in (database, Path(str(database) + "-wal"), Path(str(database) + "-shm")):
            try:
                info = path.stat()
            except OSError:
                continue
            if not path.is_file():
                continue
            signature.append((str(path), int(info.st_ino), int(info.st_size), int(info.st_mtime_ns)))
    return tuple(signature)


def _row_time(row: sqlite3.Row) -> float:
    latest = 0.0
    for key in ("recency_at_ms", "updated_at_ms", "recency_at", "updated_at"):
        value = row[key] if key in row.keys() else None
        if value is None:
            continue
        number = _number(value)
        if number > 10_000_000_000:
            number /= 1000.0
        if number > 0:
            latest = max(latest, number)
    return latest


def _routes_for_home(
    name: str,
    home: Path,
    *,
    lookback_seconds: Optional[float],
    now: float,
) -> Optional[List[Dict[str, Any]]]:
    db = _latest_state_db(home)
    if db is None:
        return None
    try:
        with contextlib.closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            columns = {row[1] for row in conn.execute("PRAGMA table_info(threads)")}
            required = {"id", "rollout_path", "archived"}
            if not required.issubset(columns):
                return None
            rows = conn.execute(
                "SELECT * FROM threads WHERE archived=0"
            ).fetchall()
    except (OSError, sqlite3.Error, ValueError):
        return None

    result: List[Dict[str, Any]] = []
    for row in rows:
        raw_id = row["id"]
        try:
            tid = thread_id(raw_id)
        except (TypeError, ValueError, BridgeError):
            continue
        updated = _row_time(row)
        if lookback_seconds is not None and updated and updated < now - lookback_seconds:
            continue
        try:
            rollout = Path(row["rollout_path"]).resolve()
            rollout.relative_to(home.resolve())
        except (OSError, TypeError, ValueError):
            # Keep the same path-boundary rule as normal bridge routing.
            continue
        result.append({
            "thread_id": tid,
            "home": str(home),
            "address": address(home, tid),
            "instance_name": name,
            "updated": updated,
            "archived": bool(row["archived"]),
            "title": row["title"] if "title" in row.keys() else None,
        })
    result.sort(key=lambda item: item["updated"], reverse=True)
    return result


def discover_routes(
    *,
    lookback_seconds: Optional[float] = 7 * 24 * 3600,
    thread_ids: Optional[Sequence[str]] = None,
    process_homes: Optional[Sequence[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Discover sessions from homes associated with live processes."""

    now = time.time()
    wanted = None
    if thread_ids:
        wanted = {thread_id(item) for item in thread_ids}
    homes: List[Tuple[str, Path]] = []
    seen_homes: set[str] = set()
    for item in process_homes or []:
        home = Path(item["home"]).expanduser().resolve()
        if str(home) not in seen_homes:
            homes.append((f"runtime-{item.get('pid', 'process')}", home))
            seen_homes.add(str(home))
    routes: List[Dict[str, Any]] = []
    for name, home in homes:
        routes.extend(_routes_for_home(name, home, lookback_seconds=lookback_seconds, now=now) or [])
    routes.sort(key=lambda item: item["updated"], reverse=True)
    if wanted is not None:
        routes = [item for item in routes if item["thread_id"] in wanted]
    return routes


def discover_codex_processes() -> List[Dict[str, Any]]:
    """Return local Codex app-server PIDs for diagnostics.

    The PID is deliberately not used as a session identity: one app-server
    owns many threads and a restart changes its PID.  Session operations use
    the per-CODEX_HOME IPC socket and thread index instead.
    """

    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,command="],
            check=False, capture_output=True, text=True, timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    result: List[Dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3:
            continue
        pid, ppid, command = parts
        if "/codex-cli/" not in command or " app-server" not in command:
            continue
        try:
            pid_value, ppid_value = int(pid), int(ppid)
        except ValueError:
            continue
        result.append({"pid": pid_value, "ppid": ppid_value, "command": command})
    return result


def discover_process_homes() -> List[Dict[str, Any]]:
    """Find CODEX_HOME paths actually opened by live ChatGPT/Codex processes.

    The registry is intentionally not consulted here.  We inspect both the
    codex app-server and its ChatGPT parent because either process may retain
    the state database or IPC socket while the other is restarting.
    """

    result: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for command_name in ("codex", "ChatGPT"):
        try:
            completed = subprocess.run(
                ["lsof", "-nP", "-a", "-c", command_name, "-Fpn"],
                check=False, capture_output=True, text=True, timeout=3,
            )
        except (OSError, subprocess.SubprocessError):
            continue
        pid: Optional[int] = None
        for line in completed.stdout.splitlines():
            if line.startswith("p"):
                try:
                    pid = int(line[1:])
                except ValueError:
                    pid = None
                continue
            if not line.startswith("n") or pid is None:
                continue
            path = line[1:]
            state_match = re.search(r"^(.*)/state_\d+\.sqlite(?:-(?:shm|wal))?$", path)
            socket_match = re.search(r"^(.*)/ipc/ipc\.sock$", path)
            match = state_match or socket_match
            if match is None:
                continue
            home = str(Path(match.group(1)).resolve())
            key = (home, pid)
            record = result.setdefault(key, {"home": home, "pid": pid, "evidence": []})
            evidence = "state" if state_match else "ipc"
            if evidence not in record["evidence"]:
                record["evidence"].append(evidence)
    return sorted(result.values(), key=lambda item: (item["home"], item["pid"]))


class RetryLedger:
    """Durable per-turn dedupe and attempt accounting."""

    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        parent_stat = self.path.parent.stat()
        if parent_stat.st_uid != os.getuid() or parent_stat.st_mode & 0o077:
            raise ValueError("retry ledger directory must be owned by you with mode 0700")
        if self.path.exists() and self.path.is_symlink():
            raise ValueError("retry ledger must not be a symlink")
        self.db = sqlite3.connect(
            self.path, timeout=10, isolation_level=None, check_same_thread=False
        )
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        os.chmod(self.path, 0o600)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS retry_turns (
          home TEXT NOT NULL,
          thread_id TEXT NOT NULL,
          turn_id TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          next_allowed REAL NOT NULL DEFAULT 0,
          unknown INTEGER NOT NULL DEFAULT 0,
          last_result TEXT,
          accepted_turn_id TEXT,
          updated_at REAL NOT NULL,
          PRIMARY KEY(home, thread_id, turn_id)
        );
        CREATE TABLE IF NOT EXISTS retry_events (
          id INTEGER PRIMARY KEY,
          at REAL NOT NULL,
          home TEXT NOT NULL,
          thread_id TEXT NOT NULL,
          turn_id TEXT NOT NULL,
          action TEXT NOT NULL,
          detail TEXT
        );
        """)

    def close(self) -> None:
        with self._lock:
            self.db.close()

    def _row(self, key: Tuple[str, str, str]) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.db.execute(
                "SELECT * FROM retry_turns WHERE home=? AND thread_id=? AND turn_id=?", key
            ).fetchone()

    def eligible(self, key: Tuple[str, str, str], now: float, max_retries: int) -> Tuple[bool, str, int]:
        with self._lock:
            row = self._row(key)
            if row is None:
                return True, "new", 0
            if row["unknown"]:
                return False, "unknown_delivery", int(row["attempts"])
            if row["accepted_turn_id"] or row["last_result"] == "turn accepted":
                return False, "already_submitted", int(row["attempts"])
            if int(row["attempts"]) >= max_retries:
                return False, "max_retries", int(row["attempts"])
            if float(row["next_allowed"]) > now:
                return False, "backoff", int(row["attempts"])
            return True, "retry", int(row["attempts"])

    def record(
        self,
        key: Tuple[str, str, str],
        *,
        action: str,
        detail: str,
        attempts: Optional[int] = None,
        next_allowed: float = 0.0,
        unknown: bool = False,
        accepted_turn_id: Optional[str] = None,
    ) -> None:
        with self._lock:
            home, thread, turn = key
            now = time.time()
            row = self._row(key)
            count = int(row["attempts"]) if row else 0
            if attempts is not None:
                count = attempts
            self.db.execute(
                """INSERT INTO retry_turns
                   (home,thread_id,turn_id,attempts,next_allowed,unknown,last_result,accepted_turn_id,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(home,thread_id,turn_id) DO UPDATE SET
                     attempts=excluded.attempts,next_allowed=excluded.next_allowed,
                     unknown=excluded.unknown,last_result=excluded.last_result,
                     accepted_turn_id=excluded.accepted_turn_id,updated_at=excluded.updated_at""",
                (home, thread, turn, count, next_allowed, int(unknown), detail, accepted_turn_id, now),
            )
            self.db.execute(
                "INSERT INTO retry_events(at,home,thread_id,turn_id,action,detail) VALUES(?,?,?,?,?,?)",
                (now, home, thread, turn, action, detail),
            )


class MonitorLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self) -> "MonitorLock":
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            self.handle = None
            raise BridgeError("another capacity-retry monitor already holds the lock") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(str(os.getpid()))
        self.handle.flush()
        return self

    def __exit__(self, *args: Any) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None


class CapacityRetryMonitor:
    def __init__(
        self,
        *,
        interval: float = 1.5,
        timeout: float = 8.0,
        owner_timeout: float = 0.2,
        max_retries: int = 8,
        retry_base: float = 1.5,
        retry_max_backoff: float = 60.0,
        max_sessions_per_cycle: int = 64,
        lookback_seconds: Optional[float] = 7 * 24 * 3600,
        all_sessions: bool = False,
        dry_run: bool = False,
        ledger_path: Optional[Path] = None,
        thread_ids: Optional[Sequence[str]] = None,
        workers: int = 8,
        verbose_unavailable: bool = False,
        active_priority_seconds: float = 30.0,
    ):
        if interval < 0.2:
            raise ValueError("interval must be >= 0.2 seconds")
        if (timeout <= 0 or owner_timeout <= 0 or max_retries < 1 or retry_base <= 0 or
                retry_max_backoff <= 0 or active_priority_seconds <= 0):
            raise ValueError("invalid retry settings")
        self.interval = interval
        self.timeout = timeout
        self.owner_timeout = owner_timeout
        self.max_retries = max_retries
        self.retry_base = retry_base
        self.retry_max_backoff = retry_max_backoff
        self.max_sessions_per_cycle = max(1, max_sessions_per_cycle)
        self.lookback_seconds = None if all_sessions else lookback_seconds
        self.dry_run = dry_run
        self.thread_ids = list(thread_ids or [])
        self.workers = max(1, min(workers, 32))
        self.verbose_unavailable = verbose_unavailable
        self.active_priority_seconds = active_priority_seconds
        path = ledger_path or Path.home() / ".codex-capacity-retry" / "retry.sqlite"
        self.ledger = RetryLedger(path)
        self.cursor = 0
        self._stop = threading.Event()
        self._live_homes: Dict[str, bool] = {}
        self._home_events: List[Dict[str, Any]] = []
        self._runtime_entries: List[Dict[str, Any]] = []
        self._runtime_signatures: Dict[str, Tuple[Tuple[str, Tuple[str, ...]], ...]] = {}
        self._route_cache: Dict[str, Dict[str, Any]] = {}
        self._dirty_addresses: set[str] = set()
        self._pending_addresses: set[str] = set()
        self._active_until: Dict[str, float] = {}
        self._full_sweep_addresses: set[str] = set()
        self._last_full_sweep_completed = -float("inf")
        self._last_probe: Dict[str, float] = {}
        self._next_probe: Dict[str, float] = {}
        self._probe_failures: Dict[str, int] = {}
        self._owner_seen: set[str] = set()
        self._warning_times: Dict[Tuple[str, str], float] = {}
        self._stream_clients: Dict[str, Client] = {}
        self._stream_generations: Dict[str, Any] = {}
        self._session_lock = threading.Lock()

    def close(self) -> None:
        for key in list(self._stream_clients):
            self._drop_stream(key)
        self.ledger.close()

    def _drop_stream(self, key: str) -> None:
        with self._session_lock:
            client = self._stream_clients.pop(key, None)
            self._stream_generations.pop(key, None)
        if client is not None:
            client.__exit__(None, None, None)

    def stop(self) -> None:
        self._stop.set()

    def _route_batch(self, routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        now = time.monotonic()
        urgent = self._dirty_addresses | self._pending_addresses | set(self._stream_clients) | {
            key for key, until in self._active_until.items() if until > now
        }
        hot = [route for route in routes if route["address"] in urgent]
        cold = [route for route in routes if route["address"] not in urgent]
        # Reserve space for the finite sweep, including with a batch size of 1.
        limit = self.max_sessions_per_cycle
        self.cursor += 1
        reserve = min(len(cold), max(1, limit // 4)) if cold else 0
        if limit == 1 and hot:
            reserve = int(bool(cold) and self.cursor % 4 == 0)
        selected = hot[:limit - reserve]
        selected += cold[:limit - len(selected)]
        # Fill unused background slots when the sweep is almost finished.
        seen = {route["address"] for route in selected}
        selected += [route for route in hot if route["address"] not in seen][:limit - len(selected)]
        return [{**route, "priority_probe": route["address"] in urgent} for route in selected]

    def _refresh_home_health(self) -> None:
        previous = dict(self._live_homes)
        previous_runtime_signatures = dict(self._runtime_signatures)
        current: Dict[str, bool] = {}
        events: List[Dict[str, Any]] = []
        runtime_homes = discover_process_homes()
        runtime_by_home: Dict[str, Dict[str, Any]] = {}
        runtime_signatures: Dict[str, List[Tuple[str, Tuple[str, ...]]]] = {}
        for item in runtime_homes:
            home_key = str(Path(item["home"]).expanduser().resolve())
            normalized = {**item, "home": home_key}
            runtime_by_home.setdefault(home_key, normalized)
            evidence = tuple(sorted(str(value) for value in item.get("evidence", [])))
            runtime_signatures.setdefault(home_key, []).append((str(item["pid"]), evidence))
        self._runtime_entries = list(runtime_by_home.values())
        self._runtime_signatures = {
            home: tuple(sorted(records)) for home, records in runtime_signatures.items()
        }
        for home in self._runtime_signatures:
            try:
                info = (Path(home) / "ipc/ipc.sock").stat()
                self._runtime_signatures[home] += (("socket", (str(info.st_ino), str(info.st_ctime_ns))),)
            except OSError:
                pass
        homes = [
            (f"runtime-{item['pid']}", Path(item["home"]))
            for item in self._runtime_entries
        ]
        for name, home in homes:
            home_text = str(home)
            socket_path = home / "ipc/ipc.sock"
            reachable = False
            detail = None
            if socket_path.is_socket():
                process_changed = previous_runtime_signatures.get(home_text) != self._runtime_signatures.get(home_text)
                if previous.get(home_text) is True and not process_changed:
                    # The process and socket are still live. Avoid opening a
                    # second IPC connection every scan; session probes below
                    # already verify the owner and stream.
                    reachable = True
                else:
                    try:
                        # A home-level handshake is only needed for a new or
                        # previously unavailable home.
                        with Client(home, timeout=min(self.timeout, 0.75)):
                            reachable = True
                    except (BridgeError, OSError, TimeoutError) as exc:
                        detail = str(exc)
            else:
                detail = "IPC socket not present"
            current[home_text] = reachable
            if previous.get(home_text) != reachable:
                if reachable:
                    self._route_cache.pop(home_text, None)
                events.append({
                    "action": "home",
                    "home": home_text,
                    "instance": name,
                    "status": "ready" if reachable else "offline",
                    "detail": detail,
                })
        self._live_homes = current
        self._home_events = events

    @staticmethod
    def _route_fingerprint(route: Dict[str, Any]) -> Tuple[Any, Any, Any]:
        return (route.get("updated"), route.get("title"), route.get("archived"))

    def _refresh_route_cache(self) -> List[Dict[str, Any]]:
        """Cache indexes, but periodically re-read them even without a stat change."""

        now, wall_now = time.monotonic(), time.time()
        active_homes = {str(item["home"]) for item in self._runtime_entries}
        for home in set(self._route_cache) - active_homes:
            del self._route_cache[home]
        wanted = {thread_id(value) for value in self.thread_ids} if self.thread_ids else None
        all_routes: List[Dict[str, Any]] = []
        for item in self._runtime_entries:
            home = Path(item["home"])
            home_text = str(home)
            process_signature = self._runtime_signatures.get(home_text, ())
            state_signature = _state_index_signature(home)
            cached = self._route_cache.get(home_text)
            process_changed = cached is None or cached["process_signature"] != process_signature
            if (cached is None or process_changed or cached["state_signature"] != state_signature
                    or now - cached["read_at"] >= FULL_ROUTE_SWEEP_SECONDS):
                routes = _routes_for_home(
                    f"runtime-{item['pid']}", home, lookback_seconds=None, now=wall_now,
                )
                if routes is not None:
                    old = {route["address"]: route for route in cached["routes"]} if cached else {}
                    # Seed recent sessions first; historical sessions belong to
                    # the background sweep, not the urgent queue.
                    changed = {route["address"] for route in routes[:16]} if process_changed else {
                        route["address"] for route in routes
                        if self._route_fingerprint(route) != self._route_fingerprint(old.get(route["address"], {}))
                    }
                    with self._session_lock:
                        self._dirty_addresses.update(changed)
                        if process_changed:
                            self._full_sweep_addresses.update(route["address"] for route in routes)
                        for key in changed:
                            self._next_probe.pop(key, None)
                    cached = self._route_cache[home_text] = {
                        "state_signature": state_signature,
                        "process_signature": process_signature,
                        "routes": routes,
                        "read_at": now,
                    }
                # A failed read must not replace a good cache or mark the
                # current signature as read: the next cycle should try again.
            if cached is None or not self._live_homes.get(home_text, False):
                continue
            all_routes.extend(
                route for route in cached["routes"]
                if (wanted is None or route["thread_id"] in wanted)
                and (self.lookback_seconds is None or not route["updated"]
                     or route["updated"] >= wall_now - self.lookback_seconds)
            )
        keys = {route["address"] for route in all_routes}
        for key in set(self._stream_clients) - keys:
            self._drop_stream(key)
        with self._session_lock:
            had_sweep = bool(self._full_sweep_addresses)
            for group in (self._dirty_addresses, self._pending_addresses,
                          self._full_sweep_addresses, self._owner_seen):
                group.intersection_update(keys)
            for mapping in (self._active_until, self._next_probe, self._probe_failures, self._last_probe):
                for key in set(mapping) - keys:
                    del mapping[key]
            if had_sweep and not self._full_sweep_addresses:
                self._last_full_sweep_completed = now
            if (not self._full_sweep_addresses
                    and now - self._last_full_sweep_completed >= FULL_ROUTE_SWEEP_SECONDS):
                self._full_sweep_addresses.update(keys)
                if not keys:
                    self._last_full_sweep_completed = now
        return all_routes

    def _candidate_routes(self, routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        now = time.monotonic()
        with self._session_lock:
            active = {key for key, until in self._active_until.items() if until > now}
            urgent = self._dirty_addresses | self._pending_addresses | active | set(self._stream_clients)
            eligible = urgent | self._full_sweep_addresses
            candidates = [route for route in routes
                          if route["address"] in eligible
                          and (route["address"] in self._dirty_addresses
                               or self._next_probe.get(route["address"], 0.0) <= now)]
            # Least-recently probed first within each priority group prevents
            # permanently busy sessions from starving other live sessions.
            candidates.sort(key=lambda route: (
                0 if route["address"] in urgent else 1,
                self._last_probe.get(route["address"], -float("inf")),
                -route["updated"],
            ))
        return candidates

    def _finish_probe(self, key: str) -> None:
        """An attempt completes sweep work even if the session has no owner."""
        with self._session_lock:
            now = time.monotonic()
            self._last_probe[key] = now
            self._dirty_addresses.discard(key)
            was_in_sweep = key in self._full_sweep_addresses
            self._full_sweep_addresses.discard(key)
            if was_in_sweep and not self._full_sweep_addresses:
                self._last_full_sweep_completed = now

    def _inspect(self, route: Dict[str, Any]) -> Dict[str, Any]:
        base = {"home": route["home"], "instance": route["instance_name"],
                "thread_id": route["thread_id"], "address": route["address"]}
        key_address = route["address"]
        stage = "initialize"
        started = time.monotonic()
        with self._session_lock:
            failures = self._probe_failures.get(key_address, 0)
        priority = route.get("priority_probe", False)
        # Cold history uses a short owner lookup. Recent/active sessions get
        # more time, and repeated read failures increase that budget.
        owner_budget = self.owner_timeout
        if priority:
            owner_budget = min(self.timeout, max(owner_budget, 1.5) * 2 ** min(failures, 3))
        client = None
        try:
            generation = self._runtime_signatures.get(route["home"])
            if self._stream_generations.get(key_address) != generation:
                self._drop_stream(key_address)
            client = self._stream_clients.get(key_address)
            if client is None:
                client = Client(route["home"], timeout=max(self.owner_timeout, min(self.timeout, 1.5)))
                client.__enter__()
            # Keep live subscriptions across polling rounds. Requests on this
            # connection consume and apply queued stream patches as well.
            with contextlib.nullcontext(client):
                stage = "owner"
                client.timeout = owner_budget
                owner = client.owner(route["thread_id"])
                with self._session_lock:
                    self._owner_seen.add(key_address)
                stage = "snapshot"
                client.timeout = self.timeout
                state = client.snapshot(route["thread_id"], owner)
                with self._session_lock:
                    # Bound descriptors. Overflow sessions still use one-shot
                    # probes; the normal live-session set is much smaller.
                    if key_address in self._stream_clients or len(self._stream_clients) < 64:
                        self._stream_clients[key_address] = client
                        self._stream_generations[key_address] = generation
                failure = find_capacity_failure(state)
                busy = is_busy(state)
                with self._session_lock:
                    self._probe_failures.pop(key_address, None)
                    self._next_probe.pop(key_address, None)
                    if busy:
                        self._active_until[key_address] = time.monotonic() + self.active_priority_seconds
                    else:
                        # A short grace period also catches an idle session
                        # starting another turn without an index update.
                        self._active_until.setdefault(key_address, time.monotonic() + self.active_priority_seconds)
                    if failure is None:
                        self._pending_addresses.discard(key_address)
                    else:
                        self._pending_addresses.add(key_address)
                if failure is None:
                    return {**base, "action": "observe", "status": "ok"}
                key = (route["home"], route["thread_id"], failure["turn_id"])
                eligible, reason, previous_attempts = self.ledger.eligible(key, time.time(), self.max_retries)
                event: Dict[str, Any] = {
                    **base, "action": "candidate", "status": "failed",
                    "turn_id": failure["turn_id"], "error": CAPACITY_ERROR,
                    "model": failure.get("model"), "attempts": previous_attempts,
                    "eligible": eligible, "reason": reason,
                }
                if not eligible or self.dry_run:
                    with self._session_lock:
                        if reason in ("max_retries", "unknown_delivery", "already_submitted"):
                            self._pending_addresses.discard(key_address)
                            self._active_until.pop(key_address, None)
                        elif reason == "backoff":
                            row = self.ledger._row(key)
                            self._next_probe[key_address] = time.monotonic() + max(
                                self.interval, float(row["next_allowed"]) - time.time(),
                            )
                    if self.dry_run and eligible:
                        event["action"] = "would_retry"
                    return event
                stage = "submit"
                try:
                    result = send_capacity_retry(client, route["thread_id"], owner, failure.get("collaboration_mode"))
                except DeliveryUnknown as exc:
                    self.ledger.record(key, action="unknown", detail=str(exc), unknown=True)
                    self._drop_stream(key_address)
                    with self._session_lock:
                        self._pending_addresses.discard(key_address)
                        self._active_until.pop(key_address, None)
                    return {**event, "action": "unknown", "status": "unknown_delivery", "detail": str(exc)}
                except Unavailable:
                    # An explicit no-owner response means no turn was sent.
                    raise
                except (ProtocolError, Rejected) as exc:
                    delay = min(self.retry_max_backoff, self.retry_base * (2 ** previous_attempts))
                    self.ledger.record(key, action="rejected", detail=str(exc),
                                       attempts=previous_attempts + 1, next_allowed=time.time() + delay)
                    with self._session_lock:
                        self._next_probe[key_address] = time.monotonic() + delay
                    return {**event, "action": "rejected", "status": "rejected", "detail": str(exc)}
                accepted_turn = (result.get("turn") or {}).get("id") if isinstance(result, dict) else None
                delay = min(self.retry_max_backoff, self.retry_base * (2 ** previous_attempts))
                self.ledger.record(key, action="accepted", detail="turn accepted",
                                   attempts=previous_attempts + 1, next_allowed=time.time() + delay,
                                   accepted_turn_id=accepted_turn)
                with self._session_lock:
                    self._next_probe[key_address] = time.monotonic() + delay
                return {**event, "action": "retry", "status": "accepted", "accepted_turn_id": accepted_turn}
        except (Unavailable, ProtocolError, Rejected, OSError, TimeoutError) as exc:
            self._drop_stream(key_address)
            with self._session_lock:
                count = self._probe_failures.get(key_address, 0) + 1
                self._probe_failures[key_address] = count
                delay = min(FULL_ROUTE_SWEEP_SECONDS, self.interval * 2 ** min(count - 1, 4))
                self._next_probe[key_address] = time.monotonic() + delay
                known_owner = key_address in self._owner_seen
                if (str(exc) != "no-client-found"
                        and (known_owner or (priority and count < 2))):
                    self._active_until[key_address] = time.monotonic() + self.active_priority_seconds
                elif not known_owner:
                    self._active_until.pop(key_address, None)
            return {**base, "action": "observe", "status": "unavailable", "stage": stage,
                    "detail": str(exc), "elapsed_seconds": round(time.monotonic() - started, 3),
                    "warning": stage in ("snapshot", "submit") or isinstance(exc, ProtocolError)
                               or ((known_owner or bool(self.thread_ids)) and count >= 2
                                   and str(exc) != "no-client-found")}
        finally:
            if client is not None and self._stream_clients.get(key_address) is not client:
                client.__exit__(None, None, None)
            self._finish_probe(key_address)

    def scan_once(self) -> List[Dict[str, Any]]:
        self._refresh_home_health()
        routes = self._refresh_route_cache()
        routes = self._candidate_routes(routes)
        batch = self._route_batch(routes)
        events: List[Dict[str, Any]] = list(self._home_events)
        self._home_events = []
        if not batch:
            return events
        with ThreadPoolExecutor(max_workers=min(self.workers, len(batch))) as pool:
            futures = {pool.submit(self._inspect, route): route for route in batch}
            for future in as_completed(futures):
                try:
                    event = future.result()
                except Exception as exc:
                    route = futures[future]
                    event = {"action": "observe", "status": "worker_error", "detail": str(exc),
                             "address": route["address"], "thread_id": route["thread_id"]}
                ordinary_probe = event.get("action") == "observe" and event.get("status") in ("ok", "unavailable")
                if event.get("warning") and not self.verbose_unavailable:
                    key = (event["address"], event.get("stage", ""))
                    now = time.monotonic()
                    if now - self._warning_times.get(key, -float("inf")) >= 30:
                        events.append(event)
                        self._warning_times[key] = now
                elif not ordinary_probe or (self.verbose_unavailable and event.get("status") == "unavailable"):
                    events.append(event)
        events.sort(key=lambda item: (item.get("home", ""), item.get("thread_id", "")))
        return events

    def run(
        self,
        *,
        duration: Optional[float] = None,
        emit: Any = None,
        json_output: bool = False,
    ) -> None:
        if emit is None:
            if json_output:
                emit = lambda value: print(json.dumps(value, ensure_ascii=False, sort_keys=True), flush=True)
            else:
                emit = lambda value: print(format_event(value), flush=True)
        started = time.monotonic()
        try:
            while not self._stop.is_set():
                for event in self.scan_once():
                    emit(event)
                if duration is not None and time.monotonic() - started >= duration:
                    break
                self._stop.wait(self.interval)
        finally:
            self.close()


def _short(value: Any, length: int = 12) -> str:
    text = str(value or "-")
    return text if len(text) <= length else text[:length]


def _one_line(value: Any) -> str:
    return " ".join(str(value or "-").split())


def format_event(event: Dict[str, Any]) -> str:
    """Render a compact operator-facing line; JSON remains available via --json."""

    stamp = datetime.now().astimezone().strftime("%H:%M:%S")
    action = event.get("action")
    instance = event.get("instance", "-")
    thread = _short(event.get("thread_id"))
    if action == "process_report":
        processes = event.get("processes", [])
        pids = ",".join(str(item.get("pid")) for item in processes) or "none"
        homes = ",".join(sorted({str(item.get("home")) for item in event.get("runtime_homes", [])})) or "none"
        return f"{stamp} INFO  app-server pids={pids} runtime_homes={homes}"
    if action == "home":
        status = event.get("status")
        if status == "ready":
            return f"{stamp} READY home={instance} path={event.get('home')}"
        return f"{stamp} WARN  home={instance} status={status} detail={_one_line(event.get('detail'))}"
    if action in ("retry", "would_retry"):
        verb = "DRY-RUN" if action == "would_retry" else "RETRY"
        return (f"{stamp} {verb:<7} instance={instance} thread={thread} "
                f"attempt={event.get('attempts', 0) + 1} model={event.get('model') or '-'} "
                f"turn={_short(event.get('turn_id'))} status={event.get('status')}")
    if action == "unknown":
        return f"{stamp} WARN  unknown-delivery instance={instance} thread={thread} detail={_one_line(event.get('detail'))}"
    if action == "rejected":
        return f"{stamp} WARN  retry-rejected instance={instance} thread={thread} detail={_one_line(event.get('detail'))}"
    if event.get("status") == "worker_error":
        return f"{stamp} ERROR worker detail={_one_line(event.get('detail'))}"
    if event.get("status") == "unavailable":
        return (f"{stamp} WARN  instance={instance} thread={event.get('thread_id')} "
                f"stage={event.get('stage', '-')} elapsed={event.get('elapsed_seconds', '-')}s "
                f"detail={_one_line(event.get('detail'))}")
    return f"{stamp} INFO  {action or 'event'} instance={instance} thread={thread} status={_one_line(event.get('status'))}"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Retry local Codex turns that fail with the model-capacity error"
    )
    parser.add_argument("--interval", type=float, default=1.5)
    parser.add_argument("--duration", type=float, default=None,
                        help="run for this many seconds; default is until interrupted")
    parser.add_argument("--timeout", type=float, default=8.0,
                        help="snapshot and retry IPC timeout in seconds (default: 8)")
    parser.add_argument("--owner-timeout", type=float, default=0.2,
                        help="cold-session owner lookup timeout; live sessions get more time (default: 0.2)")
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--retry-base", type=float, default=1.5)
    parser.add_argument("--retry-max-backoff", type=float, default=60.0)
    parser.add_argument("--max-sessions-per-cycle", type=int, default=64)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--lookback-seconds", type=float, default=7 * 24 * 3600)
    parser.add_argument("--all-sessions", action="store_true",
                        help="include older non-archived sessions; scan round-robin when large")
    parser.add_argument("--thread", action="append", dest="thread_ids",
                        help="limit monitoring to this thread UUID; repeatable")
    parser.add_argument("--dry-run", action="store_true",
                        help="detect and print candidates without sending retries")
    parser.add_argument("--ledger", type=Path,
                        help="retry ledger path (default ~/.codex-capacity-retry/retry.sqlite)")
    parser.add_argument("--lock", type=Path,
                        help="lock path (default next to the ledger)")
    parser.add_argument("--process-report", action="store_true",
                        help="print the local Codex app-server PID inventory once at startup")
    parser.add_argument("--active-priority-seconds", type=float, default=30.0,
                        help="keep successfully observed sessions at the front of the scan queue")
    parser.add_argument("--verbose-unavailable", action="store_true",
                        help="emit per-thread timeout/offline diagnostics")
    parser.add_argument("--json", action="store_true",
                        help="emit machine-readable JSON lines instead of operator log lines")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    ledger = (args.ledger or Path.home() / ".codex-capacity-retry" / "retry.sqlite").expanduser()
    lock_path = (args.lock or ledger.with_suffix(".lock")).expanduser()
    try:
        with MonitorLock(lock_path):
            if args.process_report:
                process_event = {
                    "action": "process_report",
                    "processes": discover_codex_processes(),
                    "runtime_homes": discover_process_homes(),
                }
                if args.json:
                    print(json.dumps(process_event, ensure_ascii=False, sort_keys=True), flush=True)
                else:
                    print(format_event(process_event), flush=True)
            monitor = CapacityRetryMonitor(
                interval=args.interval, timeout=args.timeout,
                owner_timeout=args.owner_timeout,
                max_retries=args.max_retries, retry_base=args.retry_base,
                retry_max_backoff=args.retry_max_backoff,
                max_sessions_per_cycle=args.max_sessions_per_cycle,
                lookback_seconds=args.lookback_seconds,
                all_sessions=args.all_sessions, dry_run=args.dry_run,
                ledger_path=ledger, thread_ids=args.thread_ids, workers=args.workers,
                verbose_unavailable=args.verbose_unavailable,
                active_priority_seconds=args.active_priority_seconds,
            )
            monitor.run(duration=args.duration, json_output=args.json)
        return 0
    except KeyboardInterrupt:
        return 130
    except (BridgeError, OSError, ValueError, sqlite3.Error) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
