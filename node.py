import argparse
import json
import random
import signal
import socket
import sys
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

from paxos import PaxosEngine
from playlist import (
    OperationType,
    build_playlist_from_log,
    can_student_propose_now,
    validate_song,
    validate_student_id,
)
from storage import NodeStorage

DEFAULT_HOST = "127.0.0.1"
BASE_PORT = 17100
MIN_DELAY_SEC = 3.0
MAX_DELAY_SEC = 6.0


def node_port(node_id: int, base_port: int = BASE_PORT) -> int:
    validate_student_id(node_id)
    return base_port + node_id


def send_json_frame(sock: socket.socket, payload: Dict[str, Any]) -> None:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    header = len(raw).to_bytes(4, byteorder="big")
    sock.sendall(header + raw)


def recv_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
    chunks = bytearray()
    while len(chunks) < num_bytes:
        chunk = sock.recv(num_bytes - len(chunks))
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)


def recv_json_frame(sock: socket.socket) -> Optional[Dict[str, Any]]:
    header = recv_exact(sock, 4)
    if header is None:
        return None
    msg_len = int.from_bytes(header, byteorder="big")
    if msg_len <= 0 or msg_len > 10 * 1024 * 1024:
        raise ValueError(f"Invalid frame length: {msg_len}")
    body = recv_exact(sock, msg_len)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


class StudentNode:
    def __init__(
        self,
        node_id: int,
        host: str = DEFAULT_HOST,
        base_port: int = BASE_PORT,
        base_dir: str = ".",
        min_delay: float = MIN_DELAY_SEC,
        max_delay: float = MAX_DELAY_SEC,
        message_handler: Optional[Callable[["StudentNode", Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
    ) -> None:
        validate_student_id(node_id)
        self.node_id = node_id
        self.host = host
        self.base_port = base_port
        self.port = node_port(node_id, base_port)
        self.min_delay = min_delay
        self.max_delay = max_delay

        self.storage = NodeStorage(node_id=node_id, base_dir=base_dir)
        self.state_lock = threading.Lock()
        self.paused = False
        self.running = False
        self.committed_log: Dict[int, Tuple[OperationType, int]] = {}
        self._server_sock: Optional[socket.socket] = None

        self.paxos = PaxosEngine(
            node_id=self.node_id,
            storage=self.storage,
            state_lock=self.state_lock,
            committed_log=self.committed_log,
            send_peer=self.send_peer_message,
            broadcast_peer=self.broadcast_peer_message,
            sync_callback=self.storage.sync_log_and_playlist,
            is_paused_fn=self.is_paused,
        )
        self.message_handler = message_handler or (lambda _node, msg: self.paxos.handle_message(msg))

    def start(self) -> None:
        self.storage.initialize_files()
        loaded_log = self.storage.load_changes_log()
        with self.state_lock:
            self.committed_log.clear()
            self.committed_log.update(loaded_log)
            self.storage.sync_log_and_playlist(dict(self.committed_log))

        self.running = True
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(32)
        server_sock.settimeout(0.5)
        self._server_sock = server_sock

        self.paxos.start_background_tasks()
        self.storage.append_message_log("SYSTEM", f"node-{self.node_id}", f"STARTED on {self.host}:{self.port}")

        while self.running:
            try:
                conn, addr = server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            t = threading.Thread(target=self._handle_connection, args=(conn, addr), daemon=True)
            t.start()

        self.stop()

    def stop(self) -> None:
        self.running = False
        self.paxos.stop_background_tasks()
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None

    def is_paused(self) -> bool:
        with self.state_lock:
            return self.paused

    def set_paused(self, paused: bool) -> None:
        with self.state_lock:
            self.paused = paused
        state_label = "PAUSED" if paused else "RESUMED"
        self.storage.append_message_log("SYSTEM", f"node-{self.node_id}", state_label)
        if not paused:
            self.paxos.request_catchup_from_peers()

    def send_peer_message(
        self,
        target_id: int,
        message: Dict[str, Any],
        wait_for_reply: bool = False,
        timeout: float = 15.0,
    ) -> Optional[Dict[str, Any]]:
        validate_student_id(target_id)
        if self.is_paused():
            self.storage.append_message_log(
                "DROPPED_PAUSED",
                f"node-{target_id}",
                json.dumps(message, separators=(",", ":")),
            )
            return None

        delay = random.uniform(self.min_delay, self.max_delay) if self.max_delay > 0 else 0.0
        if delay > 0:
            time.sleep(delay)

        if self.is_paused():
            self.storage.append_message_log(
                "DROPPED_PAUSED_AFTER_DELAY",
                f"node-{target_id}",
                json.dumps(message, separators=(",", ":")),
            )
            return None

        payload = dict(message)
        payload.setdefault("from", self.node_id)
        payload.setdefault("to", target_id)

        target_port = node_port(target_id, self.base_port)
        try:
            with socket.create_connection((self.host, target_port), timeout=timeout) as sock:
                sock.settimeout(timeout)
                send_json_frame(sock, payload)
                self.storage.append_message_log(
                    "SENT",
                    f"node-{target_id}",
                    json.dumps(payload, separators=(",", ":")),
                )
                if wait_for_reply:
                    reply = recv_json_frame(sock)
                    if reply is not None:
                        self.storage.append_message_log(
                            "RECV_REPLY",
                            f"node-{target_id}",
                            json.dumps(reply, separators=(",", ":")),
                        )
                    return reply
        except OSError as exc:
            self.storage.append_message_log(
                "SEND_ERROR",
                f"node-{target_id}",
                f"{json.dumps(payload, separators=(',', ':'))} | error={exc}",
            )
        return None

    def broadcast_peer_message(self, message: Dict[str, Any]) -> None:
        for peer_id in range(1, 6):
            if peer_id == self.node_id:
                continue
            threading.Thread(
                target=self.send_peer_message,
                args=(peer_id, message, False),
                daemon=True,
            ).start()

    def _handle_connection(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        with conn:
            try:
                msg = recv_json_frame(conn)
            except Exception as exc:
                self.storage.append_message_log("RECV_ERROR", f"{addr[0]}:{addr[1]}", str(exc))
                return

            if msg is None:
                return

            msg_type = msg.get("type")
            is_local_api = msg_type in {"PAUSE", "RESUME", "PROPOSE_SONG", "PING"}

            if not is_local_api and self.is_paused():
                self.storage.append_message_log(
                    "IGNORED_PAUSED",
                    f"node-{msg.get('from', '?')}",
                    json.dumps(msg, separators=(",", ":")),
                )
                return

            if not is_local_api and self.max_delay > 0:
                delay = random.uniform(self.min_delay, self.max_delay)
                time.sleep(delay)
                if self.is_paused():
                    self.storage.append_message_log(
                        "IGNORED_PAUSED_AFTER_DELAY",
                        f"node-{msg.get('from', '?')}",
                        json.dumps(msg, separators=(",", ":")),
                    )
                    return

            sender_label = f"node-{msg['from']}" if "from" in msg else f"cli@{addr[0]}:{addr[1]}"
            self.storage.append_message_log("RECV", sender_label, json.dumps(msg, separators=(",", ":")))

            reply = self._dispatch_message(msg)
            if reply is not None:
                try:
                    send_json_frame(conn, reply)
                    self.storage.append_message_log(
                        "SENT_REPLY",
                        sender_label,
                        json.dumps(reply, separators=(",", ":")),
                    )
                except OSError:
                    pass

    def _dispatch_message(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        msg_type = msg.get("type")

        if msg_type == "PING":
            return {"ok": True, "node_id": self.node_id, "paused": self.is_paused()}

        if msg_type == "PAUSE":
            self.set_paused(True)
            return {"ok": True, "node_id": self.node_id, "paused": True}

        if msg_type == "RESUME":
            self.set_paused(False)
            return {"ok": True, "node_id": self.node_id, "paused": False}

        if msg_type == "PROPOSE_SONG":
            return self._handle_local_propose(msg)

        if self.message_handler is not None:
            return self.message_handler(self, msg)

        return {"ok": True, "node_id": self.node_id, "received_type": msg_type}

    def _handle_local_propose(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        try:
            k = int(msg["k"])
            song = int(msg["song"])
            op = OperationType(str(msg["op"]).upper())
            validate_song(song)
        except Exception as exc:
            return {"ok": False, "error": f"Invalid proposal arguments: {exc}"}

        if k != self.node_id:
            return {"ok": False, "error": f"Target node {self.node_id} cannot serve proposal for k={k}"}

        with self.state_lock:
            current_playlist = build_playlist_from_log(self.committed_log)
            allowed, reason = can_student_propose_now(k, song, op, current_playlist)

        if not allowed:
            return {"ok": False, "error": reason}

        return self.paxos.run_proposal(op, song)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CS 171 Paxos Playlist Student Node")
    parser.add_argument("--id", type=int, required=True, help="Student ID (1..5)")
    parser.add_argument("--host", type=str, default=DEFAULT_HOST, help="Bind host")
    parser.add_argument("--base-port", type=int, default=BASE_PORT, help="Base TCP port")
    parser.add_argument("--dir", type=str, default=".", help="Output directory for node files")
    parser.add_argument("--min-delay", type=float, default=MIN_DELAY_SEC, help="Minimum network delay (sec)")
    parser.add_argument("--max-delay", type=float, default=MAX_DELAY_SEC, help="Maximum network delay (sec)")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    node = StudentNode(
        node_id=args.id,
        host=args.host,
        base_port=args.base_port,
        base_dir=args.dir,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )

    def _shutdown_handler(signum: int, frame: Any) -> None:
        node.running = False

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    try:
        node.start()
    except Exception as exc:
        print(f"Node {args.id} failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())