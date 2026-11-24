from datetime import datetime, timezone
import os
import threading
from typing import Dict, List, Optional, Tuple

from playlist import (
    LogEntry,
    OperationType,
    build_playlist_from_entries,
    sorted_playlist_songs,
    validate_song,
    validate_student_id,
)


class NodeStorage:
    def __init__(self, node_id: int, base_dir: str = ".") -> None:
        validate_student_id(node_id)
        self.node_id = node_id
        self.base_dir = base_dir
        self._lock = threading.Lock()

        os.makedirs(self.base_dir, exist_ok=True)
        self.playlist_path = os.path.join(self.base_dir, f"playlist_{node_id}.txt")
        self.changes_path = os.path.join(self.base_dir, f"changes_{node_id}.log")
        self.messages_path = os.path.join(self.base_dir, f"messages_{node_id}.log")

    def initialize_files(self) -> None:
        with self._lock:
            if not os.path.exists(self.playlist_path):
                self._write_text_atomic(self.playlist_path, "")
            if not os.path.exists(self.changes_path):
                self._write_text_atomic(self.changes_path, "")
            if not os.path.exists(self.messages_path):
                with open(self.messages_path, "a", encoding="utf-8"):
                    pass

    def sync_log_and_playlist(self, committed_log: Dict[int, Tuple[OperationType, int]]) -> List[int]:
        entries: List[LogEntry] = [
            (idx, op, song) for idx, (op, song) in sorted(committed_log.items(), key=lambda item: item[0])
        ]
        playlist_set = build_playlist_from_entries(entries)
        ordered_songs = sorted_playlist_songs(playlist_set)

        changes_lines = [f"{idx} {op.value} {song}" for idx, op, song in entries]
        changes_content = ("\n".join(changes_lines) + "\n") if changes_lines else ""

        playlist_lines = [str(song) for song in ordered_songs]
        playlist_content = ("\n".join(playlist_lines) + "\n") if playlist_lines else ""

        with self._lock:
            self._write_text_atomic(self.changes_path, changes_content)
            self._write_text_atomic(self.playlist_path, playlist_content)

        return ordered_songs

    def load_changes_log(self) -> Dict[int, Tuple[OperationType, int]]:
        with self._lock:
            if not os.path.exists(self.changes_path):
                return {}
            with open(self.changes_path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()

        committed_log: Dict[int, Tuple[OperationType, int]] = {}
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split()
            if len(parts) != 3:
                raise ValueError(f"Malformed line in {self.changes_path}: {line!r}")
            idx = int(parts[0])
            op = OperationType(parts[1])
            song = int(parts[2])
            validate_song(song)
            committed_log[idx] = (op, song)

        return committed_log

    def append_message_log(
        self,
        direction: str,
        peer: Optional[str],
        payload: str,
    ) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        peer_label = peer if peer is not None else "-"
        line = f"{timestamp} | {direction} | {peer_label} | {payload}\n"
        with self._lock:
            with open(self.messages_path, "a", encoding="utf-8") as f:
                f.write(line)

    @staticmethod
    def _write_text_atomic(path: str, content: str) -> None:
        parent = os.path.dirname(os.path.abspath(path))
        tmp_path = f"{path}.tmp.{threading.get_ident()}"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)