import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from playlist import OperationType, vote_for_operation
from storage import NodeStorage

Ballot = Tuple[int, int]  # (counter, node_id)
ProposalResult = Dict[str, Any]
SendFn = Callable[[int, Dict[str, Any], bool, float], Optional[Dict[str, Any]]]
BroadcastFn = Callable[[Dict[str, Any]], None]
SyncFn = Callable[[Dict[int, Tuple[OperationType, int]]], None]
PausedFn = Callable[[], bool]


class PaxosEngine:
    TOTAL_NODES = 5
    PROMISE_QUORUM = 2   # > 1/3 of 5
    ACCEPT_QUORUM = 4    # > 2/3 of 5
    YES_VOTE_QUORUM = 3  # >= 3 YES votes (including self)
    NO_VOTE_LIMIT = 3    # >= 3 NO votes aborts
    VOTE_TIMEOUT_SEC = 20.0
    GOSSIP_INTERVAL_SEC = 4.0

    def __init__(
        self,
        node_id: int,
        storage: NodeStorage,
        state_lock: threading.Lock,
        committed_log: Dict[int, Tuple[OperationType, int]],
        send_peer: SendFn,
        broadcast_peer: BroadcastFn,
        sync_callback: SyncFn,
        is_paused_fn: Optional[PausedFn] = None,
    ) -> None:
        self.node_id = node_id
        self.storage = storage
        self.state_lock = state_lock
        self.committed_log = committed_log
        self.send_peer = send_peer
        self.broadcast_peer = broadcast_peer
        self.sync_callback = sync_callback
        self.is_paused_fn = is_paused_fn or (lambda: False)

        # Per-log_index Paxos acceptor/proposer state
        self.highest_seen_ballot: Dict[int, Ballot] = {}
        self.promised_ballot: Dict[int, Ballot] = {}
        self.accepted_ballot: Dict[int, Ballot] = {}
        self.accepted_op: Dict[int, Tuple[OperationType, int]] = {}
        self._ballot_counter: Dict[int, int] = {}

        # Active voting sessions keyed by (log_index, ballot)
        self._vote_cond = threading.Condition(self.state_lock)
        self._votes: Dict[Tuple[int, Ballot], Dict[int, bool]] = {}

        # Background catch-up gossip control
        self._stop_event = threading.Event()
        self._gossip_thread: Optional[threading.Thread] = None

    def start_background_tasks(self) -> None:
        if self._gossip_thread is not None and self._gossip_thread.is_alive():
            return
        self._stop_event.clear()
        self._gossip_thread = threading.Thread(target=self._gossip_loop, daemon=True)
        self._gossip_thread.start()

    def stop_background_tasks(self) -> None:
        self._stop_event.set()

    def _gossip_loop(self) -> None:
        while not self._stop_event.wait(self.GOSSIP_INTERVAL_SEC):
            if self.is_paused_fn():
                continue
            commit_len = self.contiguous_commit_length()
            self.broadcast_peer(
                {
                    "type": "GOSSIP_STATE",
                    "commit_length": commit_len,
                }
            )

    def contiguous_commit_length_locked(self) -> int:
        idx = 0
        while idx in self.committed_log:
            idx += 1
        return idx

    def contiguous_commit_length(self) -> int:
        with self.state_lock:
            return self.contiguous_commit_length_locked()

    def smallest_undecided_index(self) -> int:
        with self.state_lock:
            return self.contiguous_commit_length_locked()

    def _next_ballot(self, log_index: int) -> Ballot:
        with self.state_lock:
            prev_counter = self._ballot_counter.get(log_index, 0)
            highest_seen_counter = self.highest_seen_ballot.get(log_index, (0, 0))[0]
            promised_counter = self.promised_ballot.get(log_index, (0, 0))[0]
            accepted_counter = self.accepted_ballot.get(log_index, (0, 0))[0]

            next_c = max(prev_counter, highest_seen_counter, promised_counter, accepted_counter) + 1
            self._ballot_counter[log_index] = next_c
            ballot = (next_c, self.node_id)
            self._record_seen_ballot_locked(log_index, ballot)
            return ballot

    def _record_seen_ballot_locked(self, log_index: int, ballot: Ballot) -> None:
        cur = self.highest_seen_ballot.get(log_index)
        if cur is None or ballot > cur:
            self.highest_seen_ballot[log_index] = ballot

    @staticmethod
    def _serialize_ballot(ballot: Optional[Ballot]) -> Optional[List[int]]:
        if ballot is None:
            return None
        return [ballot[0], ballot[1]]

    @staticmethod
    def _deserialize_ballot(raw: Any) -> Optional[Ballot]:
        if raw is None:
            return None
        if isinstance(raw, (list, tuple)) and len(raw) == 2:
            return (int(raw[0]), int(raw[1]))
        raise ValueError(f"Invalid ballot representation: {raw!r}")

    @staticmethod
    def _serialize_op(op_pair: Optional[Tuple[OperationType, int]]) -> Optional[Dict[str, Any]]:
        if op_pair is None:
            return None
        op, song = op_pair
        return {"op": op.value, "song": int(song)}

    @staticmethod
    def _deserialize_op(raw: Any) -> Optional[Tuple[OperationType, int]]:
        if raw is None:
            return None
        if isinstance(raw, dict) and "op" in raw and "song" in raw:
            return (OperationType(str(raw["op"]).upper()), int(raw["song"]))
        raise ValueError(f"Invalid op representation: {raw!r}")

    def run_proposal(self, op: OperationType, song: int) -> ProposalResult:
        if self.is_paused_fn():
            return {
                "ok": False,
                "committed": False,
                "reason": f"Node {self.node_id} is currently paused",
            }

        # Sync any known higher log entries before choosing smallest undecided index
        self.request_catchup_from_peers()

        log_index = self.smallest_undecided_index()
        with self.state_lock:
            if log_index in self.committed_log:
                return {
                    "ok": False,
                    "committed": False,
                    "reason": f"Log index {log_index} already decided",
                    "log_index": log_index,
                }

        ballot = self._next_ballot(log_index)
        proposed_op: Tuple[OperationType, int] = (op, int(song))

        # Phase 1a: PREPARE
        prepare_msg = {
            "type": "PREPARE",
            "ballot": self._serialize_ballot(ballot),
            "log_index": log_index,
        }

        promises: Dict[int, Dict[str, Any]] = {}
        with self.state_lock:
            if log_index in self.committed_log:
                return {
                    "ok": False,
                    "committed": False,
                    "reason": f"Log index {log_index} already committed before PREPARE",
                    "log_index": log_index,
                }
            cur_promised = self.promised_ballot.get(log_index)
            if cur_promised is None or ballot > cur_promised:
                self.promised_ballot[log_index] = ballot
                promises[self.node_id] = {
                    "accept_num": self._serialize_ballot(self.accepted_ballot.get(log_index)),
                    "accept_op": self._serialize_op(self.accepted_op.get(log_index)),
                }

        replies = self._broadcast_and_collect(prepare_msg, timeout=18.0)
        for peer_id, reply in replies.items():
            if reply and reply.get("type") == "PROMISE" and reply.get("granted"):
                promises[peer_id] = reply

        if len(promises) < self.PROMISE_QUORUM:
            return {
                "ok": False,
                "committed": False,
                "reason": f"Insufficient PROMISE quorum ({len(promises)}/{self.PROMISE_QUORUM})",
                "log_index": log_index,
                "ballot": ballot,
            }

        # Check if any promise carries an already-accepted operation
        highest_accept_num: Optional[Ballot] = None
        adopted_op: Optional[Tuple[OperationType, int]] = None
        for p in promises.values():
            acc_num = self._deserialize_ballot(p.get("accept_num"))
            acc_op = self._deserialize_op(p.get("accept_op"))
            if acc_num is not None and acc_op is not None:
                if highest_accept_num is None or acc_num > highest_accept_num:
                    highest_accept_num = acc_num
                    adopted_op = acc_op

        if adopted_op is not None:
            chosen_op = adopted_op
        else:
            vote_passed, vote_reason = self._collect_votes(log_index, ballot, proposed_op)
            if not vote_passed:
                return {
                    "ok": False,
                    "committed": False,
                    "reason": vote_reason,
                    "log_index": log_index,
                    "ballot": ballot,
                    "op": proposed_op[0].value,
                    "song": proposed_op[1],
                }
            chosen_op = proposed_op

        if self.is_paused_fn():
            return {
                "ok": False,
                "committed": False,
                "reason": f"Proposer node {self.node_id} paused before ACCEPT",
                "log_index": log_index,
                "ballot": ballot,
            }

        # Phase 2a: ACCEPT
        with self.state_lock:
            if log_index in self.committed_log:
                decided_op, decided_song = self.committed_log[log_index]
                return {
                    "ok": False,
                    "committed": False,
                    "reason": f"Log index {log_index} already decided as {decided_op.value} {decided_song}",
                    "log_index": log_index,
                }

        accept_msg = {
            "type": "ACCEPT",
            "ballot": self._serialize_ballot(ballot),
            "log_index": log_index,
            "op": chosen_op[0].value,
            "song": chosen_op[1],
        }

        accepted_by: Dict[int, bool] = {}
        with self.state_lock:
            cur_promised = self.promised_ballot.get(log_index)
            if cur_promised is None or ballot >= cur_promised:
                self.promised_ballot[log_index] = ballot
                self.accepted_ballot[log_index] = ballot
                self.accepted_op[log_index] = chosen_op
                accepted_by[self.node_id] = True

        accept_replies = self._broadcast_and_collect(accept_msg, timeout=18.0)
        for peer_id, reply in accept_replies.items():
            if reply and reply.get("type") == "ACCEPTED" and reply.get("accepted"):
                accepted_by[peer_id] = True

        if len(accepted_by) < self.ACCEPT_QUORUM:
            return {
                "ok": False,
                "committed": False,
                "reason": f"Insufficient ACCEPTED quorum ({len(accepted_by)}/{self.ACCEPT_QUORUM})",
                "log_index": log_index,
                "ballot": ballot,
            }

        # Phase 2b: DECIDE & commit locally
        decide_msg = {
            "type": "DECIDE",
            "ballot": self._serialize_ballot(ballot),
            "log_index": log_index,
            "op": chosen_op[0].value,
            "song": chosen_op[1],
        }
        committed_now = self._commit_entry(log_index, chosen_op[0], chosen_op[1])
        self.broadcast_peer(decide_msg)

        if not committed_now:
            with self.state_lock:
                decided_op, decided_song = self.committed_log[log_index]
            return {
                "ok": False,
                "committed": False,
                "reason": f"Lost race for log_index {log_index}; already {decided_op.value} {decided_song}",
                "log_index": log_index,
            }

        return {
            "ok": True,
            "committed": True,
            "log_index": log_index,
            "ballot": ballot,
            "op": chosen_op[0].value,
            "song": chosen_op[1],
        }

    def request_catchup_from_peers(self, preferred_peer: Optional[int] = None) -> None:
        if self.is_paused_fn():
            return
        start_idx = self.contiguous_commit_length()
        req = {
            "type": "SYNC_REQUEST",
            "from_index": start_idx,
        }
        peers = [preferred_peer] if preferred_peer is not None else [
            pid for pid in range(1, self.TOTAL_NODES + 1) if pid != self.node_id
        ]
        for peer_id in peers:
            if peer_id == self.node_id or peer_id not in range(1, self.TOTAL_NODES + 1):
                continue
            threading.Thread(
                target=self._fetch_from_peer,
                args=(peer_id, req),
                daemon=True,
            ).start()

    def _fetch_from_peer(self, peer_id: int, req: Dict[str, Any]) -> None:
        reply = self.send_peer(peer_id, req, True, 18.0)
        if reply and reply.get("type") == "SYNC_RESPONSE":
            entries = reply.get("entries", [])
            self._apply_sync_entries(entries)

    def _apply_sync_entries(self, entries: List[Dict[str, Any]]) -> None:
        if not entries:
            return
        parsed: List[Tuple[int, OperationType, int]] = []
        for item in entries:
            idx = int(item["index"])
            op = OperationType(str(item["op"]).upper())
            song = int(item["song"])
            parsed.append((idx, op, song))

        parsed.sort(key=lambda x: x[0])
        with self.state_lock:
            changed = False
            for idx, op, song in parsed:
                if idx not in self.committed_log:
                    self.committed_log[idx] = (op, song)
                    changed = True
            if changed:
                self.sync_callback(dict(self.committed_log))

    def _collect_votes(
        self,
        log_index: int,
        ballot: Ballot,
        op_pair: Tuple[OperationType, int],
    ) -> Tuple[bool, str]:
        op, song = op_pair
        vote_key = (log_index, ballot)

        with self._vote_cond:
            self_vote = vote_for_operation(self.node_id, song, op)
            self._votes[vote_key] = {self.node_id: self_vote}

        vote_req = {
            "type": "VOTE_REQUEST",
            "ballot": self._serialize_ballot(ballot),
            "log_index": log_index,
            "op": op.value,
            "song": song,
        }
        self.broadcast_peer(vote_req)

        deadline = time.monotonic() + self.VOTE_TIMEOUT_SEC
        with self._vote_cond:
            while True:
                votes_map = self._votes.get(vote_key, {})
                yes_count = sum(1 for v in votes_map.values() if v)
                no_count = sum(1 for v in votes_map.values() if not v)

                if yes_count >= self.YES_VOTE_QUORUM:
                    self._votes.pop(vote_key, None)
                    return True, f"Vote passed with {yes_count} YES votes"

                if no_count >= self.NO_VOTE_LIMIT:
                    self._votes.pop(vote_key, None)
                    return False, f"Vote failed with {no_count} NO votes"

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._votes.pop(vote_key, None)
                    return False, f"Vote timed out after {self.VOTE_TIMEOUT_SEC:.0f}s ({yes_count} YES, {no_count} NO)"

                self._vote_cond.wait(timeout=remaining)

    def _broadcast_and_collect(
        self,
        message: Dict[str, Any],
        timeout: float,
    ) -> Dict[int, Optional[Dict[str, Any]]]:
        results: Dict[int, Optional[Dict[str, Any]]] = {}
        results_lock = threading.Lock()
        threads: List[threading.Thread] = []

        def _worker(peer_id: int) -> None:
            reply = self.send_peer(peer_id, message, True, timeout)
            with results_lock:
                results[peer_id] = reply

        for peer_id in range(1, self.TOTAL_NODES + 1):
            if peer_id == self.node_id:
                continue
            t = threading.Thread(target=_worker, args=(peer_id,), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        return results

    def handle_message(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        msg_type = msg.get("type")
        sender_id = int(msg.get("from", -1))

        if msg_type == "GOSSIP_STATE":
            peer_commit_len = int(msg.get("commit_length", 0))
            local_commit_len = self.contiguous_commit_length()
            if peer_commit_len > local_commit_len and sender_id in range(1, self.TOTAL_NODES + 1):
                self.request_catchup_from_peers(preferred_peer=sender_id)
            return {"ok": True, "commit_length": local_commit_len}

        if msg_type == "SYNC_REQUEST":
            from_index = max(0, int(msg.get("from_index", 0)))
            with self.state_lock:
                entries = [
                    {"index": idx, "op": op.value, "song": song}
                    for idx, (op, song) in sorted(self.committed_log.items(), key=lambda x: x[0])
                    if idx >= from_index
                ]
            return {
                "type": "SYNC_RESPONSE",
                "from": self.node_id,
                "entries": entries,
            }

        if msg_type == "SYNC_RESPONSE":
            self._apply_sync_entries(msg.get("entries", []))
            return {"ok": True}

        if msg_type == "PREPARE":
            ballot = self._deserialize_ballot(msg["ballot"])
            log_index = int(msg["log_index"])
            assert ballot is not None

            with self.state_lock:
                self._record_seen_ballot_locked(log_index, ballot)
                cur_promised = self.promised_ballot.get(log_index)
                if cur_promised is None or ballot > cur_promised:
                    self.promised_ballot[log_index] = ballot
                    return {
                        "type": "PROMISE",
                        "granted": True,
                        "ballot": self._serialize_ballot(ballot),
                        "log_index": log_index,
                        "accept_num": self._serialize_ballot(self.accepted_ballot.get(log_index)),
                        "accept_op": self._serialize_op(self.accepted_op.get(log_index)),
                    }
                return {
                    "type": "PROMISE",
                    "granted": False,
                    "ballot": self._serialize_ballot(ballot),
                    "promised": self._serialize_ballot(cur_promised),
                    "log_index": log_index,
                }

        if msg_type == "VOTE_REQUEST":
            ballot = self._deserialize_ballot(msg["ballot"])
            log_index = int(msg["log_index"])
            op = OperationType(str(msg["op"]).upper())
            song = int(msg["song"])
            assert ballot is not None

            with self.state_lock:
                self._record_seen_ballot_locked(log_index, ballot)

            decision = vote_for_operation(self.node_id, song, op)
            reply = {
                "type": "VOTE_REPLY",
                "ballot": self._serialize_ballot(ballot),
                "log_index": log_index,
                "op": op.value,
                "song": song,
                "vote": decision,
            }
            if sender_id in range(1, self.TOTAL_NODES + 1) and sender_id != self.node_id:
                threading.Thread(
                    target=self.send_peer,
                    args=(sender_id, reply, False, 15.0),
                    daemon=True,
                ).start()
            return {"ok": True}

        if msg_type == "VOTE_REPLY":
            ballot = self._deserialize_ballot(msg["ballot"])
            log_index = int(msg["log_index"])
            vote_bool = bool(msg["vote"])
            assert ballot is not None

            with self._vote_cond:
                self._record_seen_ballot_locked(log_index, ballot)
                vote_key = (log_index, ballot)
                if vote_key in self._votes and sender_id in range(1, self.TOTAL_NODES + 1):
                    self._votes[vote_key][sender_id] = vote_bool
                    self._vote_cond.notify_all()
            return {"ok": True}

        if msg_type == "ACCEPT":
            ballot = self._deserialize_ballot(msg["ballot"])
            log_index = int(msg["log_index"])
            op = OperationType(str(msg["op"]).upper())
            song = int(msg["song"])
            assert ballot is not None

            with self.state_lock:
                self._record_seen_ballot_locked(log_index, ballot)
                cur_promised = self.promised_ballot.get(log_index)
                if cur_promised is None or ballot >= cur_promised:
                    self.promised_ballot[log_index] = ballot
                    self.accepted_ballot[log_index] = ballot
                    self.accepted_op[log_index] = (op, song)
                    return {
                        "type": "ACCEPTED",
                        "accepted": True,
                        "ballot": self._serialize_ballot(ballot),
                        "log_index": log_index,
                        "op": op.value,
                        "song": song,
                    }
                return {
                    "type": "ACCEPTED",
                    "accepted": False,
                    "ballot": self._serialize_ballot(ballot),
                    "promised": self._serialize_ballot(cur_promised),
                    "log_index": log_index,
                }

        if msg_type == "DECIDE":
            log_index = int(msg["log_index"])
            op = OperationType(str(msg["op"]).upper())
            song = int(msg["song"])
            self._commit_entry(log_index, op, song)
            return {"ok": True}

        return None

    def _commit_entry(self, log_index: int, op: OperationType, song: int) -> bool:
        with self.state_lock:
            if log_index in self.committed_log:
                return False
            self.committed_log[log_index] = (op, song)
            snapshot = dict(self.committed_log)
            self.sync_callback(snapshot)
            return True