from enum import Enum
from typing import Dict, Iterable, List, Optional, Set, Tuple


class Preference(str, Enum):
    LIKES = "L"
    DISLIKES = "D"
    INDIFFERENT = "I"


class OperationType(str, Enum):
    ADD = "ADD"
    DELETE = "DELETE"


LogEntry = Tuple[int, OperationType, int]


def validate_student_id(k: int) -> None:
    if not isinstance(k, int) or isinstance(k, bool) or not (1 <= k <= 5):
        raise ValueError(f"Student id k must be an integer between 1 and 5, got {k!r}")


def validate_song(song: int) -> str:
    if not isinstance(song, int) or isinstance(song, bool) or not (10000 <= song <= 99999):
        raise ValueError(f"Song must be a 5-digit positive integer, got {song!r}")
    return str(song)


def get_preference(k: int, song: int) -> Preference:
    validate_student_id(k)
    song_str = validate_song(song)
    digit = song_str[k - 1]
    if digit in ("1", "2"):
        return Preference.LIKES
    if digit in ("3", "4"):
        return Preference.DISLIKES
    return Preference.INDIFFERENT


def can_propose(k: int, song: int, op: OperationType) -> bool:
    pref = get_preference(k, song)
    if pref == Preference.LIKES:
        return op == OperationType.ADD
    if pref == Preference.DISLIKES:
        return op == OperationType.DELETE
    return False


def vote_for_operation(k: int, song: int, op: OperationType) -> bool:
    pref = get_preference(k, song)
    if pref == Preference.LIKES:
        return op == OperationType.ADD
    if pref == Preference.DISLIKES:
        return op == OperationType.DELETE
    return True


def is_operation_applicable(op: OperationType, song: int, current_playlist: Set[int]) -> bool:
    validate_song(song)
    if op == OperationType.ADD:
        return song not in current_playlist
    if op == OperationType.DELETE:
        return song in current_playlist
    raise ValueError(f"Unsupported operation: {op!r}")


def can_student_propose_now(
    k: int,
    song: int,
    op: OperationType,
    current_playlist: Set[int],
) -> Tuple[bool, Optional[str]]:
    try:
        validate_student_id(k)
        validate_song(song)
    except ValueError as exc:
        return False, str(exc)

    if not can_propose(k, song, op):
        pref = get_preference(k, song)
        return False, f"Student {k} with preference {pref.value} cannot propose {op.value} for song {song}"

    if not is_operation_applicable(op, song, current_playlist):
        if op == OperationType.ADD:
            return False, f"Song {song} is already in the playlist"
        return False, f"Song {song} is not in the playlist"

    return True, None


def build_playlist_from_entries(entries: Iterable[LogEntry]) -> Set[int]:
    sorted_entries = sorted(entries, key=lambda item: item[0])
    playlist: Set[int] = set()

    expected_index = 0
    for index, op, song in sorted_entries:
        if index != expected_index:
            raise ValueError(f"Non-contiguous log entries: expected index {expected_index}, got {index}")
        validate_song(song)
        if op == OperationType.ADD:
            if song in playlist:
                raise ValueError(f"Invalid log at index {index}: ADD {song} when song is already present")
            playlist.add(song)
        elif op == OperationType.DELETE:
            if song not in playlist:
                raise ValueError(f"Invalid log at index {index}: DELETE {song} when song is not present")
            playlist.remove(song)
        else:
            raise ValueError(f"Invalid operation at index {index}: {op!r}")
        expected_index += 1

    return playlist


def build_playlist_from_log(committed_log: Dict[int, Tuple[OperationType, int]]) -> Set[int]:
    entries: List[LogEntry] = [
        (idx, op, song) for idx, (op, song) in sorted(committed_log.items(), key=lambda item: item[0])
    ]
    return build_playlist_from_entries(entries)


def sorted_playlist_songs(playlist: Iterable[int]) -> List[int]:
    return sorted(set(playlist))