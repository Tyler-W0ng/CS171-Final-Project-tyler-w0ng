import argparse
import socket
import sys
from typing import Any, Dict, Optional

from node import BASE_PORT, DEFAULT_HOST, node_port, recv_json_frame, send_json_frame
from playlist import OperationType, validate_song, validate_student_id


def send_command(
    k: int,
    payload: Dict[str, Any],
    host: str = DEFAULT_HOST,
    base_port: int = BASE_PORT,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    validate_student_id(k)
    port = node_port(k, base_port)
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        send_json_frame(sock, payload)
        reply = recv_json_frame(sock)
        if reply is None:
            raise RuntimeError(f"No response received from node {k}")
        return reply


def propose_song(
    k: int,
    song: int,
    op: OperationType,
    host: str = DEFAULT_HOST,
    base_port: int = BASE_PORT,
) -> Dict[str, Any]:
    validate_student_id(k)
    validate_song(song)
    return send_command(
        k=k,
        payload={
            "type": "PROPOSE_SONG",
            "k": k,
            "song": song,
            "op": op.value,
        },
        host=host,
        base_port=base_port,
    )


def pause_node(k: int, host: str = DEFAULT_HOST, base_port: int = BASE_PORT) -> Dict[str, Any]:
    return send_command(k=k, payload={"type": "PAUSE", "k": k}, host=host, base_port=base_port)


def resume_node(k: int, host: str = DEFAULT_HOST, base_port: int = BASE_PORT) -> Dict[str, Any]:
    return send_command(k=k, payload={"type": "RESUME", "k": k}, host=host, base_port=base_port)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI client for CS 171 Paxos Playlist nodes")
    parser.add_argument("--host", type=str, default=DEFAULT_HOST)
    parser.add_argument("--base-port", type=int, default=BASE_PORT)

    subparsers = parser.add_subparsers(dest="command", required=True)

    propose_p = subparsers.add_parser("propose", help="Propose ADD or DELETE for a song")
    propose_p.add_argument("--node", type=int, required=True, help="Student ID (1..5)")
    propose_p.add_argument("--song", type=int, required=True, help="5-digit song integer")
    propose_p.add_argument("--op", type=str, required=True, choices=["ADD", "DELETE"])

    pause_p = subparsers.add_parser("pause", help="Pause a student node")
    pause_p.add_argument("--node", type=int, required=True, help="Student ID (1..5)")

    resume_p = subparsers.add_parser("resume", help="Resume a student node")
    resume_p.add_argument("--node", type=int, required=True, help="Student ID (1..5)")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "propose":
            resp = propose_song(
                k=args.node,
                song=args.song,
                op=OperationType(args.op),
                host=args.host,
                base_port=args.base_port,
            )
        elif args.command == "pause":
            resp = pause_node(k=args.node, host=args.host, base_port=args.base_port)
        elif args.command == "resume":
            resp = resume_node(k=args.node, host=args.host, base_port=args.base_port)
        else:
            raise ValueError(f"Unknown command: {args.command}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(resp)
    return 0 if resp.get("ok", False) else 1


if __name__ == "__main__":
    sys.exit(main())