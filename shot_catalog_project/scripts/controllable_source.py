#!/usr/bin/env python3
"""Controllable HTTP proxy in front of the messy local paged log source.

It forwards every request to the real log source while recording each
``/logs`` request range to an append-only JSONL audit log, and can block
chosen page offsets (HTTP 500 on every attempt, including retries) so a
regression driver can deterministically hold a page back until later pages
have already been staged.

Usage:
    controllable_source.py <history_path> <port> <audit_log> <state_dir>
                             [--page-size N] [--seed S]

State files (read on every request, so another process can flip live):
    <state_dir>/blocked_offsets   one integer offset per line, blank = none
    <state_dir>/redirect_offsets JSON list of {"from", "to"} mappings: a
                                  request for page `from` is answered with the
                                  contents of page `to`, letting a driver make
                                  later pages arrive while an earlier page is
                                  still held back (out-of-order delivery).
"""

from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(PROJECT_ROOT / "src"),
    "/workspace/pylibs/lib/python3.11/site-packages",
]

from shot_catalog.log_store import PagedLogSource, serve  # noqa: E402


class ControllableSource(PagedLogSource):
    """Adds request auditing and a live blocklist of page offsets."""

    def __init__(self, log_path, page_size_hint, audit_log, state_dir, seed):
        super().__init__(log_path, page_size_hint=page_size_hint,
                         shuffle=True, duplicate_rate=0.35,
                         error_rate=0.12, seed=seed)
        self.audit_log = Path(audit_log)
        self.state_dir = Path(state_dir)
        self._audit_lock = threading.Lock()
        self._block_lock = threading.Lock()
        self.blocked: set[int] = set()
        self.redirects: dict[int, int] = {}

    def refresh_blocklist(self) -> None:
        path = self.state_dir / "blocked_offsets"
        offsets: set[int] = set()
        if path.exists():
            for line in path.read_text().splitlines():
                line = line.strip()
                if line:
                    offsets.add(int(line))
        redirect_path = self.state_dir / "redirect_offsets"
        redirects: dict[int, int] = {}
        if redirect_path.exists() and redirect_path.read_text().strip():
            for mapping in json.loads(redirect_path.read_text()):
                redirects[int(mapping["from"])] = int(mapping["to"])
        with self._block_lock:
            self.blocked = offsets
            self.redirects = redirects

    def is_blocked(self, offset: int) -> bool:
        with self._block_lock:
            return offset in self.blocked

    def redirect_target(self, offset: int) -> int | None:
        with self._block_lock:
            return self.redirects.get(offset)

    def audit(self, entry: dict) -> None:
        line = json.dumps(entry, ensure_ascii=False)
        with self._audit_lock:
            with self.audit_log.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")


def build_proxy_handler(source: ControllableSource) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def _send(self, code: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            if parsed.path == "/health":
                self._send(200, {"status": "ok", "total": source.total})
                return
            if parsed.path == "/meta":
                self._send(200, {"total": source.total,
                                 "page_size_hint": source.page_size_hint,
                                 "path": str(source.log_path)})
                return
            if parsed.path != "/logs":
                self._send(404, {"error": "unknown path"})
                return
            source.refresh_blocklist()
            offset = int(qs.get("offset", ["0"])[0])
            limit = int(qs.get("limit", [str(source.page_size_hint)])[0])
            source.audit({"event": "request", "offset": offset, "limit": limit})
            # Out-of-order delivery wins over the blocklist: a redirected page
            # is served from another page's rows, while an un-redirected
            # blocked page keeps returning 500 until the harness releases it.
            target = source.redirect_target(offset)
            if target is None and source.is_blocked(offset):
                source.audit({"event": "blocked", "offset": offset})
                self._send(500, {"error": "page held back by regression harness"})
                return
            serve_offset = target if target is not None else offset
            if target is not None:
                source.audit({"event": "redirect", "offset": offset,
                              "to_offset": target})
            try:
                page = source.serve_page(serve_offset, limit)
            except Exception as exc:  # pragma: no cover - defensive
                source.audit({"event": "transient_500", "offset": offset})
                self._send(500, {"error": str(exc)})
                return
            # Keep the envelope's requested offset so the client's paging
            # state stays coherent; only the records come from elsewhere.
            page["offset"] = offset
            if target is not None:
                # The envelope's has_more must describe the REQUESTED window,
                # otherwise a redirect to a short tail page makes the client
                # stop paging one page early.
                page["has_more"] = offset + limit < source.total
            seqs = sorted({r["seq"] for r in page["records"]})
            source.audit({
                "event": "served", "offset": offset,
                "served_from": serve_offset,
                "seq_min": seqs[0], "seq_max": seqs[-1],
                "returned": page["returned"],
            })
            self._send(200, page)

    return Handler


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    history, port_s, audit_log, state_dir = argv[0], int(argv[1]), argv[2], argv[3]
    page_size = 10
    seed = 20261119
    if "--page-size" in argv:
        page_size = int(argv[argv.index("--page-size") + 1])
    if "--seed" in argv:
        seed = int(argv[argv.index("--seed") + 1])
    source = ControllableSource(history, page_size, audit_log, state_dir, seed)
    httpd = ThreadingHTTPServer(
        ("127.0.0.1", port_s), build_proxy_handler(source)
    )
    print(f"controllable source on http://127.0.0.1:{port_s} "
          f"serving {source.total} records (page_size={page_size})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


# Kept available for in-process embedding by tests/drivers.
__all__ = ["ControllableSource", "build_proxy_handler", "serve"]


if __name__ == "__main__":
    raise SystemExit(main())
