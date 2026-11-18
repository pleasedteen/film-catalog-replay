"""Local HTTP paged log source with deliberately messy delivery.

The source history itself is an immutable gapless JSONL file (see
fixturegen.py). This service serves it over HTTP while simulating every
delivery pathology the synchronizer must tolerate:

* overlapping pages  -- clients may request any (offset, limit); the same
  records are reachable through many different pages;
* duplicate records  -- records may appear twice inside one response;
* shuffled order     -- records within a page may be returned out of seq
  order, and pages themselves are requested in arbitrary order by the client;
* transient errors   -- seeded random 500 responses that succeed on retry.

HTTP API
========
GET /health
    -> {"status": "ok", "total": <record count>}

GET /meta
    -> {"total": N, "page_size_hint": P, "path": ...}

GET /logs?offset=0&limit=100
    -> 200 {"offset": o, "limit": l, "total": N, "has_more": bool,
            "records": [ <raw source record JSON objects>, ... ]}
       Records belong to seq range [o+1, o+l] (clipped to total), possibly
       shuffled and duplicated. The true seq order is recoverable from each
       record's "seq" field.
    -> 500 {"error": "transient delivery failure"} on simulated failures.

GET /raw?seq=K
    -> 200 the single, unmodified source record with seq=K (test/debug aid;
       never used by the synchronizer).

No external hosts are ever contacted.
"""

from __future__ import annotations

import argparse
import json
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


class PagedLogSource:
    """Serves an immutable history file over HTTP with messy delivery.

    The history is parsed once at construction (source-side caching, like a
    real log broker holding its segment files); the synchronizer itself never
    caches more than one page. Byte offsets are retained for /raw lookups.
    """

    def __init__(self, log_path: str | Path, page_size_hint: int = 100,
                 *, shuffle: bool = True, duplicate_rate: float = 0.35,
                 error_rate: float = 0.12, seed: int = 20261101):
        self.log_path = Path(log_path)
        self.page_size_hint = page_size_hint
        self.shuffle = shuffle
        self.duplicate_rate = duplicate_rate
        self.error_rate = error_rate
        self._rng = random.Random(seed)
        self._lock = threading.Lock()
        self._records: list[dict] = []
        self._build_index()

    def _build_index(self) -> None:
        with self.log_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    self._records.append(json.loads(line))

    @property
    def total(self) -> int:
        return len(self._records)

    def read_record(self, seq: int) -> dict:
        if not 1 <= seq <= len(self._records):
            raise IndexError(seq)
        return self._records[seq - 1]

    def serve_page(self, offset: int, limit: int) -> dict:
        """Return one messy page covering source rows [offset, offset+limit)."""
        if offset < 0 or limit <= 0:
            raise ValueError("offset must be >= 0 and limit > 0")
        total = len(self._records)
        clipped = min(limit, total - offset)
        seqs = list(range(offset + 1, offset + 1 + max(clipped, 0)))
        with self._lock:
            # Deterministic per-request chaos, safe under concurrent clients.
            if self.error_rate and self._rng.random() < self.error_rate:
                raise TransientLogError("simulated transient delivery failure")
            if self.shuffle and len(seqs) > 1:
                self._rng.shuffle(seqs)
            duplicated: list[int] = []
            for s in seqs:
                duplicated.append(s)
                if self.duplicate_rate and self._rng.random() < self.duplicate_rate:
                    duplicated.append(s)
            if self.shuffle and len(duplicated) > 1:
                self._rng.shuffle(duplicated)
        records = [self._records[s - 1] for s in duplicated]
        return {
            "offset": offset,
            "limit": limit,
            "returned": len(records),
            "total": total,
            "has_more": offset + clipped < total,
            "records": records,
        }


class TransientLogError(RuntimeError):
    pass


def build_handler(source: PagedLogSource) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):  # silence default stderr logging
            pass

        def _send(self, code: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802 - stdlib naming
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            try:
                if parsed.path == "/health":
                    self._send(200, {"status": "ok", "total": source.total})
                elif parsed.path == "/meta":
                    self._send(200, {"total": source.total,
                                     "page_size_hint": source.page_size_hint,
                                     "path": str(source.log_path)})
                elif parsed.path == "/logs":
                    offset = int(qs.get("offset", ["0"])[0])
                    limit = int(qs.get("limit", [str(source.page_size_hint)])[0])
                    try:
                        page = source.serve_page(offset, limit)
                    except TransientLogError as exc:
                        self._send(500, {"error": str(exc)})
                        return
                    except ValueError as exc:
                        self._send(400, {"error": str(exc)})
                        return
                    self._send(200, page)
                elif parsed.path == "/raw":
                    seq = int(qs["seq"][0])
                    try:
                        self._send(200, source.read_record(seq))
                    except IndexError:
                        self._send(404, {"error": f"seq {seq} not found"})
                else:
                    self._send(404, {"error": "unknown path"})
            except (ValueError, KeyError):
                self._send(400, {"error": "bad query parameters"})

    return Handler


def serve(source: PagedLogSource, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), build_handler(source))
    return httpd


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local messy paged log source")
    parser.add_argument("log_path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--no-shuffle", action="store_true")
    parser.add_argument("--no-duplicates", action="store_true")
    parser.add_argument("--error-rate", type=float, default=0.12)
    parser.add_argument("--seed", type=int, default=20261101)
    args = parser.parse_args(argv)

    source = PagedLogSource(
        args.log_path, page_size_hint=args.page_size,
        shuffle=not args.no_shuffle,
        duplicate_rate=0.0 if args.no_duplicates else 0.35,
        error_rate=args.error_rate, seed=args.seed,
    )
    httpd = serve(source, args.host, args.port)
    print(f"log source listening on http://{args.host}:{httpd.server_address[1]} "
          f"serving {source.total} records")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
