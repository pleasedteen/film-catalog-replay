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

GET /logs?offset=0&limit=100[&films=F1,F2]
    -> 200 {"offset": o, "limit": l, "total": N, "has_more": bool,
            "returned": R, "records": [ ... ],
            "batch_runs": [ {"batch_id": B, "start_seq": s, "end_seq": e}, ... ]}
       The window is the *global* record range [o+1, o+l] (clipped to total).
       When ``films`` is supplied, ``records`` carries only those films' shot
       events: unrelated ordinary revisions never cross the wire, so transferred
       bytes track the subscription rather than the whole library. ``total`` and
       ``has_more`` stay global so a sparse reader still advances its global
       snapshot.
       ``batch_runs`` reports the FULL extent of every same-batch revision that
       intersects the global window, regardless of the film filter. A sparse
       reader needs it to publish a cross-film batch atomically for subscribed
       films without ever transferring the unsubscribed remainder.
       Records may be shuffled/duplicated as described above.
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
        # Controller state for "the log keeps growing while a client syncs"
        # and for a new film that only appears partway through a history.
        # ``visible_total`` is what /health and /logs currently expose.
        self._visible_total: int | None = None
        self._published_films: set[str] = set()
        self._build_index()

    def _build_index(self) -> None:
        with self.log_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    self._records.append(json.loads(line))
        # Whole-history batch extents: a batch's run is contiguous by the
        # format contract, so the first/last seq per batch_id is its extent.
        runs: dict[str, list[int]] = {}
        for rec in self._records:
            bid = rec.get("batch_id")
            if bid is None:
                continue
            seq = int(rec["seq"])
            span = runs.get(bid)
            if span is None:
                runs[bid] = [seq, seq]
            else:
                span[0] = min(span[0], seq)
                span[1] = max(span[1], seq)
        self._batch_extents = {
            bid: (span[0], span[1]) for bid, span in runs.items()
        }
        # List of (start_seq, end_seq, batch_id) used for range overlap lookups.
        self._batch_spans = [
            (start, end, bid) for bid, (start, end) in self._batch_extents.items()
        ]

    @property
    def total(self) -> int:
        """Currently exposed record count (may grow during a sync demo)."""
        if self._visible_total is None:
            return len(self._records)
        return min(self._visible_total, len(self._records))

    def set_visible_total(self, n: int) -> None:
        """Simulate the source log growing: /health and /logs stop at ``n``."""
        self._visible_total = max(0, min(n, len(self._records)))

    def reveal_all(self) -> None:
        self._visible_total = None

    def read_record(self, seq: int) -> dict:
        if not 1 <= seq <= self.total:
            raise IndexError(seq)
        return self._records[seq - 1]

    def batch_runs_in(self, start_seq: int, end_seq: int) -> list[dict]:
        """Full extents of batches intersecting [start_seq, end_seq]."""
        out = []
        for bstart, bend, bid in self._batch_spans:
            if bstart <= end_seq and bend >= start_seq:
                out.append({"batch_id": bid, "start_seq": bstart, "end_seq": bend})
        return out

    def serve_page(self, offset: int, limit: int,
                   films: set[str] | None = None) -> dict:
        """Return one messy page covering source rows [offset, offset+limit).

        ``films`` (optional) restricts the *records* to those films without
        changing the global window, total or has_more: sparse consumers still
        advance their global snapshot while only subscribed shot events cross
        the wire. Batch extents are always reported over the whole window.
        """
        if offset < 0 or limit <= 0:
            raise ValueError("offset must be >= 0 and limit > 0")
        total = self.total
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
        if films is not None:
            records = [r for r in records if r.get("film_id") in films]
        return {
            "offset": offset,
            "limit": limit,
            "returned": len(records),
            "total": total,
            "has_more": offset + clipped < total,
            "records": records,
            "batch_runs": self.batch_runs_in(offset + 1, offset + max(clipped, 0)),
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
                    films_raw = qs.get("films", [None])[0]
                    films = None
                    if films_raw is not None:
                        films = {f for f in films_raw.split(",") if f}
                    try:
                        page = source.serve_page(offset, limit, films=films)
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
