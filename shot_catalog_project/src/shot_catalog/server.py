"""HTTP query API and process entrypoint.

Query API (read-only, served while synchronization is running)
==============================================================
GET /health
    -> {"status": "ok", "snapshot_seq": N, ...sync stats}

GET /shot?film_id=F&shot_id=S
    -> 200 the shot's current attributes (duration always in milliseconds,
            repair_note null when unavailable)
    -> 404 when the shot does not exist OR is currently withdrawn

GET /film/F
    -> 200 {"film_id", "snapshot_seq", "count", "shots": [...]}
       Streamed (chunked) from one catalog snapshot with keyset pagination,
       so the answer is internally consistent, bounded in Python memory, and
       keeps draining while later batches publish. A revision committing
       mid-request can never split the answer across two states.

GET /stats
    -> 200 staging/snapshot counters

The process runs the synchronization loop in a background thread against the
same DuckDB file. Every request reads through its own MVCC snapshot (a
DuckDB cursor), so it answers the last published version immediately -- even
while the sync thread is inside a large uncommitted ``apply_range``
transaction for a cross-page batch. Queries never take the writer's lock.
The snapshot is released when the response completes or the client
disconnects.
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .catalog import Catalog
from .sync import LogSourceClient, SyncEngine

log = logging.getLogger("shot_catalog.server")

# Streamed responses are flushed in bounded pieces; a listing of 100k shots
# never exists as one Python string or one socket buffer.
_STREAM_CHUNK_BYTES = 64 * 1024


def build_handler(catalog: Catalog) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def _send(self, code: int, payload: dict | list) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        # ---------------------------------------------------------- streaming
        def _send_chunk(self, data: bytes) -> None:
            self.wfile.write(f"{len(data):X}\r\n".encode("ascii"))
            self.wfile.write(data)
            self.wfile.write(b"\r\n")

        def _send_film_stream(self, film_id: str) -> None:
            """Stream one film listing from one published snapshot.

            The rows are read page by page inside a single read transaction
            (its own MVCC snapshot), so the answer is internally consistent
            and bounded in memory. The connection is never held on the
            writer: newer batches keep publishing while this response drains.
            If the client goes away mid-stream, writing raises and the
            ``with`` block closes the snapshot immediately, releasing it.
            """
            disconnected = False
            with catalog.read_snapshot() as snap:
                count = snap.film_shot_count(film_id)
                snapshot_seq = snap.snapshot_seq()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Transfer-Encoding", "chunked")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                head = (
                    '{"film_id":' + json.dumps(film_id, ensure_ascii=False)
                    + ',"snapshot_seq":' + str(snapshot_seq)
                    + ',"count":' + str(count)
                    + ',"shots":['
                )
                buf = [head.encode("utf-8")]
                buflen = len(buf[0])
                first = True
                try:
                    for shot in snap.iter_film_shots(film_id):
                        item = ("" if first else ",") + json.dumps(
                            shot, ensure_ascii=False, default=str)
                        first = False
                        encoded = item.encode("utf-8")
                        buf.append(encoded)
                        buflen += len(encoded)
                        if buflen >= _STREAM_CHUNK_BYTES:
                            self._send_chunk(b"".join(buf))
                            buf, buflen = [], 0
                    buf.append(b"]}")
                    self._send_chunk(b"".join(buf))
                    self.wfile.write(b"0\r\n\r\n")
                except (BrokenPipeError, ConnectionResetError):
                    # Client disconnected: stop reading immediately; the with
                    # block closes the cursor and unpins the snapshot.
                    disconnected = True
                    self.close_connection = True
                except Exception:
                    # Headers and part of the body are already on the wire, so
                    # a 500 is impossible. Abort the chunked stream instead of
                    # pretending it completed: the client sees a truncated
                    # body and knows the answer is not authoritative. Never
                    # propagate -- the caller would try to send a second
                    # response on a connection whose headers are gone.
                    log.exception("streaming /film/%s failed", film_id)
                    self.close_connection = True
                    disconnected = True
            if disconnected:
                log.debug("client disconnected while streaming /film/%s", film_id)

        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            try:
                if parsed.path == "/health":
                    with catalog.read_snapshot() as snap:
                        self._send(200, {"status": "ok", **snap.stats()})
                elif parsed.path == "/stats":
                    with catalog.read_snapshot() as snap:
                        self._send(200, snap.stats())
                elif parsed.path == "/shot":
                    film_id = qs["film_id"][0]
                    shot_id = qs["shot_id"][0]
                    with catalog.read_snapshot() as snap:
                        shot = snap.get_shot(film_id, shot_id)
                    if shot is None:
                        self._send(404, {
                            "error": "shot not found or currently withdrawn",
                            "film_id": film_id, "shot_id": shot_id,
                        })
                    else:
                        self._send(200, shot)
                elif parsed.path.startswith("/film/"):
                    film_id = parsed.path[len("/film/"):]
                    if not film_id:
                        self._send(400, {"error": "film_id required"})
                    else:
                        self._send_film_stream(film_id)
                else:
                    self._send(404, {"error": "unknown path"})
            except KeyError:
                self._send(400, {"error": "missing required query parameter"})
            except Exception:  # pragma: no cover - defensive
                log.exception("query failed")
                self._send(500, {"error": "internal error"})

    return Handler


class ShotCatalogService:
    """Owns the catalog, optional sync thread and HTTP server."""

    def __init__(self, db_path: str | Path, log_source_url: str | None = None,
                 *, page_size: int = 100, poll_interval: float = 0.15):
        self.catalog = Catalog(db_path)
        self.engine: SyncEngine | None = None
        self._sync_thread: threading.Thread | None = None
        if log_source_url:
            client = LogSourceClient(log_source_url)
            self.engine = SyncEngine(self.catalog, client,
                                     page_size=page_size,
                                     poll_interval=poll_interval)
        self.httpd: ThreadingHTTPServer | None = None

    def start_sync(self) -> None:
        if self.engine is None:
            return
        self._sync_thread = threading.Thread(
            target=self.engine.run_forever, name="sync-loop", daemon=True
        )
        self._sync_thread.start()

    def serve(self, host: str, port: int,
              runtime_json: str | Path | None = None) -> int:
        self.httpd = ThreadingHTTPServer((host, port), build_handler(self.catalog))
        actual_port = self.httpd.server_address[1]
        if runtime_json is not None:
            Path(runtime_json).write_text(json.dumps({
                "host": host, "port": actual_port,
                "query_base": f"http://{host}:{actual_port}",
            }), encoding="utf-8")
        self.start_sync()
        log.info("query API on http://%s:%d (snapshot_seq=%d)",
                 host, actual_port, self.catalog.last_snapshot_seq())
        try:
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            if self.engine:
                self.engine.stop()
            self.httpd.shutdown()
            self.httpd.server_close()
            if self._sync_thread:
                self._sync_thread.join(timeout=5)
            self.catalog.close()
        return actual_port


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Shot catalog synchronization service")
    parser.add_argument("--db", required=True, help="DuckDB file path (durable state)")
    parser.add_argument("--log-source-url", default=None,
                        help="Base URL of the local paged log source. "
                             "Omit to serve queries only from existing state.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--poll-interval", type=float, default=0.15)
    parser.add_argument("--runtime-json", default=None,
                        help="Write the actual listening port here (for demos/tests)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    service = ShotCatalogService(
        args.db, args.log_source_url,
        page_size=args.page_size, poll_interval=args.poll_interval,
    )
    service.serve(args.host, args.port, runtime_json=args.runtime_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
