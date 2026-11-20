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
       The whole response is read from one catalog snapshot: a revision
       committing mid-request can never split the answer across two states.

GET /stats
    -> 200 staging/snapshot counters

The process runs the synchronization loop in a background thread against the
same DuckDB file (separate connection), so queries stay responsive while
catching up. Every catalog mutation is one DuckDB transaction; read-only
query connections see a consistent snapshot for the duration of a request.
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

        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            try:
                if parsed.path == "/health":
                    self._send(200, {"status": "ok", **catalog.stats()})
                elif parsed.path == "/stats":
                    self._send(200, catalog.stats())
                elif parsed.path == "/subscriptions":
                    self._send(200, {
                        "films": catalog.subscription_rows(),
                        "sparse": catalog.has_subscriptions(),
                    })
                elif parsed.path == "/shot":
                    film_id = qs["film_id"][0]
                    shot_id = qs["shot_id"][0]
                    shot = catalog.get_shot(film_id, shot_id)
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
                        self._send(200, catalog.list_film_shots(film_id))
                else:
                    self._send(404, {"error": "unknown path"})
            except KeyError:
                self._send(400, {"error": "missing required query parameter"})
            except Exception:  # pragma: no cover - defensive
                log.exception("query failed")
                self._send(500, {"error": "internal error"})

        def do_POST(self):  # noqa: N802
            parsed = urlparse(self.path)
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(length) if length else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                if parsed.path == "/subscriptions":
                    film_id = payload.get("film_id")
                    if not isinstance(film_id, str) or not film_id:
                        self._send(400, {"error": "film_id required"})
                        return
                    status = catalog.add_subscription(film_id)
                    self._send(200, {"film_id": film_id, "status": status,
                                     "subscriptions": catalog.subscription_rows()})
                else:
                    self._send(404, {"error": "unknown path"})
            except (ValueError, json.JSONDecodeError):
                self._send(400, {"error": "invalid JSON body"})
            except Exception:  # pragma: no cover - defensive
                log.exception("subscription request failed")
                self._send(500, {"error": "internal error"})

    return Handler


class ShotCatalogService:
    """Owns the catalog, optional sync thread and HTTP server."""

    def __init__(self, db_path: str | Path, log_source_url: str | None = None,
                 *, page_size: int = 100, poll_interval: float = 0.15,
                 subscribe: list[str] | None = None):
        self.catalog = Catalog(db_path)
        # Seed subscriptions before the sync thread starts so its first pass
        # only transfers the films the warehouse asked for.
        for film in subscribe or []:
            self.catalog.add_subscription(film)
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
    parser.add_argument("--subscribe", default=None,
                        help="Comma-separated film ids to sync (sparse mode). "
                             "Omit to sync every film in the source history.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    subscribe = [f for f in (args.subscribe or "").split(",") if f]
    service = ShotCatalogService(
        args.db, args.log_source_url,
        page_size=args.page_size, poll_interval=args.poll_interval,
        subscribe=subscribe,
    )
    service.serve(args.host, args.port, runtime_json=args.runtime_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
