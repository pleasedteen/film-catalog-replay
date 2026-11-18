"""Shot catalog synchronization service.

Modules:
    log_format:  source log record format and normalization (v1/v2 workstations)
    fixturegen: deterministic fixture (source history) generator
    log_store:  local HTTP paged log source with messy delivery behavior
    catalog:    DuckDB-backed durable catalog (staging + atomic snapshots)
    sync:       paged catch-up synchronization loop
    server:     HTTP query API + process entrypoint
"""

__version__ = "1.0.0"
