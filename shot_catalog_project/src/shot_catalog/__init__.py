"""Shot catalog synchronization service.

Modules:
    log_format:  source log record format and normalization (v1/v2 workstations)
    fixturegen: deterministic fixture (source history) generator
    log_store:  local HTTP paged log source with messy delivery behavior
    catalog:    DuckDB-backed durable catalog (hot staging + atomic snapshots)
    cold:       independently maintainable cold Parquet archive + manifest
    sync:       paged catch-up synchronization loop with cold backpressure
    server:     HTTP query API + process entrypoint
"""

__version__ = "1.1.0"
