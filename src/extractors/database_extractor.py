"""Database extractor for PostgreSQL and MySQL sources.

Uses SQLAlchemy with connection pooling. Supports either a full-table read, a
custom query, or incremental extraction filtered on a watermark column.

Supported ``options``:

    connection:      SQLAlchemy URL, or a dict of host/port/database/user/password.
    table:           Table to read (mutually exclusive with ``query``).
    query:           Raw SQL to execute.
    incremental_key: Column used for incremental watermarking.
    batch_size:      Server-side fetch size for large reads.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.extractors.base import BaseExtractor, ExtractionResult


class DatabaseExtractor(BaseExtractor):
    """Extract data from a relational database source."""

    _DRIVERS = {"postgresql": "postgresql+psycopg2", "mysql": "mysql+pymysql"}

    def _connection_url(self) -> str:
        conn = self.options.get("connection")
        if isinstance(conn, str):
            return conn
        if isinstance(conn, dict):
            driver = self._DRIVERS[self.config.type]
            return (
                f"{driver}://{conn.get('user')}:{conn.get('password')}"
                f"@{conn.get('host', 'localhost')}:{conn.get('port')}/{conn.get('database')}"
            )
        raise ValueError(f"Source '{self.config.name}' is missing a 'connection' option")

    def _engine(self) -> Engine:
        return create_engine(
            self._connection_url(),
            pool_pre_ping=True,
            pool_size=self.options.get("pool_size", 5),
            max_overflow=self.options.get("max_overflow", 10),
        )

    def extract(self, since: Any | None = None) -> ExtractionResult:
        engine = self._engine()
        incremental_key = self.options.get("incremental_key")

        if self.options.get("query"):
            query = self.options["query"]
            params: dict[str, Any] = {}
            if since is not None and incremental_key:
                query = f"SELECT * FROM ({query}) AS sub WHERE {incremental_key} > :since"
                params = {"since": since}
            sql = text(query)
        else:
            table = self.options["table"]
            if since is not None and incremental_key:
                sql = text(f"SELECT * FROM {table} WHERE {incremental_key} > :since")
                params = {"since": since}
            else:
                sql = text(f"SELECT * FROM {table}")
                params = {}

        with engine.connect().execution_options(stream_results=True) as conn:
            frame = pd.read_sql(sql, conn, params=params)

        engine.dispose()
        self.log.info(
            "Database extraction complete",
            extra={"source": self.config.name, "rows": len(frame)},
        )
        return ExtractionResult.from_frame(
            self.config, frame, incremental=since is not None
        )

    def test_connection(self) -> bool:
        try:
            engine = self._engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return True
        except Exception:  # noqa: BLE001 - connectivity probe
            return False
