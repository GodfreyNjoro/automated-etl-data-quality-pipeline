"""SQL warehouse loader with full/incremental strategies and transactions.

Loading happens inside a single database transaction so a mid-load failure
rolls back cleanly, leaving the target table in its previous consistent state.

Strategies:
    full         - replace the entire table contents.
    incremental  - upsert/append only new rows. When an ``incremental_key`` is
                   provided, existing rows with matching keys are deleted before
                   the new batch is appended (delete-insert upsert), which works
                   uniformly across PostgreSQL and MySQL.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.loaders.base import BaseLoader, LoadResult
from src.utils.settings import get_settings


class WarehouseLoader(BaseLoader):
    """Load data into a PostgreSQL or MySQL warehouse table."""

    _DRIVERS = {"postgresql": "postgresql+psycopg2", "mysql": "mysql+pymysql"}

    def _connection_url(self) -> str:
        conn = self.config.connection
        if isinstance(conn, str) and conn:
            return conn
        if self.config.type == "sqlite":
            path = (conn or {}).get("path", ":memory:") if isinstance(conn, dict) else ":memory:"
            return f"sqlite:///{path}"
        if isinstance(conn, dict) and conn:
            driver = self._DRIVERS[self.config.type]
            return (
                f"{driver}://{conn.get('user')}:{conn.get('password')}"
                f"@{conn.get('host', 'localhost')}:{conn.get('port')}/{conn.get('database')}"
            )
        # Fall back to the default warehouse from settings.
        return get_settings().warehouse_db_url()

    def _engine(self) -> Engine:
        # SQLite does not support connection pooling arguments the same way.
        if self.config.type == "sqlite":
            return create_engine(self._connection_url())
        return create_engine(
            self._connection_url(),
            pool_pre_ping=True,
            pool_size=self.config.options.get("pool_size", 5),
            max_overflow=self.config.options.get("max_overflow", 10),
        )

    def load(
        self,
        df: pd.DataFrame,
        table: str,
        strategy: str = "full",
        incremental_key: str | None = None,
    ) -> LoadResult:
        engine = self._engine()
        batch_size = self.config.options.get("batch_size", get_settings().batch_size)
        schema = self.config.options.get("schema")

        try:
            with engine.begin() as conn:  # single transaction: commit or rollback
                if strategy == "full":
                    df.to_sql(
                        table, conn, schema=schema, if_exists="replace",
                        index=False, chunksize=batch_size, method="multi",
                    )
                elif strategy == "incremental":
                    if (
                        incremental_key
                        and incremental_key in df.columns
                        and self._table_exists(conn, table, schema)
                    ):
                        keys = df[incremental_key].dropna().unique().tolist()
                        if keys:
                            self._delete_existing(conn, table, schema, incremental_key, keys)
                    df.to_sql(
                        table, conn, schema=schema, if_exists="append",
                        index=False, chunksize=batch_size, method="multi",
                    )
                else:
                    raise ValueError(f"Unknown load strategy: {strategy}")
        finally:
            engine.dispose()

        self.log.info(
            "Load complete",
            extra={"target": self.config.name, "table": table,
                   "rows": len(df), "strategy": strategy},
        )
        return LoadResult(
            target_name=self.config.name,
            table=table,
            rows_loaded=len(df),
            strategy=strategy,
            metadata={"incremental_key": incremental_key},
        )

    @staticmethod
    def _table_exists(conn, table: str, schema: str | None) -> bool:
        try:
            from sqlalchemy import inspect

            return inspect(conn).has_table(table, schema=schema)
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    def _delete_existing(conn, table, schema, key, keys) -> None:
        qualified = f"{schema}.{table}" if schema else table
        # Chunk the delete to keep the IN-list manageable.
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            placeholders = ", ".join(f":k{j}" for j in range(len(batch)))
            params = {f"k{j}": v for j, v in enumerate(batch)}
            conn.execute(text(f"DELETE FROM {qualified} WHERE {key} IN ({placeholders})"), params)

    def test_connection(self) -> bool:
        try:
            engine = self._engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return True
        except Exception:  # noqa: BLE001
            return False
