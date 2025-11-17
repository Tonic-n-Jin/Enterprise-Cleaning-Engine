"""
DuckDB storage layer for efficient data persistence and querying.
"""

from pathlib import Path
from typing import Optional, Union

import duckdb
import polars as pl
from opentelemetry import trace

tracer = trace.get_tracer(__name__)


class DuckDBStorage:
    """
    DuckDB storage backend for the cleaning engine.
    Provides efficient columnar storage and SQL querying capabilities.
    """

    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        """
        Initialize DuckDB storage.

        Args:
            db_path: Path to DuckDB file. If None, uses in-memory database.
        """
        self.db_path = str(db_path) if db_path else ":memory:"
        self.connection: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self) -> "DuckDBStorage":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        """Context manager exit."""
        self.close()

    def connect(self) -> None:
        """Establish connection to DuckDB."""
        with tracer.start_as_current_span("duckdb.connect"):
            self.connection = duckdb.connect(self.db_path)

    def close(self) -> None:
        """Close DuckDB connection."""
        if self.connection:
            with tracer.start_as_current_span("duckdb.close"):
                self.connection.close()
                self.connection = None

    def save_dataframe(
        self, df: pl.DataFrame, table_name: str, if_exists: str = "replace"
    ) -> None:
        """
        Save a Polars DataFrame to DuckDB.

        Args:
            df: Polars DataFrame to save
            table_name: Name of the table
            if_exists: Action if table exists ('replace', 'append', 'fail')
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        with tracer.start_as_current_span(
            "duckdb.save_dataframe", attributes={"table_name": table_name, "rows": len(df)}
        ):
            # Convert Polars DataFrame to Arrow for efficient transfer
            arrow_table = df.to_arrow()

            if if_exists == "replace":
                self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            elif if_exists == "fail":
                # Check if table exists
                result = self.connection.execute(
                    f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
                ).fetchone()
                if result and result[0] > 0:
                    raise ValueError(f"Table {table_name} already exists")

            # Create or append to table
            self.connection.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM arrow_table")
            if if_exists == "append":
                self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")

    def load_dataframe(self, table_name: str, limit: Optional[int] = None) -> pl.DataFrame:
        """
        Load a table as a Polars DataFrame.

        Args:
            table_name: Name of the table to load
            limit: Optional row limit

        Returns:
            Polars DataFrame
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        with tracer.start_as_current_span(
            "duckdb.load_dataframe", attributes={"table_name": table_name}
        ):
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"

            # Use Arrow for efficient transfer
            arrow_table = self.connection.execute(query).fetch_arrow_table()
            return pl.from_arrow(arrow_table)

    def query(self, sql: str) -> pl.DataFrame:
        """
        Execute a SQL query and return results as Polars DataFrame.

        Args:
            sql: SQL query to execute

        Returns:
            Query results as Polars DataFrame
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        with tracer.start_as_current_span("duckdb.query"):
            arrow_table = self.connection.execute(sql).fetch_arrow_table()
            return pl.from_arrow(arrow_table)

    def execute(self, sql: str) -> None:
        """
        Execute a SQL statement without returning results.

        Args:
            sql: SQL statement to execute
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        with tracer.start_as_current_span("duckdb.execute"):
            self.connection.execute(sql)

    def list_tables(self) -> list[str]:
        """
        List all tables in the database.

        Returns:
            List of table names
        """
        if not self.connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        result = self.connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        return [row[0] for row in result]
