"""Tests for DuckDB storage functionality."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from cleaning_engine.storage import DuckDBStorage


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Create a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "value": [10.5, 20.3, 15.7, 30.2, 25.1],
        }
    )


def test_in_memory_storage() -> None:
    """Test in-memory DuckDB storage."""
    storage = DuckDBStorage()
    storage.connect()

    assert storage.connection is not None
    assert storage.db_path == ":memory:"

    storage.close()
    assert storage.connection is None


def test_file_based_storage() -> None:
    """Test file-based DuckDB storage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        storage = DuckDBStorage(db_path)
        storage.connect()

        assert storage.connection is not None
        assert Path(storage.db_path).exists()

        storage.close()


def test_context_manager() -> None:
    """Test DuckDBStorage as context manager."""
    with DuckDBStorage() as storage:
        assert storage.connection is not None

    assert storage.connection is None


def test_save_and_load_dataframe(sample_df: pl.DataFrame) -> None:
    """Test saving and loading DataFrames."""
    with DuckDBStorage() as storage:
        # Save DataFrame
        storage.save_dataframe(sample_df, "test_table")

        # Load DataFrame
        loaded_df = storage.load_dataframe("test_table")

        # Compare
        assert loaded_df.shape == sample_df.shape
        assert loaded_df.columns == sample_df.columns
        assert loaded_df["id"].to_list() == sample_df["id"].to_list()


def test_save_replace_mode(sample_df: pl.DataFrame) -> None:
    """Test replace mode when saving DataFrame."""
    with DuckDBStorage() as storage:
        storage.save_dataframe(sample_df, "test_table")

        # Save again with different data
        new_df = pl.DataFrame({"id": [10], "name": ["New"]})
        storage.save_dataframe(new_df, "test_table", if_exists="replace")

        loaded_df = storage.load_dataframe("test_table")
        assert len(loaded_df) == 1
        assert loaded_df["id"][0] == 10


def test_save_fail_mode(sample_df: pl.DataFrame) -> None:
    """Test fail mode when saving DataFrame."""
    with DuckDBStorage() as storage:
        storage.save_dataframe(sample_df, "test_table")

        # Try to save again with fail mode
        with pytest.raises(ValueError):
            storage.save_dataframe(sample_df, "test_table", if_exists="fail")


def test_query(sample_df: pl.DataFrame) -> None:
    """Test SQL query execution."""
    with DuckDBStorage() as storage:
        storage.save_dataframe(sample_df, "test_table")

        # Execute query
        result = storage.query("SELECT * FROM test_table WHERE value > 20")

        assert len(result) == 3  # Should have 3 rows with value > 20 (20.3, 30.2, 25.1)


def test_execute(sample_df: pl.DataFrame) -> None:
    """Test SQL statement execution."""
    with DuckDBStorage() as storage:
        storage.save_dataframe(sample_df, "test_table")

        # Execute statement
        storage.execute("UPDATE test_table SET value = 100 WHERE id = 1")

        # Verify
        result = storage.load_dataframe("test_table")
        assert result.filter(pl.col("id") == 1)["value"][0] == 100


def test_list_tables(sample_df: pl.DataFrame) -> None:
    """Test listing tables."""
    with DuckDBStorage() as storage:
        assert len(storage.list_tables()) == 0

        storage.save_dataframe(sample_df, "table1")
        storage.save_dataframe(sample_df, "table2")

        tables = storage.list_tables()
        assert len(tables) == 2
        assert "table1" in tables
        assert "table2" in tables


def test_load_with_limit(sample_df: pl.DataFrame) -> None:
    """Test loading DataFrame with row limit."""
    with DuckDBStorage() as storage:
        storage.save_dataframe(sample_df, "test_table")

        # Load with limit
        limited_df = storage.load_dataframe("test_table", limit=3)

        assert len(limited_df) == 3


def test_not_connected_error() -> None:
    """Test error when operating without connection."""
    storage = DuckDBStorage()

    with pytest.raises(RuntimeError):
        storage.save_dataframe(pl.DataFrame({"a": [1]}), "test")

    with pytest.raises(RuntimeError):
        storage.load_dataframe("test")

    with pytest.raises(RuntimeError):
        storage.query("SELECT 1")

    with pytest.raises(RuntimeError):
        storage.execute("SELECT 1")

    with pytest.raises(RuntimeError):
        storage.list_tables()
