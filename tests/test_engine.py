"""Tests for the cleaning engine core functionality."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from cleaning_engine import CleaningEngine, RuleConfig
from cleaning_engine.rules import CleaningOperation, CleaningRule, ColumnSelector


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Create a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["  Alice  ", "Bob", None, "CHARLIE", "david"],
            "age": [25, 30, 35, None, 28],
            "score": [85.5, 90.0, 78.5, 88.0, 92.5],
        }
    )


@pytest.fixture
def basic_config() -> RuleConfig:
    """Create a basic cleaning configuration."""
    return RuleConfig(
        name="basic_cleaning",
        description="Basic data cleaning rules",
        rules=[
            CleaningRule(
                name="trim_names",
                operation=CleaningOperation.TRIM_WHITESPACE,
                columns=ColumnSelector(columns=["name"]),
                order=0,
            ),
            CleaningRule(
                name="lowercase_names",
                operation=CleaningOperation.LOWERCASE,
                columns=ColumnSelector(columns=["name"]),
                order=1,
            ),
            CleaningRule(
                name="fill_missing_age",
                operation=CleaningOperation.FILL_NULLS,
                columns=ColumnSelector(columns=["age"]),
                parameters={"strategy": "mean"},
                order=2,
            ),
        ],
    )


def test_engine_initialization() -> None:
    """Test CleaningEngine initialization."""
    engine = CleaningEngine()
    assert engine.config is None
    assert engine.storage is None


def test_load_config_from_object(basic_config: RuleConfig) -> None:
    """Test loading config from RuleConfig object."""
    engine = CleaningEngine(config=basic_config, enable_observability=False)
    assert engine.config is not None
    assert engine.config.name == "basic_cleaning"
    assert len(engine.config.rules) == 3


def test_basic_cleaning(sample_df: pl.DataFrame, basic_config: RuleConfig) -> None:
    """Test basic cleaning operations."""
    engine = CleaningEngine(config=basic_config, enable_observability=False)
    cleaned = engine.clean(sample_df, validate_input=False, validate_output=False)

    # Check trimming and lowercase
    assert cleaned["name"][0] == "alice"
    assert cleaned["name"][1] == "bob"
    assert cleaned["name"][3] == "charlie"
    assert cleaned["name"][4] == "david"

    # Check null filling - age should have no nulls
    assert cleaned["age"].null_count() == 0


def test_drop_nulls_operation() -> None:
    """Test drop_nulls operation."""
    df = pl.DataFrame({"a": [1, 2, None, 4], "b": [5, None, 7, 8]})

    config = RuleConfig(
        name="drop_nulls_test",
        rules=[
            CleaningRule(
                name="drop_nulls",
                operation=CleaningOperation.DROP_NULLS,
                columns=ColumnSelector(columns=["a"]),
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert len(cleaned) == 3  # One row with null in 'a' should be dropped
    assert cleaned["a"].null_count() == 0


def test_drop_duplicates_operation() -> None:
    """Test drop_duplicates operation."""
    df = pl.DataFrame({"a": [1, 2, 2, 3, 3], "b": [4, 5, 5, 6, 7]})

    config = RuleConfig(
        name="drop_duplicates_test",
        rules=[
            CleaningRule(
                name="drop_dupes",
                operation=CleaningOperation.DROP_DUPLICATES,
                columns=ColumnSelector(columns=["a"]),
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert len(cleaned) == 3  # Should have unique values in 'a'


def test_replace_operation() -> None:
    """Test replace operation."""
    df = pl.DataFrame({"text": ["hello", "world", "hello", "test"]})

    config = RuleConfig(
        name="replace_test",
        rules=[
            CleaningRule(
                name="replace_hello",
                operation=CleaningOperation.REPLACE,
                columns=ColumnSelector(columns=["text"]),
                parameters={"value": "hello", "replacement": "hi"},
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert cleaned["text"][0] == "hi"
    assert cleaned["text"][2] == "hi"
    assert cleaned["text"][1] == "world"


def test_cast_type_operation() -> None:
    """Test cast_type operation."""
    df = pl.DataFrame({"numbers": ["1", "2", "3", "4"]})

    config = RuleConfig(
        name="cast_test",
        rules=[
            CleaningRule(
                name="cast_to_int",
                operation=CleaningOperation.CAST_TYPE,
                columns=ColumnSelector(columns=["numbers"]),
                parameters={"dtype": "int"},
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert cleaned["numbers"].dtype == pl.Int64


def test_column_selector_all() -> None:
    """Test column selector with all=True."""
    df = pl.DataFrame({"a": ["X", "Y"], "b": ["Z", "W"]})

    config = RuleConfig(
        name="selector_test",
        rules=[
            CleaningRule(
                name="lowercase_all",
                operation=CleaningOperation.LOWERCASE,
                columns=ColumnSelector(all=True),
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert cleaned["a"][0] == "x"
    assert cleaned["b"][0] == "z"


def test_column_selector_pattern() -> None:
    """Test column selector with regex pattern."""
    df = pl.DataFrame({"col_1": ["A"], "col_2": ["B"], "other": ["C"]})

    config = RuleConfig(
        name="pattern_test",
        rules=[
            CleaningRule(
                name="lowercase_cols",
                operation=CleaningOperation.LOWERCASE,
                columns=ColumnSelector(pattern=r"col_\d+"),
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert cleaned["col_1"][0] == "a"
    assert cleaned["col_2"][0] == "b"
    assert cleaned["other"][0] == "C"  # Should not be changed


def test_rule_order() -> None:
    """Test that rules are executed in order."""
    df = pl.DataFrame({"text": ["  HELLO  "]})

    config = RuleConfig(
        name="order_test",
        rules=[
            CleaningRule(
                name="lowercase",
                operation=CleaningOperation.LOWERCASE,
                columns=ColumnSelector(columns=["text"]),
                order=0,
            ),
            CleaningRule(
                name="trim",
                operation=CleaningOperation.TRIM_WHITESPACE,
                columns=ColumnSelector(columns=["text"]),
                order=1,
            ),
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert cleaned["text"][0] == "hello"


def test_disabled_rule() -> None:
    """Test that disabled rules are not executed."""
    df = pl.DataFrame({"text": ["HELLO"]})

    config = RuleConfig(
        name="disabled_test",
        rules=[
            CleaningRule(
                name="lowercase",
                operation=CleaningOperation.LOWERCASE,
                columns=ColumnSelector(columns=["text"]),
                enabled=False,
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert cleaned["text"][0] == "HELLO"  # Should not be changed


def test_save_and_load_config(basic_config: RuleConfig) -> None:
    """Test saving and loading configuration from YAML."""
    engine = CleaningEngine(config=basic_config, enable_observability=False)

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.yaml"
        engine.save_config(config_path)

        # Load the config
        engine2 = CleaningEngine(config=config_path, enable_observability=False)
        assert engine2.config is not None
        assert engine2.config.name == "basic_cleaning"
        assert len(engine2.config.rules) == 3


def test_filter_operation() -> None:
    """Test filter operation."""
    df = pl.DataFrame({"value": [1, 2, 3, 4, 5]})

    config = RuleConfig(
        name="filter_test",
        rules=[
            CleaningRule(
                name="filter_greater_than_2",
                operation=CleaningOperation.FILTER,
                columns=ColumnSelector(columns=["value"]),
                parameters={"operator": ">", "value": 2},
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    assert len(cleaned) == 3
    assert cleaned["value"].min() > 2


def test_standardize_operation() -> None:
    """Test standardize operation."""
    df = pl.DataFrame({"values": [10.0, 20.0, 30.0, 40.0, 50.0]})

    config = RuleConfig(
        name="standardize_test",
        rules=[
            CleaningRule(
                name="standardize_values",
                operation=CleaningOperation.STANDARDIZE,
                columns=ColumnSelector(columns=["values"]),
            )
        ],
    )

    engine = CleaningEngine(config=config, enable_observability=False)
    cleaned = engine.clean(df, validate_input=False, validate_output=False)

    # Standardized values should have mean ≈ 0 and std ≈ 1
    mean = cleaned["values"].mean()
    std = cleaned["values"].std()

    assert abs(mean) < 1e-10  # type: ignore
    assert abs(std - 1.0) < 1e-10  # type: ignore
