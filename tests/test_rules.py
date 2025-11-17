"""Tests for Pydantic rule configuration."""

import pytest
from pydantic import ValidationError

from cleaning_engine.rules import (
    CleaningOperation,
    CleaningRule,
    ColumnSelector,
    DataContract,
    RuleConfig,
)


def test_column_selector_columns() -> None:
    """Test ColumnSelector with specific columns."""
    selector = ColumnSelector(columns=["a", "b", "c"])
    assert selector.columns == ["a", "b", "c"]
    assert selector.pattern is None
    assert selector.all is False


def test_column_selector_pattern() -> None:
    """Test ColumnSelector with regex pattern."""
    selector = ColumnSelector(pattern=r"col_\d+")
    assert selector.pattern == r"col_\d+"
    assert selector.columns is None
    assert selector.all is False


def test_column_selector_all() -> None:
    """Test ColumnSelector with all flag."""
    selector = ColumnSelector(all=True)
    assert selector.all is True
    assert selector.columns is None
    assert selector.pattern is None


def test_column_selector_mutual_exclusion() -> None:
    """Test that column selectors are mutually exclusive."""
    with pytest.raises(ValidationError):
        ColumnSelector(columns=["a"], pattern=r"b.*")

    with pytest.raises(ValidationError):
        ColumnSelector(pattern=r"a.*", all=True)

    with pytest.raises(ValidationError):
        ColumnSelector(columns=["a"], all=True)


def test_cleaning_rule_basic() -> None:
    """Test basic CleaningRule creation."""
    rule = CleaningRule(
        name="test_rule",
        operation=CleaningOperation.DROP_NULLS,
        columns=ColumnSelector(columns=["col1"]),
    )

    assert rule.name == "test_rule"
    assert rule.operation == CleaningOperation.DROP_NULLS
    assert rule.enabled is True
    assert rule.order == 0
    assert rule.parameters == {}


def test_cleaning_rule_with_parameters() -> None:
    """Test CleaningRule with parameters."""
    rule = CleaningRule(
        name="fill_rule",
        operation=CleaningOperation.FILL_NULLS,
        columns=ColumnSelector(columns=["age"]),
        parameters={"strategy": "mean"},
        order=5,
    )

    assert rule.parameters == {"strategy": "mean"}
    assert rule.order == 5


def test_cleaning_operation_enum() -> None:
    """Test CleaningOperation enum values."""
    assert CleaningOperation.DROP_NULLS == "drop_nulls"
    assert CleaningOperation.FILL_NULLS == "fill_nulls"
    assert CleaningOperation.LOWERCASE == "lowercase"
    assert CleaningOperation.REPLACE == "replace"


def test_data_contract_basic() -> None:
    """Test DataContract creation."""
    contract = DataContract(
        columns={
            "id": {"dtype": "Int64", "nullable": False},
            "name": {"dtype": "Utf8", "nullable": True},
        }
    )

    assert "id" in contract.columns
    assert "name" in contract.columns
    assert contract.strict is True
    assert contract.coerce is False


def test_rule_config_basic() -> None:
    """Test RuleConfig creation."""
    config = RuleConfig(
        name="test_config",
        description="Test configuration",
        rules=[
            CleaningRule(
                name="rule1",
                operation=CleaningOperation.DROP_NULLS,
            )
        ],
    )

    assert config.name == "test_config"
    assert config.description == "Test configuration"
    assert len(config.rules) == 1
    assert config.version == "1.0"


def test_rule_config_sorting() -> None:
    """Test that rules are sorted by order."""
    config = RuleConfig(
        name="sorted_config",
        rules=[
            CleaningRule(name="rule3", operation=CleaningOperation.DROP_NULLS, order=3),
            CleaningRule(name="rule1", operation=CleaningOperation.DROP_NULLS, order=1),
            CleaningRule(name="rule2", operation=CleaningOperation.DROP_NULLS, order=2),
        ],
    )

    assert config.rules[0].name == "rule1"
    assert config.rules[1].name == "rule2"
    assert config.rules[2].name == "rule3"


def test_rule_config_with_contracts() -> None:
    """Test RuleConfig with data contracts."""
    config = RuleConfig(
        name="contract_config",
        input_contract=DataContract(
            columns={"id": {"dtype": "Int64", "nullable": False}}
        ),
        output_contract=DataContract(
            columns={"id": {"dtype": "Int64", "nullable": False}}
        ),
        rules=[
            CleaningRule(
                name="rule1",
                operation=CleaningOperation.DROP_NULLS,
            )
        ],
    )

    assert config.input_contract is not None
    assert config.output_contract is not None
    assert "id" in config.input_contract.columns
    assert "id" in config.output_contract.columns


def test_rule_config_observability() -> None:
    """Test RuleConfig observability settings."""
    config = RuleConfig(
        name="obs_config",
        rules=[
            CleaningRule(name="rule1", operation=CleaningOperation.DROP_NULLS)
        ],
        observability={"enabled": True, "service_name": "custom-service"},
    )

    assert config.observability["enabled"] is True
    assert config.observability["service_name"] == "custom-service"


def test_rule_config_default_observability() -> None:
    """Test RuleConfig default observability settings."""
    config = RuleConfig(
        name="default_obs",
        rules=[
            CleaningRule(name="rule1", operation=CleaningOperation.DROP_NULLS)
        ],
    )

    assert config.observability["enabled"] is True
    assert config.observability["service_name"] == "cleaning-engine"


def test_cleaning_rule_validation() -> None:
    """Test CleaningRule requires name and operation."""
    with pytest.raises(ValidationError):
        CleaningRule(operation=CleaningOperation.DROP_NULLS)  # type: ignore

    with pytest.raises(ValidationError):
        CleaningRule(name="test")  # type: ignore


def test_rule_config_validation() -> None:
    """Test RuleConfig requires name and rules."""
    with pytest.raises(ValidationError):
        RuleConfig(rules=[])  # type: ignore

    with pytest.raises(ValidationError):
        RuleConfig(  # type: ignore
            name="test"
        )  # Missing rules
