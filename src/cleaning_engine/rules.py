"""
Configuration models for cleaning rules using Pydantic.
Provides type-safe, validated configuration loaded from YAML.
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class CleaningOperation(str, Enum):
    """Supported cleaning operations."""

    DROP_NULLS = "drop_nulls"
    FILL_NULLS = "fill_nulls"
    DROP_DUPLICATES = "drop_duplicates"
    TRIM_WHITESPACE = "trim_whitespace"
    LOWERCASE = "lowercase"
    UPPERCASE = "uppercase"
    REPLACE = "replace"
    CAST_TYPE = "cast_type"
    FILTER = "filter"
    REMOVE_OUTLIERS = "remove_outliers"
    STANDARDIZE = "standardize"
    VALIDATE = "validate"


class ColumnSelector(BaseModel):
    """Define which columns an operation applies to."""

    columns: Optional[List[str]] = Field(
        None, description="Specific column names (mutually exclusive with pattern)"
    )
    pattern: Optional[str] = Field(
        None, description="Regex pattern to match column names (mutually exclusive with columns)"
    )
    all: bool = Field(False, description="Apply to all columns")

    @model_validator(mode="after")
    def validate_selector(self) -> "ColumnSelector":
        """Ensure only one selection method is used."""
        if self.pattern and self.columns:
            raise ValueError("Cannot specify both 'columns' and 'pattern'")
        if self.pattern and self.all:
            raise ValueError("Cannot specify both 'pattern' and 'all'")
        if self.columns and self.all:
            raise ValueError("Cannot specify both 'columns' and 'all'")
        return self


class CleaningRule(BaseModel):
    """A single cleaning rule with operation and parameters."""

    name: str = Field(..., description="Human-readable name for the rule")
    operation: CleaningOperation = Field(..., description="The cleaning operation to perform")
    columns: ColumnSelector = Field(
        default_factory=lambda: ColumnSelector(all=True),
        description="Column selector for the operation",
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict, description="Operation-specific parameters"
    )
    enabled: bool = Field(True, description="Whether this rule is active")
    order: int = Field(0, description="Execution order (lower numbers execute first)")


class DataContract(BaseModel):
    """Pandera-compatible data contract definition."""

    columns: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Column schemas with constraints"
    )
    strict: bool = Field(True, description="Whether to enforce strict schema")
    coerce: bool = Field(False, description="Whether to coerce types")


class RuleConfig(BaseModel):
    """Complete configuration for the cleaning engine."""

    version: str = Field("1.0", description="Configuration schema version")
    name: str = Field(..., description="Name of this cleaning configuration")
    description: Optional[str] = Field(None, description="Description of the cleaning process")
    
    input_contract: Optional[DataContract] = Field(
        None, description="Expected input data schema"
    )
    output_contract: Optional[DataContract] = Field(
        None, description="Expected output data schema"
    )
    
    rules: List[CleaningRule] = Field(..., description="List of cleaning rules to apply")
    
    observability: Dict[str, Any] = Field(
        default_factory=lambda: {"enabled": True, "service_name": "cleaning-engine"},
        description="OpenTelemetry configuration",
    )

    @field_validator("rules")
    @classmethod
    def sort_rules_by_order(cls, v: List[CleaningRule]) -> List[CleaningRule]:
        """Ensure rules are sorted by execution order."""
        return sorted(v, key=lambda r: r.order)

    model_config = {"use_enum_values": True}
