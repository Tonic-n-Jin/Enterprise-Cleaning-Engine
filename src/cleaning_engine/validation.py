"""
Pandera schema validation and data contracts.
"""

from typing import Any, Dict, Optional

import pandera.polars as pa
import polars as pl
from opentelemetry import trace

from cleaning_engine.rules import DataContract

tracer = trace.get_tracer(__name__)


class SchemaValidator:
    """
    Validates Polars DataFrames against Pandera schemas.
    Enforces data contracts for input/output validation.
    """

    @staticmethod
    def create_schema_from_contract(contract: DataContract) -> pa.DataFrameSchema:
        """
        Create a Pandera schema from a DataContract configuration.

        Args:
            contract: DataContract with column definitions

        Returns:
            Pandera DataFrameSchema
        """
        columns: Dict[str, pa.Column] = {}

        for col_name, col_spec in contract.columns.items():
            dtype = col_spec.get("dtype", pl.Utf8)
            nullable = col_spec.get("nullable", True)
            checks = []

            # Add checks based on specification
            if "min" in col_spec:
                checks.append(pa.Check.greater_than_or_equal_to(col_spec["min"]))
            if "max" in col_spec:
                checks.append(pa.Check.less_than_or_equal_to(col_spec["max"]))
            if "regex" in col_spec:
                checks.append(pa.Check.str_matches(col_spec["regex"]))
            if "isin" in col_spec:
                checks.append(pa.Check.isin(col_spec["isin"]))

            columns[col_name] = pa.Column(dtype, nullable=nullable, checks=checks)

        return pa.DataFrameSchema(columns=columns, strict=contract.strict, coerce=contract.coerce)

    @staticmethod
    def validate(
        df: pl.DataFrame, contract: Optional[DataContract], contract_name: str = "data"
    ) -> pl.DataFrame:
        """
        Validate a DataFrame against a contract.

        Args:
            df: DataFrame to validate
            contract: DataContract to validate against
            contract_name: Name for tracing/logging

        Returns:
            Validated DataFrame

        Raises:
            pa.errors.SchemaError: If validation fails
        """
        if contract is None:
            return df

        with tracer.start_as_current_span(
            "schema.validate",
            attributes={"contract": contract_name, "rows": len(df), "columns": len(df.columns)},
        ):
            schema = SchemaValidator.create_schema_from_contract(contract)
            return schema.validate(df)

    @staticmethod
    def infer_contract_from_dataframe(df: pl.DataFrame, strict: bool = True) -> DataContract:
        """
        Infer a DataContract from an existing DataFrame.

        Args:
            df: DataFrame to infer contract from
            strict: Whether to enforce strict schema

        Returns:
            Inferred DataContract
        """
        columns: Dict[str, Dict[str, Any]] = {}

        for col in df.columns:
            dtype = df[col].dtype
            has_nulls = df[col].null_count() > 0

            col_spec: Dict[str, Any] = {"dtype": dtype, "nullable": has_nulls}

            # Add numeric constraints
            if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
                col_spec["min"] = df[col].min()
                col_spec["max"] = df[col].max()

            columns[col] = col_spec

        return DataContract(columns=columns, strict=strict, coerce=False)
