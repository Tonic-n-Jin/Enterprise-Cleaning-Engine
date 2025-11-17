"""
Main cleaning engine implementation.
Orchestrates the execution of cleaning rules on Polars DataFrames.
"""

from pathlib import Path
from typing import Optional, Union

import polars as pl
import yaml
from opentelemetry import trace

from cleaning_engine.observability import setup_observability
from cleaning_engine.operations import OPERATIONS, select_columns
from cleaning_engine.rules import RuleConfig
from cleaning_engine.storage import DuckDBStorage
from cleaning_engine.validation import SchemaValidator

tracer = trace.get_tracer(__name__)


class CleaningEngine:
    """
    Production-ready data cleaning engine.

    Features:
    - Polars-based high-performance processing (100M × 900 scale)
    - YAML + Pydantic configurable rules
    - Pandera data contracts
    - OpenTelemetry observability
    - DuckDB storage integration
    """

    def __init__(
        self,
        config: Optional[Union[str, Path, RuleConfig]] = None,
        storage: Optional[DuckDBStorage] = None,
        enable_observability: bool = True,
    ):
        """
        Initialize the cleaning engine.

        Args:
            config: Path to YAML config file or RuleConfig object
            storage: Optional DuckDBStorage instance
            enable_observability: Whether to enable OpenTelemetry tracing
        """
        self.config: Optional[RuleConfig] = None
        self.storage = storage
        self.validator = SchemaValidator()

        if config:
            self.load_config(config)

        if enable_observability and self.config:
            service_name = self.config.observability.get("service_name", "cleaning-engine")
            console_export = self.config.observability.get("console_export", True)
            setup_observability(service_name=service_name, console_export=console_export)

    def load_config(self, config: Union[str, Path, RuleConfig]) -> None:
        """
        Load cleaning configuration.

        Args:
            config: Path to YAML file or RuleConfig object
        """
        with tracer.start_as_current_span("engine.load_config"):
            if isinstance(config, RuleConfig):
                self.config = config
            else:
                config_path = Path(config)
                with open(config_path, "r") as f:
                    config_dict = yaml.safe_load(f)
                self.config = RuleConfig(**config_dict)

    def clean(
        self,
        df: pl.DataFrame,
        validate_input: bool = True,
        validate_output: bool = True,
    ) -> pl.DataFrame:
        """
        Apply cleaning rules to a DataFrame.

        Args:
            df: Input DataFrame to clean
            validate_input: Whether to validate against input contract
            validate_output: Whether to validate against output contract

        Returns:
            Cleaned DataFrame

        Raises:
            ValueError: If config is not loaded
        """
        if not self.config:
            raise ValueError("Configuration not loaded. Call load_config() first.")

        with tracer.start_as_current_span(
            "engine.clean",
            attributes={
                "input_rows": len(df),
                "input_columns": len(df.columns),
                "rules_count": len(self.config.rules),
            },
        ) as span:
            # Validate input contract
            if validate_input and self.config.input_contract:
                df = self.validator.validate(df, self.config.input_contract, "input")

            # Apply each cleaning rule
            for rule in self.config.rules:
                if not rule.enabled:
                    continue

                df = self._apply_rule(df, rule)

            # Validate output contract
            if validate_output and self.config.output_contract:
                df = self.validator.validate(df, self.config.output_contract, "output")

            span.set_attribute("output_rows", len(df))
            span.set_attribute("output_columns", len(df.columns))

            return df

    def _apply_rule(self, df: pl.DataFrame, rule) -> pl.DataFrame:  # type: ignore
        """
        Apply a single cleaning rule.

        Args:
            df: DataFrame to clean
            rule: CleaningRule to apply

        Returns:
            Cleaned DataFrame
        """
        with tracer.start_as_current_span(
            "engine.apply_rule",
            attributes={"rule_name": rule.name, "operation": rule.operation},
        ):
            # Get the operation function
            operation = OPERATIONS.get(rule.operation)
            if not operation:
                raise ValueError(f"Unknown operation: {rule.operation}")

            # Select columns
            columns = select_columns(df, rule.columns)

            # Apply operation
            return operation(df, columns, **rule.parameters)

    def clean_from_storage(
        self,
        table_name: str,
        output_table: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Clean data directly from DuckDB storage.

        Args:
            table_name: Source table name
            output_table: Optional output table name (if None, returns DataFrame)
            batch_size: Optional batch size for processing large datasets

        Returns:
            Cleaned DataFrame
        """
        if not self.storage:
            raise ValueError("Storage not configured. Provide DuckDBStorage instance.")

        with tracer.start_as_current_span("engine.clean_from_storage"):
            # Load data
            df = self.storage.load_dataframe(table_name)

            # Clean
            cleaned_df = self.clean(df)

            # Save if output table specified
            if output_table:
                self.storage.save_dataframe(cleaned_df, output_table)

            return cleaned_df

    def infer_contracts(self, df: pl.DataFrame) -> None:
        """
        Infer and set input/output contracts from a sample DataFrame.

        Args:
            df: Sample DataFrame to infer contracts from
        """
        if not self.config:
            raise ValueError("Configuration not loaded.")

        with tracer.start_as_current_span("engine.infer_contracts"):
            inferred = self.validator.infer_contract_from_dataframe(df)
            if not self.config.input_contract:
                self.config.input_contract = inferred
            if not self.config.output_contract:
                self.config.output_contract = inferred

    def save_config(self, path: Union[str, Path]) -> None:
        """
        Save current configuration to YAML file.

        Args:
            path: Output file path
        """
        if not self.config:
            raise ValueError("Configuration not loaded.")

        with tracer.start_as_current_span("engine.save_config"):
            config_dict = self.config.model_dump(mode="json")
            with open(path, "w") as f:
                yaml.safe_dump(config_dict, f, default_flow_style=False, sort_keys=False)
