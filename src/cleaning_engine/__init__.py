"""
Enterprise Cleaning Engine

A production-ready, DRY, 100M × 900-scale data cleaning engine built with:
- Polars for performance
- YAML + Pydantic for configurable rules
- Pandera for contracts
- OpenTelemetry for observability
- DuckDB for storage
"""

__version__ = "0.1.0"

from cleaning_engine.engine import CleaningEngine
from cleaning_engine.rules import CleaningRule, RuleConfig
from cleaning_engine.storage import DuckDBStorage

__all__ = ["CleaningEngine", "CleaningRule", "RuleConfig", "DuckDBStorage"]
