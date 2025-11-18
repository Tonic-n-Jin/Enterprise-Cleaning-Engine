# Enterprise Cleaning Engine

A **production-ready, DRY, 100M × 900-scale data cleaning engine** built with modern open-source tools.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Features

- ⚡ **High Performance**: Built on [Polars](https://www.pola.rs/) for blazing-fast columnar data processing
- 📋 **Declarative Configuration**: YAML-based rules with [Pydantic](https://docs.pydantic.dev/) validation
- 🛡️ **Data Contracts**: [Pandera](https://pandera.readthedocs.io/) schema validation for inputs/outputs
- 📊 **Observability**: Full [OpenTelemetry](https://opentelemetry.io/) instrumentation
- 💾 **Efficient Storage**: [DuckDB](https://duckdb.org/) integration for SQL querying and persistence
- 🧪 **Testable**: Pure functions, dependency injection, comprehensive test suite
- 🔧 **Extensible**: Plugin-style operations, easy to add custom cleaning logic
- 📈 **Scalable**: Designed and tested for 100M rows × 900 columns

## 📦 Installation

```bash
# Using Poetry (recommended)
poetry install

# Using pip
pip install -e .
```

## 🎯 Quick Start

### Basic Usage

```python
import polars as pl
from cleaning_engine import CleaningEngine

# Load your data
df = pl.DataFrame({
    "email": ["  ALICE@TEST.COM  ", "bob@test.com", None],
    "age": [25, 30, None],
})

# Load cleaning configuration
engine = CleaningEngine(config="config.yaml")

# Clean your data
cleaned_df = engine.clean(df)
```

### YAML Configuration

```yaml
version: "1.0"
name: "my_cleaning_pipeline"
description: "Clean customer data"

rules:
  - name: "trim_whitespace"
    operation: "trim_whitespace"
    columns:
      columns: ["email"]
    order: 0

  - name: "lowercase_emails"
    operation: "lowercase"
    columns:
      columns: ["email"]
    order: 1

  - name: "fill_missing_age"
    operation: "fill_nulls"
    columns:
      columns: ["age"]
    parameters:
      strategy: "mean"
    order: 2
```

### Using DuckDB Storage

```python
from cleaning_engine import CleaningEngine, DuckDBStorage

# Create storage backend
with DuckDBStorage("data.duckdb") as storage:
    # Save raw data
    storage.save_dataframe(df, "raw_data")
    
    # Clean from storage
    engine = CleaningEngine(config="config.yaml", storage=storage)
    cleaned = engine.clean_from_storage("raw_data", "cleaned_data")
```

## 📚 Documentation

### Supported Operations

The engine supports the following cleaning operations:

| Operation | Description | Parameters |
|-----------|-------------|------------|
| `drop_nulls` | Remove rows with null values | - |
| `fill_nulls` | Fill null values | `value`, `strategy` (value, forward, backward, mean, median) |
| `drop_duplicates` | Remove duplicate rows | `maintain_order` |
| `trim_whitespace` | Trim whitespace from strings | - |
| `lowercase` | Convert strings to lowercase | - |
| `uppercase` | Convert strings to uppercase | - |
| `replace` | Replace values | `pattern`, `value`, `replacement` |
| `cast_type` | Cast column types | `dtype`, `strict` |
| `filter` | Filter rows by condition | `operator`, `value` |
| `remove_outliers` | Remove statistical outliers | `method` (iqr, zscore), `threshold` |
| `standardize` | Z-score normalization | - |

### Column Selectors

Select columns using three methods:

```yaml
# Specific columns
columns:
  columns: ["col1", "col2"]

# Regex pattern
columns:
  pattern: "^num_.*"

# All columns
columns:
  all: true
```

### Data Contracts

Define input and output schemas with Pandera:

```yaml
input_contract:
  strict: true
  coerce: false
  columns:
    customer_id:
      dtype: Int64
      nullable: false
      min: 1
    email:
      dtype: Utf8
      nullable: true
      regex: "^[a-z0-9]+@[a-z]+\\.[a-z]{2,}$"
```

### Observability

OpenTelemetry tracing is built-in:

```yaml
observability:
  enabled: true
  service_name: "my-cleaning-service"
  console_export: true
```

All operations emit spans with relevant attributes (row counts, column names, etc.).

## 🧪 Testing

```bash
# Run tests with coverage
poetry run pytest

# Run specific test file
poetry run pytest tests/test_engine.py

# Run with verbose output
poetry run pytest -v
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   CleaningEngine                        │
├─────────────────────────────────────────────────────────┤
│  • Orchestrates cleaning pipeline                      │
│  • Validates input/output contracts                    │
│  • Emits OpenTelemetry traces                          │
└───────────────┬─────────────────────────────────────────┘
                │
        ┌───────┴────────┐
        │                │
┌───────▼──────┐  ┌──────▼──────┐
│   Polars     │  │   DuckDB    │
│  Processing  │  │   Storage   │
└──────────────┘  └─────────────┘
```

### Stack

- **Polars**: In-memory data processing (100M+ rows)
- **DuckDB**: SQL queries and out-of-core processing
- **Pydantic**: Type-safe configuration validation
- **Pandera**: Data schema validation and contracts
- **OpenTelemetry**: Distributed tracing and metrics

## 🎨 Design Principles

1. **DRY (Don't Repeat Yourself)**: All operations are reusable functions
2. **Single Responsibility**: Each component has one job
3. **Testability**: Pure functions, dependency injection
4. **Extensibility**: Easy to add new operations
5. **Performance**: Leverages Polars' lazy evaluation and parallelism
6. **Observability**: Every operation is traced

## 📊 Performance

Designed and tested for enterprise scale:

- ✅ 100M rows × 900 columns
- ✅ Lazy evaluation for memory efficiency
- ✅ Parallel processing via Polars
- ✅ Out-of-core processing via DuckDB
- ✅ Sub-second operations on typical datasets

## 🔧 Extending

Add custom operations by registering functions:

```python
from cleaning_engine.operations import OPERATIONS

def my_custom_operation(df, columns, **params):
    # Your logic here
    return df

OPERATIONS["my_operation"] = my_custom_operation
```

## 📝 Examples

See the `examples/` directory for:

- `example_config.yaml`: Production-ready configuration
- `usage_example.py`: Comprehensive usage examples

Run examples:

```bash
cd examples
python usage_example.py
```

## 🤝 Contributing

Contributions welcome! This is an open-source project following best practices:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details

## 🙏 Acknowledgments

Built with amazing open-source tools:

- [Polars](https://www.pola.rs/) - Lightning-fast DataFrames
- [DuckDB](https://duckdb.org/) - Analytical SQL database
- [Pydantic](https://docs.pydantic.dev/) - Data validation
- [Pandera](https://pandera.readthedocs.io/) - Statistical data validation
- [OpenTelemetry](https://opentelemetry.io/) - Observability framework