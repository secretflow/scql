# SCQL Python Package

Secure Collaborative Query Language (SCQL) Python bindings for compiling SQL queries and executing them on secure multi-party computation engine.

## Overview

SCQL Python package provides two main components:

- **SCQL Compiler**: Translates SQL queries into secure multi-party computation execution plans
- **SCQL Engine**: Executes the compiled plans on secure multi-party computation engine

## Requirements

- Python 3.11
- Linux x86_64

## Installation

```bash
pip install scql
```

## Usage

```python
import scql

# Build catalog
catalog = scql.compiler.Catalog()
table = scql.compiler.TableEntry()
table.Name = "users"
table.Db = "test_db"
table.Owner = "alice"

# Add columns
col1 = scql.compiler.Column()
col1.Name = "id"
col1.Type = "INT64"
table.AddColumn(col1)

col2 = scql.compiler.Column()
col2.Name = "name"
col2.Type = "STRING"
table.AddColumn(col2)

catalog.AddTableEntry(table)

# Compile SQL query
inputs = scql.compiler.CompileInputs()
inputs.Query = "SELECT id FROM users WHERE name = 'alice'"
inputs.Issuer = "alice"
inputs.Db = "test_db"
inputs.Catalog = catalog

# Compile to execution plan
result = scql.compiler.Compile(inputs)
print(f"Operator graph: {result.OperatorGraph}")
print(f"Marshaled plan size: {len(result.MarshaledPlan)} bytes")

# Execute with engine (requires link context setup)
engine = scql.engine.Engine("alice", '{"router_type": "embed"}')
# execution_response = engine.run_plan(result.MarshaledPlan, link_context)
```

## Architecture

The SCQL Python package provides:

- **Compiler Module**: SQL query compilation and planning
- **Engine Module**: Secure multi-party computation execution
- **Unified Interface**: Single package with integrated functionality

## License

Apache License 2.0
