# MOCK

## MOCK SCHEMA

* If you want to create new table schema or add new data type, edit mock_schema.py then run command `make` in current folder. mock_schema.py will create *.json in testdata folder which read by mock_data.go

**Warning**: Don't modify testdata/generated_*.json directly, it will be overwritten.

## MOCK DATA

* If you want to create data from schema, use script named `mock_from_testdata.py` with type "data"

## FINALLY

* If you want to update schema, and run tests in `.ci`, just run `bash mock.sh`