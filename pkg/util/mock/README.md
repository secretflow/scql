# MOCK

## MOCK SCHEMA

* If you want to create new table schema or add new data type, edit mock_schema.py then make in current folder. mock_schema.py will create *.json in testdata folder which read by mock_data.go

## MOCK DATA

* If you want to create data from schema, use script named `mock_from_testdata.py` with type "data"

## MOCK TOY GRM

* If you want to create grm conf from schema, use script named `mock_from_testdata.py` with type "grm"

## FINALLY

* If you want to update schema, and run tests in `.ci`, just run `bash mock.sh`. The script can update toy grm and create data in `.ci`