import json
import os
import re
import argparse
from mock_schema import CUR_PATH
from mock_db_data import *

BACKEND_TYPE = {"alice": "MYSQL", "bob": "CSVDB", "carol": "POSTGRESQL"}


# create mock xxx.sql used in mysql initiating
def create_mock_data(source_file: str, rows: int, dest_dir: str):
    schema_info = parse_json(source_file)
    str_pool = create_str_pool(POOL_SIZE)
    db_name = schema_info.get("db_name")
    tables = schema_info.get("tables")
    contents = [""] * len(DB_TYPES)
    for table_name in tables:
        table = tables.get(table_name)
        columns = table.get("columns")
        table_str = create_table(table_name, columns)
        insert_str = create_insert(table_name, columns, str_pool, rows)
        for i, db_type in enumerate(DB_TYPES):
            contents[i] += table_str
            contents[i] += insert_str
            contents[i] += "\n\n\n\n"
        if IS_DB_USE_FILE["csv"][db_name]:
            column_strs = []
            for column in columns:
                column_strs.append(column["column_name"])
            csv_contents = ", ".join(column_strs)
            csv_contents += (
                insert_str.replace("INSERT INTO " + table_name + " VALUES (", "")
                .replace(");", "")
                .replace(", ", ",")
                .replace("'", '"')
            )
            with open(
                os.path.join(CUR_PATH, dest_dir, f"{db_name}_{table_name}.csv"),
                "w",
            ) as f:
                f.write(csv_contents)
    for i, db_type in enumerate(DB_TYPES):
        # replace float type in content
        # postgres use numeric, mysql use float
        contents[i] = (
            contents[i]
            .replace("FLOAT_TYPE", REPLACE_MAP[db_type]["FLOAT_TYPE"])
            .replace("DATETIME_TYPE", REPLACE_MAP[db_type]["DATETIME_TYPE"])
        )
        if IS_DB_USE_FILE[db_type][db_name]:
            with open(
                os.path.join(CUR_PATH, dest_dir, f"{db_type}_{SQL_FILE_LIST[db_name]}"),
                "w",
            ) as f:
                f.write(CREATE_DB_STR[i].format(db_name))
                f.write(contents[i])
        contents[i] = ""


def parse_json(source_file: str):
    result = {}
    with open(os.path.join(CUR_PATH, source_file), "r") as f:
        result = json.load(f)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parameters")
    parser.add_argument(
        "--dest_data",
        "-dd",
        type=str,
        help="destination dir",
        default="testdata",
    )
    parser.add_argument(
        "--source",
        "-s",
        type=str,
        help="source path",
        default="testdata/db_alice.json",
    )
    parser.add_argument("--rows", "-r", type=int, help="rows of table", default=600)
    args = vars(parser.parse_args())
    source = args["source"]
    rows = args["rows"]
    data_dest = args["dest_data"]
    create_mock_data(source, rows, data_dest)
