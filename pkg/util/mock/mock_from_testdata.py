import json
import os
import re
import argparse
from mock_schema import CUR_PATH
from mock_db_data import *

BACKEND_TYPE = {"alice": "MYSQL", "bob": "CSVDB", "carol": "POSTGRESQL"}


# create mock xxx.sql used in mysql initiating
def create_mock_data(source: dict, rows: int, dest_dir: str):
    db_infos = get_db_from_json(source)
    str_pool = create_str_pool(POOL_SIZE)
    for db_name in db_infos:
        schema_info = db_infos.get(db_name)
        tables = schema_info.get("table_info")
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
                    insert_str.replace("START TRANSACTION;\n", "")
                    .replace("\nCOMMIT;", "")
                    .replace("INSERT INTO " + table_name + " VALUES ", "")
                    .replace(";", "")
                    .replace(", ", ",")
                    .replace("'", '"')
                    .replace("(", "")
                    .replace("),", "\n")
                    .replace(")", "")
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
                    os.path.join(
                        CUR_PATH, dest_dir, f"{db_type}_{SQL_FILE_LIST[db_name]}"
                    ),
                    "w",
                ) as f:
                    f.write(CREATE_DB_STR[i].format(db_name))
                    f.write(contents[i])
            contents[i] = ""


def get_db_from_json(source_file: str):
    result = dict()
    info = parse_json(source_file)
    db_info = info.get("db_info")
    for db_name in db_info:
        result[db_name] = db_info[db_name]
        result[db_name]["table_info"] = dict()
    for table_file in info.get("table_files"):
        table_info = parse_json(table_file)
        for table_name in table_info:
            result[table_info[table_name]["db_name"]]["table_info"][table_name] = (
                table_info[table_name]
            )
    return result


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
        default="testdata/db.json",
    )
    parser.add_argument("--rows", "-r", type=int, help="rows of table", default=600)
    args = vars(parser.parse_args())
    source = args["source"]
    rows = args["rows"]
    data_dest = args["dest_data"]
    create_mock_data(source, rows, data_dest)
