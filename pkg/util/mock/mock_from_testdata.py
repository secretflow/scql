import json
import os
import re
import argparse
from mock_schema import CUR_PATH
from mock_db_data import *

BACKEND_TYPE = {"alice": "MYSQL", "bob": "CSVDB", "carol": "POSTGRESQL"}


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
        contents[i] = contents[i].replace(
            "FLOAT_TYPE", REPLACE_MAP[db_type]["FLOAT_TYPE"]
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


def add_to_list_not_exist(in_list: list, to_add: str):
    if to_add not in in_list:
        in_list.append(to_add)


def create_toy_grm(source_file: str, dest_file: str):
    schema_info = parse_json(source_file)
    res = {}
    absolute_dest_file = os.path.join(CUR_PATH, dest_file)
    if os.path.exists(absolute_dest_file):
        with open(absolute_dest_file, "r") as f:
            res = json.load(f)
    # create engine info
    db_name = schema_info["db_name"]
    party_code = schema_info["party_code"]
    url = schema_info["engine"]
    credentials = schema_info["credentials"]
    token = schema_info["token"]
    tables = schema_info["tables"]
    if "engine" not in res:
        res["engine"] = {}
    engine_conf = res["engine"]
    if "read_token" not in engine_conf:
        engine_conf["read_token"] = []
    add_to_list_not_exist(engine_conf["read_token"], token)
    if "engines_info" not in engine_conf:
        engine_conf["engines_info"] = []
    exist = False
    new_conf = {}
    for conf in engine_conf["engines_info"]:
        if conf.get("party") == party_code:
            exist = True
            new_conf = conf
    if not exist:
        engine_conf["engines_info"].append(new_conf)
    new_conf["party"] = party_code
    new_conf["url"] = url
    new_conf["credential"] = credentials
    if "table" not in res:
        res["table"] = {}
    table_conf = res["table"]
    if "read_token" not in table_conf:
        table_conf["read_token"] = []
    add_to_list_not_exist(table_conf["read_token"], token)
    if "ownerships" not in table_conf:
        table_conf["ownerships"] = []
    ownerships = table_conf["ownerships"]
    exist = False
    owner_conf = {"token": token, "tids": []}
    for ownership in ownerships:
        if ownership.get("token") == token:
            exist = True
            owner_conf = ownership
    if not exist:
        ownerships.append(owner_conf)
    if "table_schema" not in table_conf:
        table_conf["table_schema"] = []
    for table_name in tables:
        table = tables[table_name]
        exist = False
        current_schema = {}
        for schema_conf in table_conf["table_schema"]:
            if schema_conf.get("tid") == table.get("tid"):
                exist = True
                current_schema = schema_conf
        if not exist:
            table_conf["table_schema"].append(current_schema)
        current_schema["tid"] = table.get("tid")
        s = {}
        current_schema["schema"] = s
        s["ref_db_name"] = db_name
        s["ref_table_name"] = table_name
        s["db_type"] = BACKEND_TYPE[party_code]
        s["columns"] = []
        for col in table["columns"]:
            s["columns"].append(
                {"column_name": col["column_name"], "column_type": col["dtype"]}
            )
        # update ownership
        add_to_list_not_exist(owner_conf["tids"], current_schema["tid"])
    with open(absolute_dest_file, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=4, separators=(",", ": "), ensure_ascii=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parameters")
    parser.add_argument(
        "--type",
        "-t",
        type=str,
        help="data: mock data; grm: mock grm.json",
        default="data",
    )
    parser.add_argument(
        "--dest_data",
        "-dd",
        type=str,
        help="destination dir",
        default="testdata",
    )
    parser.add_argument(
        "--dest_grm_file",
        "-dgf",
        type=str,
        help="destination file",
        default="testdata/toy_grm.json",
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
    run_type = args["type"]
    source = args["source"]
    rows = args["rows"]
    data_dest = args["dest_data"]
    grm_dest = args["dest_grm_file"]
    if run_type == "data":
        create_mock_data(source, rows, data_dest)
    elif run_type == "grm":
        create_toy_grm(source, grm_dest)
    else:
        os._exit("invalid type, please use -h to see which type you need")
