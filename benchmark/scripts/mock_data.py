# Copyright 2024 Ant Group Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import re
import argparse
from pathlib import Path
import random


CUR_PATH = Path(__file__).parent.resolve()

Default_STRING_POOL = ["abc", "def", "ghi", "jkl", "mno", "pqr", "stu", "vwx"]

# mock data type
# default RANDOM_MOCK
RANDOM_MOCK = "random"
RANGE_MOCK = "random_range"
RANDOM_POOL = "random_pool"
INCREMENT_MOCK = "increment"

# key word
MOCK_TYPE_KEY = "mock_type"
RANGE_KEY = "range"
RANDOM_POOL_KEY = "pool"
STRING_LEN = "str_len"


def bench_mock_int(column: map, row_num: int, cur_pos: int):
    if not MOCK_TYPE_KEY in column:
        column[MOCK_TYPE_KEY] = RANDOM_MOCK
    mock_type = column[MOCK_TYPE_KEY]
    result = []
    if mock_type == RANDOM_MOCK:
        result = [str(random.randint(-100, 100)) for i in range(row_num)]
    elif mock_type == RANGE_MOCK:
        assert RANGE_KEY in column
        assert len(column[RANGE_KEY]) == 2
        result = [
            str(random.randint(column[RANGE_KEY][0], column[RANGE_KEY][1]))
            for i in range(row_num)
        ]
    elif mock_type == RANDOM_POOL:
        assert RANDOM_POOL_KEY in column
        assert len(column[RANDOM_POOL_KEY]) > 1
        index = [
            random.randint(0, len(column[RANDOM_POOL_KEY]) - 1) for i in range(row_num)
        ]
        for i in index:
            result.append(str(column[RANDOM_POOL_KEY][i]))
    elif mock_type == INCREMENT_MOCK:
        result = [str(i + cur_pos) for i in range(row_num)]
    else:
        raise Exception("unknown mock type for int")
    return result


def bench_mock_float(column: map, row_num: int, cur_pos: int):
    if not MOCK_TYPE_KEY in column:
        column[MOCK_TYPE_KEY] = RANDOM_MOCK
    mock_type = column[MOCK_TYPE_KEY]
    result = []
    if mock_type == RANDOM_MOCK:
        result = [str(random.randint(-10000, 10000) / 100) for i in range(row_num)]
    elif mock_type == RANGE_MOCK:
        assert RANGE_KEY in column
        assert len(column[RANGE_KEY]) == 2
        result = [
            str(
                random.randint(column[RANGE_KEY][0] * 1000, column[RANGE_KEY][1] * 1000)
                / 1000
            )
            for i in range(row_num)
        ]
    elif mock_type == RANDOM_POOL:
        assert RANDOM_POOL_KEY in column
        assert len(column[RANDOM_POOL_KEY]) > 1
        index = [
            random.randint(0, len(column[RANDOM_POOL_KEY]) - 1) for i in range(row_num)
        ]
        for i in index:
            result.append(str(column[RANDOM_POOL_KEY][i]))
    else:
        raise Exception("unknown mock type for float")
    return result


def bench_mock_str(column: map, row_num: int, cur_pos: int):
    if not MOCK_TYPE_KEY in column:
        column[MOCK_TYPE_KEY] = RANDOM_MOCK
    mock_type = column[MOCK_TYPE_KEY]
    result = []
    str_len = 0
    if STRING_LEN in column:
        str_len = column[STRING_LEN]
    if mock_type == RANDOM_MOCK:
        index = [random.randint(0, len(Default_STRING_POOL)) for i in range(row_num)]
        for i in index:
            result.append(column[Default_STRING_POOL][i])
    elif mock_type == RANDOM_POOL:
        assert RANDOM_POOL_KEY in column
        assert len(column[RANDOM_POOL_KEY]) > 1
        index = [
            random.randint(0, len(column[RANDOM_POOL_KEY]) - 1) for i in range(row_num)
        ]
        for i in index:
            result.append(column[RANDOM_POOL_KEY][i])
    elif mock_type == INCREMENT_MOCK:
        result = [str(i + cur_pos).zfill(str_len) for i in range(row_num)]
    else:
        raise Exception("unknown mock type for string")
    return result


def create_bench_data(column: map, row_num: int, cur_pos: int):
    data_type = column["dtype"]
    if data_type == "int":
        return bench_mock_int(column, row_num, cur_pos)
    elif data_type == "float":
        return bench_mock_float(column, row_num, cur_pos)
    elif data_type == "string":
        return bench_mock_str(column, row_num, cur_pos)
    else:
        raise Exception("unknown mock type")


def create_csv(table_name, columns, row_num, file_path):
    with open(file_path, "w") as f:
        column_strs = []
        for column in columns:
            assert "dtype" in column
            column_strs.append(column["column_name"])
        f.write(", ".join(column_strs) + "\n")
        column_datas = []
        batch_size = 100000
        cur_pos = 0
        while cur_pos < row_num:
            tmp_num = min(batch_size, row_num - cur_pos)
            column_datas = []
            for column in columns:
                column_datas.append(create_bench_data(column, tmp_num, cur_pos))
            for i in range(tmp_num):
                t_data = [column_datas[j][i] for j in range(len(column_datas))]
                f.write(", ".join(t_data) + "\n")
            cur_pos += tmp_num
            print(f"create {cur_pos} rows")
    return


def create_mock_data(source: dict, rows: int, dest_dir: str):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    db_infos = get_db_from_json(source)
    tmp_rows = rows
    for db_name in db_infos:
        schema_info = db_infos.get(db_name)
        assert "table_info" in schema_info
        tables = schema_info["table_info"]
        for table_name in tables:
            table = tables.get(table_name)
            assert "columns" in table
            columns = table.get("columns")
            if "row_num" in table:
                tmp_rows = table.get("row_num")
            create_csv(
                table_name,
                columns,
                tmp_rows,
                os.path.join(dest_dir, f"{db_name}_{table_name}.csv"),
            )


def parse_json(source_file: str):
    result = {}
    with open(source_file, "r") as f:
        result = json.load(f)
    return result


def get_db_from_json(source_file: str):
    result = dict()
    info = parse_json(source_file)
    db_info = info.get("db_info")
    for db_name in db_info:
        result[db_name] = db_info[db_name]
        result[db_name]["table_info"] = dict()
    for table_file in info.get("table_files"):
        table_info = parse_json(f"{os.path.dirname(source_file)}/{table_file}")
        for table_name in table_info:
            result[table_info[table_name]["db_name"]]["table_info"][table_name] = (
                table_info[table_name]
            )
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
