# Copyright 2023 Ant Group Co., Ltd.
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
from pathlib import Path
from typing import List

COLUMN_COPY_NUM = 3
TABLE_COPY_NUM = 3
DATA_TYPE = ["int", "float", "string", "datetime", "timestamp"]
CCL_LEVEL = [
    "plain",
    "join",
    "joinpayload",
    "groupby",
    "compare",
    "aggregate",
    "encrypt",
]
CUR_PATH = Path(__file__).parent.resolve()
DATABASES = ["alice", "bob", "carol"]


def create_table_for_db(db_name: str):
    tables = dict()
    for i in range(TABLE_COPY_NUM):
        name, table_info = create_table(i, db_name)
        tables[name] = table_info
    return tables


def create_table(pos, db_name: str):
    table_name_prefix = "tbl"
    table = dict()
    table_name = f"{table_name_prefix}_{pos}"
    table["columns"] = list()
    table["db_name"] = db_name
    for i in range(COLUMN_COPY_NUM):
        for dtype in DATA_TYPE:
            for level in CCL_LEVEL:
                table["columns"].append(create_column(dtype, [level], i))
    return table_name, table


def create_column(data_type: str, levels: List[str], pos: int):
    column = dict()
    level_strs = "-".join(levels)
    column["column_name"] = f"{level_strs}_{data_type}_{pos}"
    column["dtype"] = data_type
    column["ccl"] = levels
    return column


def write_table(file_name):
    path = os.path.join(CUR_PATH, f"testdata")
    if not os.path.exists(path):
        os.system(f"mkdir {path}")
    file = os.path.join(path, f"{file_name}.json")
    with open(file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, separators=(",", ": "), ensure_ascii=False)
    print("write success")


if __name__ == "__main__":
    for db_name in DATABASES:
        file_name = f"generated_table_{db_name}.json"
        path = os.path.join(CUR_PATH, f"testdata")
        file = os.path.join(path, file_name)
        if os.path.exists(file):
            os.remove(file)
        with open(file, "w", encoding="utf-8") as f:
            json.dump(
                create_table_for_db(db_name),
                f,
                indent=2,
                separators=(",", ": "),
                ensure_ascii=False,
            )
        print("write success")
