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

DATABASES = {
    "alice": {
        "db_name": "alice",
        "party_code": "alice",
        "engine": ["engine-alice:8003"],
        "credentials": ["alice_credential"],
        "token": "alice_token",
        "db_type": "MYSQL",
    },
    "bob": {
        "db_name": "bob",
        "party_code": "bob",
        "engine": ["engine-bob:8003"],
        "credentials": ["bob_credential"],
        "token": "bob_token",
        "db_type": "CSVDB",
    },
    "carol": {
        "db_name": "carol",
        "party_code": "carol",
        "engine": ["engine-carol:8003"],
        "credentials": ["carol_credential"],
        "token": "carol_token",
        "db_type": "POSTGRESQL",
    },
}
COLUMN_COPY_NUM = 3
TABLE_COPY_NUM = 3
DATA_TYPE = ["long", "float", "string", "datetime", "timestamp"]
CCL_LEVEL = ["plain", "join", "groupby", "compare", "aggregate", "encrypt"]
CUR_PATH = Path(__file__).parent.resolve()


def create_db(db_info):
    for i in range(TABLE_COPY_NUM):
        name, table_info = create_table(i)
        db_info["tables"][name] = table_info


def create_table(pos):
    table_name_prefix = "tbl"
    table = dict()
    table_name = f"{table_name_prefix}_{pos}"
    table["columns"] = list()
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


def write_json(data, file_name):
    path = os.path.join(CUR_PATH, f"testdata")
    if not os.path.exists(path):
        os.system(f"mkdir {path}")
    file = os.path.join(path, f"{file_name}.json")
    with open(file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, separators=(",", ": "), ensure_ascii=False)
    print("write success")


if __name__ == "__main__":
    for key in DATABASES:
        db_info = DATABASES[key]
        dest_file = f"db_{db_info['db_name']}"
        db_info["tables"] = {}
        if os.path.exists(dest_file):
            with open(dest_file, "r") as f:
                old_info = json.dump(dest_file)
                if "tables" in old_info:
                    db_info["tables"] = old_info["tables"]
        create_db(db_info)
        write_json(db_info, dest_file)
