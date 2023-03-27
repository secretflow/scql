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

DATABASES = ["alice", "bob", "carol"]
PARTY_CODES = ["alice", "bob", "carol"]
COLUMN_COPY_NUM = 3
TABLE_COPY_NUM = 3
DATA_TYPE = ["long", "float", "string"]
CCL_LEVEL = ["plain", "join", "groupby", "compare", "aggregate", "encrypt"]
CUR_PATH = Path(__file__).parent.resolve()


def create_db(db_name, party_code):
    db = dict()
    db["db_name"] = db_name
    db["tables"] = list()
    for i in range(TABLE_COPY_NUM):
        db["tables"].append(create_table(party_code, i))
    return db


def create_table(party_code, pos):
    table_name_prefix = "tbl"
    table = dict()
    table["party_code"] = party_code
    table["table_name"] = f"{table_name_prefix}_{pos}"
    table["columns"] = list()
    for i in range(COLUMN_COPY_NUM):
        for dtype in DATA_TYPE:
            for level in CCL_LEVEL:
                table["columns"].append(create_column(dtype, level, i))
    return table


def create_column(data_type: str, level: str, pos: int):
    column = dict()
    column["column_name"] = f"{level}_{data_type}_{pos}"
    column["dtype"] = data_type
    column["ccl"] = level
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
    for i, db_name in enumerate(DATABASES):
        db = create_db(db_name, PARTY_CODES[i])
        write_json(db, f"db_{db_name}")
