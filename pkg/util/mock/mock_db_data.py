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

import random
import datetime

DEFAULT_MAX_STR_LEN = 10
POOL_SIZE = 50
DB_TYPES = ["postgres", "mysql"]
DROP_TABLE = """DROP TABLE IF EXISTS {};"""
CREATE_TABLE_STR = """
CREATE TABLE {0} ({1});
"""
CREATE_DB_STR = [
    """
CREATE SCHEMA IF NOT EXISTS {0};
set search_path to {0};

""",
    """
CREATE DATABASE IF NOT EXISTS `{0}`;
USE `{0}`;

""",
]

COLUMN_NAME_FORMAT = "{}_{}_{}"
TYPE_TO_COLUMN = {
    "int": "{} integer NOT NULL DEFAULT 0",
    "float": "{} FLOAT_TYPE NOT NULL DEFAULT 0.0",
    "string": "{} varchar(64) NOT NULL DEFAULT ''",
    "datetime": "{} DATETIME_TYPE NOT NULL DEFAULT '2020-10-10 10:10:10'",
    "timestamp": "{} timestamp NOT NULL DEFAULT '2020-10-10 10:10:10'",
}

REPLACE_MAP = {
    "postgres": {"FLOAT_TYPE": "numeric", "DATETIME_TYPE": "timestamp with time zone"},
    "mysql": {"FLOAT_TYPE": "double", "DATETIME_TYPE": "datetime"},
}

INSERT_STR = """
INSERT INTO {} VALUES {};"""
SQL_FILE_LIST = {
    "alice": "alice_init.sql",
    "bob": "bob_init.sql",
    "carol": "carol_init.sql",
}
IS_DB_USE_FILE = {
    "mysql": {
        "alice": 1,
        "bob": 1,
        "carol": 1,
    },  # mysql contains all data for comparing results
    "csv": {"alice": 0, "bob": 1, "carol": 0},  # bob use csv as data source
    "postgres": {"alice": 0, "bob": 0, "carol": 1},  # carol use postgres as data source
}


def create_str_pool(pool_size, min_num=97, max_num=115):
    pool = []
    for _ in range(pool_size):
        # from ascii 97-115
        str_len = random.randint(0, DEFAULT_MAX_STR_LEN)
        chr_list = []
        for i in range(str_len):
            chr_list.append(chr(random.randint(min_num, max_num)))
        pool.append("'" + "".join(chr_list) + "'")
    return pool


def create_random_datatime():
    year = random.randint(1971, 2030)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    # avoid DST problem
    hour = random.randint(0, 23)
    while hour == 2:
        hour = random.randint(0, 23)

    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    ds = datetime.datetime(year, month, day, hour, minute, second)
    str_ds = "'" + ds.strftime("%Y-%m-%d %H:%M:%S") + "'"
    return str_ds


def create_data(data_type, str_pool):
    if data_type == "int":
        return random.randint(-100, 100)
    elif data_type == "float":
        return random.randint(-10000, 10000) / 100
    elif data_type == "string":
        return str_pool[random.randint(0, POOL_SIZE - 1)]
    elif data_type == "datetime" or data_type == "timestamp":
        return create_random_datatime()
    else:
        return 0


def fill_column_name(columns):
    column_strs = []
    data_types = []
    for column in columns:
        column_strs.append(
            TYPE_TO_COLUMN[column["dtype"]].format(column["column_name"])
        )
        data_types.append(column["dtype"])
    return column_strs, data_types


def create_table(table_name, columns):
    str = DROP_TABLE.format(table_name)
    column_strs, _ = fill_column_name(columns)
    all_column_strs = ",\n".join(column_strs)
    str += CREATE_TABLE_STR.format(table_name, all_column_strs)
    return str


def create_insert(table_name, columns, str_pool, rows):
    batch_size = 50

    insert_str = "START TRANSACTION;\n"
    for _ in range((rows + batch_size - 1) // batch_size):
        _, data_types = fill_column_name(columns)
        batch_insert = []
        for _ in range(min(batch_size, rows)):
            column_datas = []
            for dt in data_types:
                column_datas.append(str(create_data(dt, str_pool)))
            batch_insert.append("({})".format(", ".join(column_datas)))

        insert_str += INSERT_STR.format(table_name, ", ".join(batch_insert))
        rows -= batch_size
    insert_str += "\nCOMMIT;"

    return insert_str
