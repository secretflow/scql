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


import os
import random

DEFAULT_MAX_STR_LEN = 10
POOL_SIZE = 50
CCL_LEVELS = {
    "plain",
    "compare",
    "aggregate",
    "join",
    "union",
    "groupby",
    "encrypt",
}

DATA_TYPE = ["long", "float", "string"]
DROP_TABLE = """DROP TABLE IF EXISTS `{}`;"""
CREATE_TABLE_STR = """
CREATE TABLE `{0}` ({1}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
CREATE_DB_STR = """
CREATE DATABASE IF NOT EXISTS `{0}`;
USE `{0}`;

"""
COLUMN_NAME_FORMAT = "{}_{}_{}"
TYPE_TO_COLUMN = {
    "long": "`{}` int(11) NOT NULL DEFAULT 0",
    "float": "`{}` float NOT NULL DEFAULT 0.0",
    "string": '`{}` varchar(64) NOT NULL DEFAULT ""',
}

INSERT_STR = """
INSERT INTO `{}` VALUES ({});"""
SQL_LIST = {"alice": "alice_init.sql", "bob": "bob_init.sql", "carol": "carol_init.sql"}
STR_POOL = []
DATETIME_POOL = []

DATABASES = ["alice", "bob", "carol"]


def create_str_pool(pool_size, min_num=97, max_num=115):
    pool = []
    for _ in range(pool_size):
        # from ascii 97-115
        str_len = random.randint(0, DEFAULT_MAX_STR_LEN)
        chr_list = []
        for i in range(str_len):
            chr_list.append(chr(random.randint(min_num, max_num)))
        pool.append('"' + "".join(chr_list) + '"')
    return pool


def create_data(data_type):
    if data_type == "long":
        return random.randint(-100, 100)
    elif data_type == "float":
        return random.randint(-10000, 10000) / 100
    elif data_type == "string":
        return str_pool[random.randint(0, POOL_SIZE - 1)]
    else:
        return 0


def fill_column_name(repeat_number):
    columns = []
    data_types = []
    for ccl in CCL_LEVELS:
        for dt in DATA_TYPE:
            for i in range(repeat_number):
                column_name = COLUMN_NAME_FORMAT.format(ccl, dt, i)
                columns.append(TYPE_TO_COLUMN[dt].format(column_name))
                data_types.append(dt)
    return columns, data_types


def create_table(table_name, repeat_number, file_handler):
    file_handler.write(DROP_TABLE.format(table_name))
    columns, _ = fill_column_name(repeat_number)
    all_columns = ",\n".join(columns)
    file_handler.write(CREATE_TABLE_STR.format(table_name, all_columns))


def create_insert(table_name, repeat_number, rows, file_handler):
    for _ in range(rows):
        _, data_types = fill_column_name(repeat_number)
        column_datas = []
        for dt in data_types:
            column_datas.append(str(create_data(dt)))
        file_handler.write(INSERT_STR.format(table_name, ", ".join(column_datas)))


if __name__ == "__main__":
    str_pool = create_str_pool(POOL_SIZE)
    CUR_PATH = os.path.dirname(os.path.abspath(__file__))
    table_list = ["tbl_0", "tbl_1", "tbl_2"]
    repeat_columns = 3
    rows = 600
    for db in SQL_LIST:
        file = open(CUR_PATH + "/initdb/" + SQL_LIST[db], "w")
        file.writelines(CREATE_DB_STR.format(db))
        for table in table_list:
            create_table(table, repeat_columns, file)
            create_insert(table, repeat_columns, rows, file)
            file.writelines("\n\n\n\n")
        file.close()
