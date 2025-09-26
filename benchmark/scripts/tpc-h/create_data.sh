#!/bin/bash
#
# Copyright 2025 Ant Group Co., Ltd.
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
#

set -eu

git clone --depth=1 https://github.com/electrum/tpch-dbgen.git

cd tpch-dbgen
git apply ../tpch.patch

make
./dbgen -vf -s 1
cd ..

python clean_data.py tpch-dbgen/nation.tbl "n_nationkey, n_name, n_regionkey, n_comment" alice_nation.csv
python clean_data.py tpch-dbgen/customer.tbl "c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment" bob_customer.csv
python clean_data.py tpch-dbgen/lineitem.tbl "l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment" alice_lineitem.csv
python clean_data.py tpch-dbgen/orders.tbl "o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment" bob_orders.csv
python clean_data.py tpch-dbgen/part.tbl "p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment" alice_part.csv
python clean_data.py tpch-dbgen/partsupp.tbl "ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment" alice_partsupp.csv
python clean_data.py tpch-dbgen/supplier.tbl "s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment" alice_supplier.csv
python clean_data.py tpch-dbgen/region.tbl "r_regionkey, r_name, r_comment" alice_region.csv
