#!/usr/bin/python
# -*- coding: UTF-8 -*-

import random
import csv
import argparse
from datetime import date, timedelta

# rows' number
SF = 1

currentdate = date(1995, 6, 17)
start_date = date(1995, 1, 1)


# writing to csv file
def generate_lineitem(filename, row_num):
    with open(filename, "w", newline="") as csvfile:
        rows = SF * row_num
        print("generate data in ", filename, "with", rows, "rows")
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # generate the data rows
        for i in range(rows):
            l_orderkey = i
            l_partkey = l_orderkey

            l_quantity = random.randint(1, 50)

            p_partkey = l_partkey
            p_retailprice = (
                90000 + ((p_partkey / 10) % 20001) + 100 * (p_partkey % 1000)
            ) / 100

            l_extendedprice = round(l_quantity * p_retailprice, 2)

            l_discount = random.randint(4, 6) / 100

            l_tax = random.randint(0, 8) / 100

            random_day = timedelta(days=random.randint(0, 364))
            l_shipdate = start_date + random_day
            l_commitdate = l_shipdate + timedelta(days=1)
            l_receiptdate = l_commitdate + timedelta(days=1)

            l_returnflag = "N"
            if l_receiptdate <= currentdate:
                l_returnflag = random.choice(["R", "A"])

            l_linestatus = "F"
            if l_shipdate > currentdate:
                l_linestatus = "O"

            l_shipmode = random.choice(["AIR", "RAIL", "SHIP"])
            csvwriter.writerow(
                [
                    str(l_orderkey),
                    str(l_partkey),
                    str(l_quantity),
                    str(l_extendedprice),
                    str(l_discount),
                    str(l_tax),
                    str(l_returnflag),
                    str(l_linestatus),
                    str(l_shipdate),
                    str(l_commitdate),
                    str(l_receiptdate),
                    str(l_shipmode),
                    str(l_shipmode),
                ]
            )


def generate_orders(filename, row_num):
    with open(filename, "w", newline="") as csvfile:
        rows = SF * row_num
        print("generate data in ", filename, "with", rows, "rows")
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # generate the data rows
        for i in range(rows):
            o_orderkey = i
            o_orderpriority = random.choice(
                ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED"]
            )
            csvwriter.writerow([str(o_orderkey), str(o_orderpriority)])


def generate_part(filename, row_num):
    with open(filename, "w", newline="") as csvfile:
        rows = SF * row_num
        print("generate data in ", filename, "with", rows, "rows")
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # generate the data rows
        for i in range(rows):
            p_partkey = i
            syllable_1 = random.choice(
                ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
            )
            syllable_2 = random.choice(
                ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
            )
            syllable_3 = random.choice(["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"])
            p_type = syllable_1 + " " + syllable_2 + " " + syllable_3
            csvwriter.writerow([str(p_partkey), str(p_type)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parameters")
    parser.add_argument(
        "--sf",
        "-s",
        type=int,
        help="scale factor",
        default=1,
    )
    parser.add_argument(
        "--party",
        "-p",
        type=str,
        help="party code",
        default="both",
    )
    parser.add_argument(
        "--row",
        "-r",
        type=int,
        help="row number",
        default=10000000,
    )
    args = parser.parse_args()

    SF = args.sf
    if args.party == "both":
        generate_lineitem("ant_lineitem.csv", args.row)
        generate_lineitem("isv_lineitem.csv", args.row)
        generate_orders("isv_orders.csv", args.row)
        generate_part("isv_part.csv", args.row)
    elif args.party == "isv":
        generate_lineitem("isv_lineitem.csv", args.row)
        generate_orders("isv_orders.csv", args.row)
        generate_part("isv_part.csv", args.row)
    elif args.party == "ant":
        generate_lineitem("ant_lineitem.csv", args.row)
    # generate_orders("isv_orders.csv")
