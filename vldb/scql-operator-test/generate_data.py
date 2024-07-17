#!/usr/bin/python
# -*- coding: UTF-8 -*-

# Generate test data to docker-compose/data for testing scql operators

import random
import string
import csv

# rows' number
rows = 1000 * 10000

# name of csv file
target = "../docker-compose/data/"
filename_alice = target + "test_alice_" + str(rows) + ".csv"
filename_bob = target + "test_bob_" + str(rows) + ".csv"

# writing to csv file
with open(filename_alice, "w") as csvfile:
    # creating a csv writer object
    csvwriter = csv.writer(csvfile)

    # generate the data rows
    for i in range(rows):
        credit_rank = str(random.randint(0, 10))
        income = str(random.randint(100, 1000))
        age = str(random.randint(18, 50))
        # print(phone)
        # print(mail)
        csvwriter.writerow([str(i), credit_rank, income, age])

with open(filename_bob, "w") as csvfile:
    # creating a csv writer object
    csvwriter = csv.writer(csvfile)

    # generate the data rows
    for i in range(rows):
        order_amount = str(random.randint(100, 1000) / 10)
        is_active = str(random.randint(0, 1))

        csvwriter.writerow([str(i), order_amount, is_active])
