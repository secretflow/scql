#! /usr/bin/python3

import os
import pathlib
import datetime
import sys
import re


CUR_PATH = pathlib.Path(__file__).parent.resolve()


def getTime(str):
    return datetime.datetime.strptime(str, "%Y-%m-%d %H:%M:%S.%f").timestamp()


def getTimeList(file):
    output = []
    with open(file, "r") as f:
        line = f.readline()
        while line:
            times = line.split(" [info]")
            times[0] = times[0].strip("\n").strip(" ")
            # t = getTime(times[0].strip("\n").strip(" ")) - getTime(
            #     times[0].strip("\n").strip(" ")
            # )
            # t.seconds
            output.append(getTime(times[0].strip("\n").strip(" ")))
            line = f.readline()
    return output


def myPrint(inputs):
    i = 0
    for p in inputs:
        if i % 5 == 0:
            print("\n")
        if i % 25 == 0:
            print("-------------")
        print(p, end=" ")
        i += 1
    print("\n")


def getCost(file):
    output = []
    with open(file, "r") as f:
        line = f.readline()
        while line:
            matchObj = re.match(r".* running_cost\((.*?)\)", line, re.M | re.I)
            # print(matchObj.group(1))
            output.append(matchObj.group(1))
            line = f.readline()
    return output


def main(fname, start_time):
    times = getTimeList(fname)
    cost = getCost(fname)
    result = []
    for i, t in enumerate(times):
        print(t)
        print(getTime(start_time))
        if t - getTime(start_time) > 0:
            result.append(cost[i])

    myPrint(result)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
