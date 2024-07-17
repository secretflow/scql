#! /usr/bin/python3
import os
import pathlib
import datetime
import sys
import re

CUR_PATH = pathlib.Path(__file__).parent.resolve()


def getTime(str):
    return datetime.datetime.strptime(str, "%Y-%m-%d %H:%M:%S.%f").timestamp()


def myPrint(inputs):
    i = 0
    for p in inputs:
        print("{}: {:.4f}".format(p[1], p[0]), end="\n")
        i += 1
        if p[1] == "(Publish)":
            print("-------------")
    print("\n")


def getTimeList(file, is_start):
    output = []
    with open(file, "r") as f:
        line = f.readline()
        while line:
            times = line.split(" [info]")
            times[0] = times[0].strip("\n").strip(" ")
            # print(times[0])
            # t = getTime(times[0].strip("\n").strip(" ")) - getTime(
            #     times[0].strip("\n").strip(" ")
            # )
            # t.seconds
            # start to execute node(0), op(Permute)
            if is_start:
                print(line)
                matchObj = re.match(
                    r".* start to execute node\((.*?)\), op(\(.*?\))",
                    line,
                    re.M | re.I,
                )
                # print(matchObj.group(1))
                output.append(
                    (getTime(times[0].strip("\n").strip(" ")), matchObj.group(1))
                )
            else:
                matchObj = re.match(
                    r".* finished executing node\((.*?)\), op(\(.*?\))",
                    line,
                    re.M | re.I,
                )
                # print(matchObj.group(1))
                output.append(
                    (getTime(times[0].strip("\n").strip(" ")), matchObj.group(1))
                )
            line = f.readline()
    return output


def main(file1, file2, start_time):
    start = getTimeList(file1, True)
    print(len(start))
    filter_start = []
    for t in start:
        if t[0] - getTime(start_time) > 0:
            filter_start.append(t)

    end = getTimeList(file2, False)
    print(len(end))
    filter_end = []
    for t in end:
        if t[0] - getTime(start_time) > 0:
            filter_end.append(t)
    # print(filter_start)
    # print(filter_end)
    cost = []
    for i, t in enumerate(filter_start):
        if start[i][1] != end[i][1]:
            print("abc{}, {}".format(start[0], start[1]))
        cost.append((filter_end[i][0] - filter_start[i][0], filter_start[i][1]))
    print(len(cost))
    myPrint(cost)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
