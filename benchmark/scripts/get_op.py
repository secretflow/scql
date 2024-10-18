# Copyright 2024 Ant Group Co., Ltd.
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
import pathlib
import datetime
import sys
import re

CUR_PATH = pathlib.Path(__file__).parent.resolve()


def get_time(str):
    return (
        datetime.datetime.strptime(str, "%Y-%m-%d %H:%M:%S.%f").timestamp() + 8 * 3600
    )


def dump_csv(inputs, dump_file):
    with open(dump_file, "w") as f:
        f.write("op,duration_ms,start_time_s,running_time_s\n")
        for p in inputs:
            f.write(
                "{}, {:.2f}, {:.0f}, {:.0f}\n".format(p[0], p[1] * 1000, p[2], p[3])
            )


def get_time_list(file):
    start = []
    end = []
    with open(file, "r") as f:
        line = f.readline()
        while line:
            times = line.split(" [info]")
            if len(times) > 1:
                times[0] = times[0].strip("\n").strip(" ")
                # match start
                matchObj = re.match(
                    r".* start to execute node\((.*?)\) op(\(.*?\))",
                    line,
                    re.M | re.I,
                )
                if matchObj:
                    start.append(
                        (get_time(times[0].strip("\n").strip(" ")), matchObj.group(1))
                    )

                # match end
                matchObj = re.match(
                    r".* finished executing node\((.*?)\), op(\(.*?\))",
                    line,
                    re.M | re.I,
                )
                if matchObj:
                    end.append(
                        (get_time(times[0].strip("\n").strip(" ")), matchObj.group(1))
                    )
            line = f.readline()
    return start, end


def main(file, dump_file):
    start, end = get_time_list(file)
    cost = []
    for i, t in enumerate(start):
        assert start[i][1] == end[i][1]
        cost.append(
            (
                start[i][1],
                end[i][0] - start[i][0],
                start[i][0],
                start[i][0] - start[0][0],
            )
        )
    dump_csv(cost, dump_file)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
