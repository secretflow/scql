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

import pandas as pd
import matplotlib.pyplot as plt
import sys
import os


def plot_cpu(op_csv_path, docker_csv_path, output_path):
    df1 = pd.read_csv(docker_csv_path)
    df2 = pd.read_csv(op_csv_path)

    df1.set_index("running_time_s", inplace=True)

    plt.figure(figsize=(10, 6))
    plt.plot(df1.index, df1["cpu_usage"], marker="o", linestyle="-", color="b")
    zeros = [0 for _ in range(len(df2))]
    plt.scatter(df2["running_time_s"], zeros, color="r", marker="*", label="Values")
    for i, row in df2.iterrows():
        plt.text(
            row["running_time_s"],
            0,
            str(row["op"]),
            fontsize=9,
            ha="right",
            rotation=45,
        )
    plt.title("cpu over Time")
    plt.xlabel("running time sec")
    plt.ylabel("cpu")
    plt.grid(True)
    plt.xticks(rotation=45)

    plt.savefig(output_path)
    plt.show()


def plot_mem(op_csv_path, docker_csv_path, output_path):
    df1 = pd.read_csv(docker_csv_path)
    df2 = pd.read_csv(op_csv_path)

    df1.set_index("running_time_s", inplace=True)

    plt.figure(figsize=(10, 6))
    plt.plot(df1.index, df1["mem_usage"], marker="o", linestyle="-", color="b")
    zeros = [0 for _ in range(len(df2))]
    plt.scatter(df2["running_time_s"], zeros, color="r", marker="*", label="Values")
    for i, row in df2.iterrows():
        plt.text(
            row["running_time_s"],
            0,
            str(row["op"]),
            fontsize=9,
            ha="right",
            rotation=45,
        )
    plt.title("mem over Time")
    plt.xlabel("running time sec")
    plt.ylabel("mem")
    plt.grid(True)
    plt.xticks(rotation=45)

    plt.savefig(output_path)
    plt.show()


if __name__ == "__main__":
    if not os.path.exists(sys.argv[3]):
        os.makedirs(sys.argv[3])
    plot_cpu(sys.argv[1], sys.argv[2], os.path.join(sys.argv[3], "cpu.png"))
    plot_mem(sys.argv[1], sys.argv[2], os.path.join(sys.argv[3], "mem.png"))
