#!/usr/bin/env bash
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
import os
import sys
import re

PACKAGE_PREFIX = "github.com/secretflow/scql"


def parse_cover_file(cover_file):
    """
    Parse go cover profile, extract all uncovered blocks.
    Returns: list of (filename, start_line, end_line) for uncovered blocks
    """
    result = []
    with open(cover_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("mode:"):
                result.append(line)
                continue
            parts = line.split()
            if len(parts) != 3:
                result.append(line)
                continue
            filepath_pos, stmt_count, exec_count = parts
            if exec_count != "0":
                result.append(line)
                continue  # only care about uncovered
            if ":" not in filepath_pos:
                result.append(line)
                continue
            filepath, pos = filepath_pos.split(":", 1)
            if "," not in pos:
                result.append(line)
                continue
            start, end = pos.split(",")
            sline = int(start.split(".")[0])
            eline = int(end.split(".")[0])
            replaced_filepath = filepath.replace(PACKAGE_PREFIX, os.getcwd())
            if not os.path.exists(replaced_filepath):
                print(f"{replaced_filepath} not exists")
                result.append(line)
                continue
            if find_if_err_block((replaced_filepath, sline, eline)):
                result.append(f"{filepath}:{start},{end} {stmt_count} 1")
                continue
            result.append(line)
    return result


def find_if_err_block(block):
    """
    Iterate over uncovered blocks, locate lines containing patterns like 'if xxxErr != nil {'
    """
    # This regex pattern looks for:
    # - "if" followed by whitespace
    # - any characters (the variable name, e.g., "result.", "finish")
    # - "err" or "Err" or "Error"
    # - "!=" surrounded by optional whitespace
    # - "nil"
    # - an opening curly brace "{"
    err_pattern = re.compile(r"if\s+.*([Ee]rr|Error)\s*!=\s*nil\s*\{")
    try:
        with open(block[0], encoding="utf-8") as f:
            lines = f.readlines()
            # lines number starts at 1!
            for i in range(block[1], min(block[2] + 1, len(lines) + 1)):
                code = lines[i - 1]
                if err_pattern.search(code):
                    print(f"Matched: {block[0]}:{i}: {code.strip()}")
                    return True
    except Exception as e:
        # file might not exist or encoding issue
        print(e)
        return False
    return False


def main():
    results = parse_cover_file(sys.argv[1])
    with open(sys.argv[2], "w", encoding="utf-8") as f:
        for line in results:
            f.write(line)
            f.write("\n")


if __name__ == "__main__":
    main()
