#! /usr/bin/env python3

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

import argparse
import os


def main():
    """
    Configures the .bazelrc file for BuildBuddy remote caching by reading
    an API key from the BUILDBUDDY_API_KEY environment variable.
    """
    parser = argparse.ArgumentParser(
        description="Setup BuildBuddy remote cache for Bazel."
    )

    parser.parse_args()

    # --- BuildBuddy Configuration ---

    # 1. Get the API key from the CircleCI environment variable.
    api_key = os.getenv("BUILDBUDDY_API_KEY")

    if not api_key:
        print("Warning: BUILDBUDDY_API_KEY environment variable not found.")
        print("Proceeding with the build without remote caching.")
        return

    print("Found BUILDBUDDY_API_KEY. Configuring .bazelrc for BuildBuddy...")

    # 2. Append the required BuildBuddy flags to the .bazelrc file.
    with open(".bazelrc", "a") as f:
        f.write(
            "build --bes_results_url=https://secretflow.buildbuddy.io/invocation/\n"
        )
        f.write("build --bes_backend=grpcs://secretflow.buildbuddy.io\n")
        f.write("build --remote_cache=grpcs://secretflow.buildbuddy.io\n")
        f.write("build --remote_timeout=10m\n")
        f.write(f"build --remote_header=x-buildbuddy-api-key={api_key}\n")

    print(".bazelrc has been successfully updated for BuildBuddy.")


if __name__ == "__main__":
    main()
