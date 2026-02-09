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

"""SCQL Engine Module

Provides Python bindings for SCQL execution engine that runs
secure multi-party computation on compiled execution plans.
"""

try:
    from .scql_engine import *

    print("✅ Engine module loaded successfully")
except ImportError as e:
    import sys
    import warnings

    warnings.warn(
        f"Failed to import SCQL engine: {e}. "
        "Make sure the C++ bindings are properly built.",
        ImportWarning,
    )
    print(f"❌ Engine import failed: {e}")
    # Don't set sys.modules[__name__] = None to allow module to be imported
