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

"""SCQL Compiler Module

Provides Python bindings for SCQL compiler that translates SQL queries
into secure multi-party computation execution plans.
"""

try:
    # Try import with correct module name (gopy uses underscore prefix)
    import importlib.util
    import os

    # Load the .so file manually
    so_path = os.path.join(os.path.dirname(__file__), "scql_compiler.so")
    if os.path.exists(so_path):
        # Load using the correct module name and make it available as _scql_compiler
        spec = importlib.util.spec_from_file_location("_scql_compiler", so_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Make _scql_compiler available for sc.py import
        globals()["_scql_compiler"] = module

        # Load the sc.py module and merge its interface directly into this module
        try:
            from . import sc

            # Expose all sc module classes and functions directly in compiler module
            for name in dir(sc):
                if not name.startswith("_"):
                    obj = getattr(sc, name)
                    globals()[name] = obj

        except ImportError as sc_error:
            # If sc.py import fails, fall back to direct low-level interface
            import warnings

            warnings.warn(
                f"Could not import sc.py wrapper: {sc_error}. Using low-level interface.",
                ImportWarning,
            )

            # Expose all symbols from the module for direct access
            for name in dir(module):
                if not name.startswith("_"):
                    globals()[name] = getattr(module, name)

    else:
        raise ImportError(f"scql_compiler.so not found in {so_path}")

except Exception as e:
    import sys
    import warnings

    warnings.warn(
        f"Failed to import SCQL compiler: {e}. "
        "Make sure the Go bindings are properly built.",
        ImportWarning,
    )
    sys.modules[__name__] = None
