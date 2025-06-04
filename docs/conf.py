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

project = "SCQL"

extensions = [
    "myst_parser",
    "secretflow_doctools",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.extlinks",
    "sphinx.ext.graphviz",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
    "sphinxcontrib.mermaid",
]

# https://www.sphinx-doc.org/en/master/usage/extensions/autosectionlabel.html#confval-autosectionlabel_prefix_document
autosectionlabel_prefix_document = True

# source files are in this language
language = "en"
# translation files are in this directory
locale_dirs = ["./locales/"]
# this should be false so 1 doc file corresponds to 1 translation file
gettext_compact = False
gettext_uuid = False
# allow source texts to keep using outdated translations if they are only marginally changed
# otherwise any change to source text will cause their translations to not appear
gettext_allow_fuzzy_translations = True

exclude_patterns = [
    "CONTRIBUTING.md",  # prevent CONTRIBUTING.md from being included in output, optional
    ".venv",
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

todo_include_todos = True

# https://myst-parser.readthedocs.io/en/latest/syntax/optional.html
myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "linkify",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]
# https://myst-parser.readthedocs.io/en/latest/configuration.html#global-configuration
myst_gfm_only = False
# https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#auto-generated-header-anchors
myst_heading_anchors = 6

suppress_warnings = ["autosectionlabel", "myst.header"]
