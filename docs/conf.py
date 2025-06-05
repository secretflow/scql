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
    # enable support for .md files
    # https://myst-parser.readthedocs.io/en/latest/
    "myst_parser",
    "secretflow_doctools",
    # link to titles using :ref:`Title text`
    # https://www.sphinx-doc.org/en/master/usage/extensions/autosectionlabel.html
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.extlinks",
    "sphinx.ext.graphviz",
    "sphinx.ext.todo",
    "sphinxcontrib.mermaid",
]

# also link to titles using :ref:`path/to/document:Title text`
# (note that path should not have a leading slash)
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

# list of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
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
    # LaTeX math
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#direct-latex-math
    "amsmath",
    # attributes
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#attributes
    "attrs_block",
    "attrs_inline",
    # code fence using :::
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#code-fences-using-colons
    "colon_fence",
    # $math$ and $$math$$
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#dollar-delimited-math
    "dollarmath",
    # :name: value
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#field-lists
    "fieldlist",
    # <img src="...">
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#html-images
    "html_image",
    # detect "bare" links
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#linkify
    "linkify",
    # "double quotes" => “double quotes”
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#typography
    "smartquotes",
    # ~~strikethrough~~
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#strikethrough
    "strikethrough",
]
# enable all MyST syntax features
# https://myst-parser.readthedocs.io/en/latest/configuration.html#global-configuration
myst_gfm_only = False
# generate #anchors for heading # through ######
# https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#auto-generated-header-anchors
myst_heading_anchors = 6

suppress_warnings = ["autosectionlabel", "myst.header"]
