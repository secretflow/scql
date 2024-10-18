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
import re


def parse_dot_file(dot_file_path):
    nodes = []
    links = []

    with open(dot_file_path, "r") as file:
        lines = file.readlines()

    node_pattern = re.compile(r'(\d+) \[label="(.+?)"\]')
    link_pattern = re.compile(r'(\d+) -> (\d+) \[label = "(.+?)"\]')

    for line in lines:
        node_match = node_pattern.match(line)
        if node_match:
            nodes.append(
                {
                    "id": int(node_match.group(1)),
                    "label": node_match.group(2),
                }
            )
        else:
            link_match = link_pattern.match(line)
            if link_match:
                links.append(
                    {
                        "source": int(link_match.group(1)),
                        "target": int(link_match.group(2)),
                        "label": link_match.group(3),
                    }
                )

    return nodes, links


def generate_js_data(nodes, links):
    js_data = "const data = {\n"

    js_data += "    nodes: [\n"
    for node in nodes:
        js_data += f'        {{ id: {node["id"]}, label: "{node["label"]}" }},\n'
    js_data += "    ],\n"

    js_data += "    links: [\n"
    for link in links:
        js_data += f'        {{ source: {link["source"]}, target: {link["target"]}, label: "{link["label"]}" }},\n'
    js_data += "    ]\n"

    js_data += "};\n"
    return js_data


def insert_js_data_into_html(html_template_path, js_data, output_path):
    with open(html_template_path, "r") as file:
        html_content = file.read()

    modified_html_content = re.sub(
        r"const data = {[^}]*};", js_data, html_content, flags=re.DOTALL
    )

    with open(output_path, "w") as file:
        file.write(modified_html_content)


if __name__ == "__main__":
    script_directory = os.path.dirname(os.path.abspath(__file__))

    dot_file_path = os.path.join(script_directory, "graph.dot")
    html_template_path = os.path.join(script_directory, "template.html")
    output_path = os.path.join(script_directory, "graph_visualization.html")

    nodes, links = parse_dot_file(dot_file_path)
    js_data = generate_js_data(nodes, links)
    insert_js_data_into_html(html_template_path, js_data, output_path)

    print("HTML file generated successfully.")
