import socket
from mako.template import Template
from pathlib import Path
from dotenv import load_dotenv
import os
import subprocess
import sys
import base64
import json
import string
import yaml
from random import sample

CUR_PATH = Path(__file__).parent.resolve()
MYSQL_PORT_ENV_NAME = "MYSQL_PORT"
BROKER_ADDRS_ENV_NAME = "BROKER_ADDRS"
SCQL_IMAGE_NAME_ENV_NAME = "SCQL_IMAGE_TAG"
PROJECT_CONF_ENV_NAME = "PROJECT_CONF"
SPU_PROTOCOL_ENV_NAME = "SPU_PROTOCOL"
MYSQL_ROOT_PASSWORD_ENV_NAME = "MYSQL_ROOT_PASSWORD"
ALICE_MEM_ENV_NAME = "ALICE_MEMORY_LIMIT"
BOB_MEM_ENV_NAME = "BOB_MEMORY_LIMIT"
CAROL_MEM_ENV_NAME = "CAROL_MEMORY_LIMIT"
ALICE_CPU_ENV_NAME = "ALICE_CPU_LIMIT"
BOB_CPU_ENV_NAME = "BOB_CPU_LIMIT"
CAROL_CPU_ENV_NAME = "CAROL_CPU_LIMIT"


DOCKER_COMPOSE_YAML_FILE = os.path.join(CUR_PATH, "docker-compose.yml")
DOCKER_COMPOSE_TEMPLATE = os.path.join(CUR_PATH, "docker-compose.yml.template")

PARTIES = ["alice", "bob", "carol"]


def load_env(path: str):
    load_dotenv(path, override=True)


def random_mysql_password(length=13):
    passwd = os.getenv(MYSQL_ROOT_PASSWORD_ENV_NAME)
    if passwd:
        return passwd
    chars = string.ascii_letters + string.digits
    return "".join(sample(chars, length))


MYSQL_ROOT_PASSWORD = random_mysql_password()


def split_string(s, separator=","):
    splitted_str = s.split(separator)
    trimmed_str = []
    for s in splitted_str:
        trimmed_str.append(s.lstrip().rstrip())
    return trimmed_str


def create_csv_conn_str(source_file: str, party: str):
    result = dict()
    # use party as db name
    result["dbName"] = party
    info = parse_json(source_file)
    data_type_map = {
        "int": "INT64",
        "float": "FLOAT",
        "string": "STRING",
    }
    result["tables"] = []
    for table_file in info.get("table_files"):
        table_info = parse_json(f"{os.path.dirname(source_file)}/{table_file}")
        for table_name in table_info:
            if table_info[table_name]["db_name"] == party:
                table_schema = dict()
                table_schema["tableName"] = table_name
                table_schema["dataPath"] = f"/data/{party}_{table_name}.csv"
                table_schema["columns"] = []
                for column_info in table_info[table_name]["columns"]:
                    column_schema = dict()
                    column_schema["columnName"] = column_info["column_name"]
                    column_schema["columnType"] = data_type_map[column_info["dtype"]]
                    table_schema["columns"].append(column_schema)
                result["tables"].append(table_schema)
    return json.dumps(result).replace('"', '\\"')


def parse_json(source_file: str):
    result = {}
    with open(os.path.join(CUR_PATH, source_file), "r") as f:
        result = json.load(f)
    return result


def find_free_port(hint, host="127.0.0.1"):
    """
    Find a free port near the specified port, changing the tens place to 0 and incrementing by 10.
    """
    start = (hint // 100) * 100 + (hint % 10)
    increment = 10

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        for port in range(start, 65536, increment):
            if port < 1024:
                continue  # Skip invalid port numbers
            try:
                s.bind((host, port))
                return port
            except OSError:
                continue
        raise RuntimeError("No free ports available in the desired range")


def get_available_port(port, tag=""):
    port = int(port)
    if tag != "":
        print(f"Port for {tag}: ")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", port))
            if tag != "":
                print(f"Port {port} is available.")
            return port
    except OSError:
        print(f"Port {port} is already in use. Trying to find a free port...")
        port = find_free_port(port)
        print(f"Port {port} is available.")
        return port


def render_files():
    broker_addrs = split_string(os.getenv(BROKER_ADDRS_ENV_NAME))
    assert len(PARTIES) == len(broker_addrs)
    mysql_port = os.getenv(MYSQL_PORT_ENV_NAME)
    image_tag = os.getenv(SCQL_IMAGE_NAME_ENV_NAME)
    spu_protocol = os.getenv(SPU_PROTOCOL_ENV_NAME)
    docker_compose_template = Template(filename=DOCKER_COMPOSE_TEMPLATE)
    print(f"original broker addrs: {broker_addrs}")

    docker_compose = docker_compose_template.render(
        MYSQL_PORT=get_available_port(mysql_port, "mysql"),
        ALICE_PORT=get_available_port(split_string(broker_addrs[0], ":")[1], "alice"),
        BOB_PORT=get_available_port(split_string(broker_addrs[1], ":")[1], "bob"),
        CAROL_PORT=get_available_port(split_string(broker_addrs[2], ":")[1], "carol"),
        SCQL_IMAGE_TAG=image_tag,
        MYSQL_ROOT_PASSWORD=MYSQL_ROOT_PASSWORD,
        ALICE_CPU_LIMIT=os.getenv(ALICE_CPU_ENV_NAME),
        BOB_CPU_LIMIT=os.getenv(BOB_CPU_ENV_NAME),
        CAROL_CPU_LIMIT=os.getenv(CAROL_CPU_ENV_NAME),
        ALICE_MEMORY_LIMIT=os.getenv(ALICE_MEM_ENV_NAME),
        BOB_MEMORY_LIMIT=os.getenv(BOB_MEM_ENV_NAME),
        CAROL_MEMORY_LIMIT=os.getenv(CAROL_MEM_ENV_NAME),
    )
    with open(DOCKER_COMPOSE_YAML_FILE, "w") as f:
        f.write(docker_compose)

    # rendering engine conf
    for p in PARTIES:
        tmpl_path = os.path.join(CUR_PATH, f"engine/{p}/conf/gflags.conf.template")
        dest_path = os.path.join(CUR_PATH, f"engine/{p}/conf/gflags.conf")
        # cc = create_csv_conn_str(
        #     os.path.join(CUR_PATH, f"../testdata/db.json"),
        #     p,
        # )
        # print(f"{p} conn str: {cc}")
        with open(dest_path, "w") as f:
            f.write(
                Template(filename=tmpl_path).render(
                    CONN_STR=create_csv_conn_str(
                        os.path.join(CUR_PATH, f"../testdata/db.json"),
                        p,
                    ),
                )
            )
    # rendering broker conf
    for p in PARTIES:
        tmpl_path = os.path.join(CUR_PATH, f"broker/conf/{p}/config.yml.template")
        dest_path = os.path.join(CUR_PATH, f"broker/conf/{p}/config.yml")
        with open(dest_path, "w") as f:
            conn_str = "root:{}@tcp(mysql:3306)/broker{}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true".format(
                MYSQL_ROOT_PASSWORD, p[0]
            )
            f.write(Template(filename=tmpl_path).render(DB_CONN_STR=conn_str))


# generate private key for broker and engine
def generate_private_keys():
    for p in PARTIES:
        pem_path = os.path.join(CUR_PATH, f"broker/conf/{p}/private_key.pem")
        try:
            result = subprocess.run(
                ["openssl", "genpkey", "-algorithm", "ed25519", "-out", pem_path]
            )
            result.check_returncode()
        except subprocess.CalledProcessError as e:
            print(e, file=sys.stderr)


def generate_party_info_json():
    pubkeys = dict()
    for p in PARTIES:
        pem_path = os.path.join(
            CUR_PATH,
            f"broker/conf/{p}/private_key.pem",
        )
        result = subprocess.run(
            ["openssl", "pkey", "-in", pem_path, "-pubout", "-outform", "DER"],
            capture_output=True,
        )
        result.check_returncode()
        pubkey = base64.standard_b64encode(result.stdout).decode()
        pubkeys[p] = pubkey
    for p in PARTIES:
        tmpl_path = os.path.join(CUR_PATH, f"broker/conf/{p}/party_info.json.template")
        dest_path = os.path.join(CUR_PATH, f"broker/conf/{p}/party_info.json")
        with open(dest_path, "w") as f:
            f.write(
                Template(filename=tmpl_path).render(
                    ALICE_PUBKEY=pubkeys["alice"],
                    BOB_PUBKEY=pubkeys["bob"],
                    CAROL_PUBKEY=pubkeys["carol"],
                )
            )


def generate_regtest_config():
    conf = """
    http_protocol: http
    broker_addrs:
    skip_create_table_ccl: false
    mysql_conn_str:
    spu_protocol:
    project_conf:
    """
    conf = yaml.safe_load(conf)
    # populate conf content
    conf["broker_addrs"] = os.getenv(BROKER_ADDRS_ENV_NAME)
    mysql_port = os.getenv(MYSQL_PORT_ENV_NAME)
    project_conf = os.getenv(PROJECT_CONF_ENV_NAME)
    spu_protocol = os.getenv(SPU_PROTOCOL_ENV_NAME)
    conf["mysql_conn_str"] = (
        f"root:{MYSQL_ROOT_PASSWORD}@tcp(localhost:{mysql_port})/brokera?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
    )
    conf["spu_protocol"] = spu_protocol
    conf["project_conf"] = project_conf
    conf_path = os.path.join(CUR_PATH, "regtest.yml")
    yaml.safe_dump(conf, open(conf_path, "w"), indent=2)
    print(f"run p2p regtest with --conf={conf_path}\n")


if __name__ == "__main__":
    load_env(sys.argv[1])
    render_files()
    generate_private_keys()
    generate_party_info_json()
    generate_regtest_config()
