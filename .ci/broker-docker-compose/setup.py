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
POSTGRES_PORT_ENV_NAME = "POSTGRES_PORT"
PROJECT_CONF_ENV_NAME = "PROJECT_CONF"
SPU_PROTOCOL_ENV_NAME = "SPU_PROTOCOL"

DOCKER_COMPOSE_YAML_FILE = os.path.join(CUR_PATH, "docker-compose.yml")
DOCKER_COMPOSE_TEMPLATE = os.path.join(CUR_PATH, "docker-compose.yml.template")

PARTIES = ["alice", "bob", "carol"]
PARTIES_DB = {"alice": "MYSQL", "bob": "MYSQL", "carol": "MYSQL"}
DB_TYPE = ["MYSQL", "POSTGRES"]


def random_password(length=13):
    chars = string.ascii_letters + string.digits
    return "".join(sample(chars, length))


MYSQL_ROOT_PASSWORD = random_password()
POSTGRES_PASSWORD = random_password()


def split_string(str, separator=","):
    splitted_str = str.split(separator)
    trimmed_str = []
    for s in splitted_str:
        trimmed_str.append(s.lstrip().rstrip())
    return trimmed_str


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


def set_ports_env():
    load_dotenv(override=True)

    mysql_port = os.getenv(MYSQL_PORT_ENV_NAME, "mysql")
    os.environ[MYSQL_PORT_ENV_NAME] = str(get_available_port(mysql_port, "mysql"))
    postgres_port = os.getenv(POSTGRES_PORT_ENV_NAME, "postgres")
    os.environ[POSTGRES_PORT_ENV_NAME] = str(
        get_available_port(postgres_port, "postgres")
    )

    broker_addrs = os.getenv(BROKER_ADDRS_ENV_NAME)
    if broker_addrs:
        addresses = broker_addrs.split(",")
        new_addresses = []

        for address in addresses:
            host, port = address.split(":")
            assert host == "127.0.0.1"
            new_port = get_available_port(port)
            new_address = f"{host}:{new_port}"
            new_addresses.append(new_address)

        # 更新 BROKER_ADDRS 环境变量
        new_broker_addrs = ",".join(new_addresses)
        os.environ[BROKER_ADDRS_ENV_NAME] = new_broker_addrs


def render_files():
    broker_addrs = split_string(os.getenv(BROKER_ADDRS_ENV_NAME))
    assert len(PARTIES) == len(broker_addrs)
    mysql_port = os.getenv(MYSQL_PORT_ENV_NAME)
    pg_port = os.getenv(POSTGRES_PORT_ENV_NAME)
    image_tag = os.getenv(SCQL_IMAGE_NAME_ENV_NAME)
    docker_compose_template = Template(filename=DOCKER_COMPOSE_TEMPLATE)
    print(f"broker addrs: {broker_addrs}")
    docker_compose = docker_compose_template.render(
        MYSQL_PORT=mysql_port,
        POSTGRES_PORT=pg_port,
        ALICE_PORT=split_string(broker_addrs[0], ":")[1],
        BOB_PORT=split_string(broker_addrs[1], ":")[1],
        CAROL_PORT=split_string(broker_addrs[2], ":")[1],
        SCQL_IMAGE_TAG=image_tag,
        MYSQL_ROOT_PASSWORD=MYSQL_ROOT_PASSWORD,
        POSTGRES_PASSWORD=POSTGRES_PASSWORD,
    )
    with open(DOCKER_COMPOSE_YAML_FILE, "w") as f:
        f.write(docker_compose)

    # rendering engine conf
    for p in PARTIES:
        tmpl_path = os.path.join(CUR_PATH, f"engine/{p}/conf/gflags.conf.template")
        dest_path = os.path.join(CUR_PATH, f"engine/{p}/conf/gflags.conf")
        with open(dest_path, "w") as f:
            f.write(
                Template(filename=tmpl_path).render(
                    MYSQL_ROOT_PASSWORD=MYSQL_ROOT_PASSWORD,
                    POSTGRES_PASSWORD=POSTGRES_PASSWORD,
                )
            )
    # rendering broker conf
    for p in PARTIES:
        tmpl_path = os.path.join(CUR_PATH, f"broker/conf/{p}/config.yml.template")
        dest_path = os.path.join(CUR_PATH, f"broker/conf/{p}/config.yml")
        with open(dest_path, "w") as f:
            db_type = PARTIES_DB[p]
            conn_str = ""
            if db_type == "MYSQL":
                conn_str = "root:{}@tcp(mysql:3306)/broker{}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true".format(
                    MYSQL_ROOT_PASSWORD, p[0]
                )
            elif db_type == "POSTGRES":
                conn_str = "user=root password={} dbname=root port=5432 sslmode=disable TimeZone=Asia/Shanghai host=postgres search_path=broker{}".format(
                    POSTGRES_PASSWORD, p[0]
                )
            f.write(
                Template(filename=tmpl_path).render(
                    DB_TYPE=db_type.lower(), DB_CONN_STR=conn_str
                )
            )


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
    skip_create_table_ccl: false
    skip_concurrent_test: false
    skip_plaintext_ccl_test: false
    http_protocol: http
    broker_addrs:
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
        f"root:{MYSQL_ROOT_PASSWORD}@tcp(localhost:{mysql_port})/alice?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
    )
    conf["spu_protocol"] = spu_protocol
    conf["project_conf"] = project_conf
    conf_path = os.path.join(CUR_PATH, "regtest.yml")
    yaml.safe_dump(conf, open(conf_path, "w"), indent=2)
    print(f"run p2p regtest with --conf={conf_path}\n")


if __name__ == "__main__":
    print(
        "The broker uses mysql to store metadata by default, if you want to use specific db instead, use python setup.py postgres bob carol mysql alice"
    )

    if len(sys.argv) > 2:
        db_type = "MYSQL"
        for argv in sys.argv[1:]:
            if argv.upper() in DB_TYPE:
                db_type = argv.upper()
            elif argv.lower() in PARTIES:
                PARTIES_DB[argv.lower()] = db_type
            else:
                print("unknown argv")
                print("db type supports MYSQL/POSTGRES only")
                print("name must in party list")
                exit(1)

    set_ports_env()
    render_files()
    generate_private_keys()
    generate_party_info_json()
    generate_regtest_config()
