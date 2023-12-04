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
BROKER_PORTS_ENV_NAME = "BROKER_PORTS"
SCQL_IMAGE_NAME_ENV_NAME = "SCQL_IMAGE_TAG"
POSTGRES_PORT_ENV_NAME = "POSTGRES_PORT"
PROJECT_CONF_ENV_NAME = "PROJECT_CONF"

DOCKER_COMPOSE_YAML_FILE = os.path.join(CUR_PATH, "docker-compose.yml")
DOCKER_COMPOSE_TEMPLATE = os.path.join(CUR_PATH, "docker-compose.yml.template")

PARTIES = ["alice", "bob", "carol"]


def random_password(length=13):
    chars = string.ascii_letters + string.digits
    return "".join(sample(chars, length))


MYSQL_ROOT_PASSWORD = random_password()
POSTGRES_PASSWORD = random_password()


def split_string(str):
    splitted_str = str.split(",")
    trimmed_str = []
    for s in splitted_str:
        trimmed_str.append(s.lstrip().rstrip())
    return trimmed_str


def render_files():
    load_dotenv(override=True)
    broker_ports = split_string(os.getenv(BROKER_PORTS_ENV_NAME))
    assert len(PARTIES) == len(broker_ports)
    mysql_port = os.getenv(MYSQL_PORT_ENV_NAME)
    pg_port = os.getenv(POSTGRES_PORT_ENV_NAME)
    image_tag = os.getenv(SCQL_IMAGE_NAME_ENV_NAME)
    docker_compose_template = Template(filename=DOCKER_COMPOSE_TEMPLATE)
    print(broker_ports)
    docker_compose = docker_compose_template.render(
        MYSQL_PORT=mysql_port,
        POSTGRES_PORT=pg_port,
        ALICE_PORT=broker_ports[0],
        BOB_PORT=broker_ports[1],
        CAROL_PORT=broker_ports[2],
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
            f.write(
                Template(filename=tmpl_path).render(
                    MYSQL_ROOT_PASSWORD=MYSQL_ROOT_PASSWORD,
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
    broker_ports: 8080
    mysql_conn_str:
    project_conf:
    """
    conf = yaml.safe_load(conf)
    # populate conf content
    conf["broker_ports"] = os.getenv(BROKER_PORTS_ENV_NAME)
    mysql_port = os.getenv(MYSQL_PORT_ENV_NAME)
    project_conf = os.getenv(PROJECT_CONF_ENV_NAME)
    conf[
        "mysql_conn_str"
    ] = f"root:{MYSQL_ROOT_PASSWORD}@tcp(localhost:{mysql_port})/alice?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
    conf["project_conf"] = project_conf
    conf_path = os.path.join(CUR_PATH, "regtest.yml")
    yaml.safe_dump(conf, open(conf_path, "w"), indent=2)
    print(f"run p2p regtest with --conf={conf_path}\n")


if __name__ == "__main__":
    render_files()
    generate_private_keys()
    generate_party_info_json()
    generate_regtest_config()
