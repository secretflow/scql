#!/bin/bash
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
cd "${SCRIPT_DIR}"

if [ ! -f "docker-compose.yml" ]; then
    if [ -f "setup.sh" ]; then
        ./setup.sh
    else
        echo "Error: Neither docker-compose.yml nor setup.sh found"
        exit 1
    fi
fi

COMPOSE_FILE="docker-compose.yml"

docker-compose -f "${COMPOSE_FILE}" -p tutorial down 2>/dev/null || true
docker-compose -f "${COMPOSE_FILE}" -p tutorial up -d

if [ ! -f "example_config.json" ]; then
    echo "Error: example_config.json not found"
    exit 1
fi

sleep 5

echo ""
echo "Setup complete! Please take a try with the following command:"
echo "./opencore-demo --config=example_config.json"
