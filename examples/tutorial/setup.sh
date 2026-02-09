#!/bin/bash
set -eu
# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

# generate password for mysql root user
MYSQL_PASSWD=$(cat /dev/urandom | base64 | tr -dc A-Za-z0-9 | head -c 13)
for file in $(grep -rl '__MYSQL_ROOT_PASSWD__' --exclude='*.sh' ${SCRIPT_DIR}/); do
  # remove suffix .template
  newfile=$(echo $file | sed 's/\.[^.]*$//')
  sed "s/__MYSQL_ROOT_PASSWD__/${MYSQL_PASSWD}/g" $file >$newfile
done

# Check if the system is macOS and LibreSSL is installed
if [[ "$(uname)" == "Darwin" ]] && openssl version | grep -q "LibreSSL"; then
  echo "You are using macOS with LibreSSL, which does not support ed25519."
  echo "Please install OpenSSL using the following command:"
  echo "brew install openssl"
  exit 1
fi

# generate self-signed ca files for engine
(cd $SCRIPT_DIR && bash ../../test-tools/ca_generator.sh)

echo "successfully completed password generation and ca file generation"

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

cd "${PROJECT_ROOT}"
go build -o "${SCRIPT_DIR}/opencore-demo" ./opencore-demo/main.go
