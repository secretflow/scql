#!/bin/bash
set -eu
# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

DATA_FILES=("$SCRIPT_DIR/scdb/conf/toy_grm.json" "$SCRIPT_DIR/client/users.json" "$SCRIPT_DIR/engine/alice/conf/gflags.conf" "$SCRIPT_DIR/engine/bob/conf/gflags.conf")

ALICE_TOKEN=$(openssl rand -base64 32 | tr -d [/+=])
BOB_TOKEN=$(openssl rand -base64 32 | tr -d [/+=])
ALICE_CREDENTIAL=$(openssl rand -base64 32 | tr -d [/+=])
BOB_CREDENTIAL=$(openssl rand -base64 32 | tr -d [/+=])

for file in ${DATA_FILES[*]}; do
  if [[ "$(uname)" == "Darwin" ]]; then
    # macOS
    sed -i '' -e "s/__ALICE_TOKEN__/${ALICE_TOKEN}/" $file
    sed -i '' -e "s/__BOB_TOKEN__/${BOB_TOKEN}/" $file
    sed -i '' -e "s/__ALICE_CREDENTIAL__/${ALICE_CREDENTIAL}/" $file
    sed -i '' -e "s/__BOB_CREDENTIAL__/${BOB_CREDENTIAL}/" $file
  else
    # linux
    sed -i "s/__ALICE_TOKEN__/${ALICE_TOKEN}/" $file
    sed -i "s/__BOB_TOKEN__/${BOB_TOKEN}/" $file
    sed -i "s/__ALICE_CREDENTIAL__/${ALICE_CREDENTIAL}/" $file
    sed -i "s/__BOB_CREDENTIAL__/${BOB_CREDENTIAL}/" $file
  fi
done
echo "success register user and engine information"
