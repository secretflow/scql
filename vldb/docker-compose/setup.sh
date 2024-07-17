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

# generate private pem for each party
PARTIES=("alice" "bob")
for party in ${PARTIES[*]}; do
  openssl genpkey -algorithm ed25519 -out "$SCRIPT_DIR/broker/$party/conf/private_key.pem"
done

DATA_FILES=("$SCRIPT_DIR/broker/alice/conf/party_info.json.template" "$SCRIPT_DIR/broker/bob/conf/party_info.json.template")

ALICE_PUBKEY=$(openssl pkey -in "$SCRIPT_DIR/broker/alice/conf/private_key.pem" -pubout -outform DER | base64)
BOB_PUBKEY=$(openssl pkey -in "$SCRIPT_DIR/broker/bob/conf/private_key.pem" -pubout -outform DER | base64)

for file in ${DATA_FILES[*]}; do
  # remove suffix .template
  newfile=$(echo $file | sed 's/\.[^.]*$//')
  if [[ "$(uname)" == "Darwin" ]]; then
    # macOS
    # NOTE: base64 alphabet contains '/', so use '|' as sed delimiters
    sed "s|__ALICE_PUBLIC_KEY__|${ALICE_PUBKEY}|g" $file >$newfile
    sed -i '' "s|__BOB_PUBLIC_KEY__|${BOB_PUBKEY}|g" $newfile
  else
    # linux
    sed "s|__ALICE_PUBLIC_KEY__|${ALICE_PUBKEY}|g" $file >$newfile
    sed -i "s|__BOB_PUBLIC_KEY__|${BOB_PUBKEY}|g" $newfile
  fi
done

# generate self-signed ca files for each broker and engine
(cd $SCRIPT_DIR && bash ../../test-tools/ca_generator.sh)

echo "successfully completed private key generation, public key configuration and ca file generation"
