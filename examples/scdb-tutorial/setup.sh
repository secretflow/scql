#!/bin/bash
set -eu
# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

# generate password for mysql root user
MYSQL_PASSWD=$(LC_CTYPE=C tr -dc A-Za-z0-9 </dev/urandom | head -c 13)
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
  openssl genpkey -algorithm ed25519 -out "$SCRIPT_DIR/engine/$party/conf/ed25519key.pem"
done

DATA_FILES=("$SCRIPT_DIR/engine/alice/conf/authorized_profile.json.template" "$SCRIPT_DIR/engine/bob/conf/authorized_profile.json.template")

ALICE_PUBKEY=$(openssl pkey -in "$SCRIPT_DIR/engine/alice/conf/ed25519key.pem" -pubout -outform DER | base64)
BOB_PUBKEY=$(openssl pkey -in "$SCRIPT_DIR/engine/bob/conf/ed25519key.pem" -pubout -outform DER | base64)

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
echo "successfully completed private key generation and authorized profile configuration"
