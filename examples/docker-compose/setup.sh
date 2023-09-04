#!/bin/bash
set -eu
# get work dir
SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

# generate private pem for each party
PARTIES=("alice" "bob")
for party in ${PARTIES[*]}; do
  openssl genpkey -algorithm ed25519 -out "$SCRIPT_DIR/engine/$party/conf/ed25519key.pem"
done

DATA_FILES=("$SCRIPT_DIR/engine/alice/conf/authorized_profile.json" "$SCRIPT_DIR/engine/bob/conf/authorized_profile.json")

ALICE_PUBKEY=$(openssl pkey -in "$SCRIPT_DIR/engine/alice/conf/ed25519key.pem" -pubout -outform DER | base64)
BOB_PUBKEY=$(openssl pkey -in "$SCRIPT_DIR/engine/bob/conf/ed25519key.pem" -pubout -outform DER | base64)

for file in ${DATA_FILES[*]}; do
  if [[ "$(uname)" == "Darwin" ]]; then
    # macOS
    # NOTE: base64 alphabet contains '/', so use '|' as sed delimiters
    sed -i '' -e "s|__ALICE_PUBLIC_KEY__|${ALICE_PUBKEY}|" $file
    sed -i '' -e "s|__BOB_PUBLIC_KEY__|${BOB_PUBKEY}|" $file
  else
    # linux
    sed -i "s|__ALICE_PUBLIC_KEY__|${ALICE_PUBKEY}|" $file
    sed -i "s|__BOB_PUBLIC_KEY__|${BOB_PUBKEY}|" $file
  fi
done
echo "successfully completed private key generation and authorized profile configuration"
