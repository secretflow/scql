#!/usr/bin/env bash

# Example SCQL CA certificate generation script.
# It is mainly modified from https://github.com/k3s-io/k3s/blob/master/contrib/util/generate-custom-ca-certs.sh
#
# This script will generate CA files certificate for SCQL to enalbe tls.
# The required files are default located under `./tls` directory.
#
# This script will also auto-generate certificates and keys for both root and intermediate
# certificate authorities if none are found.
# If you have existing certs, you must place them in `./tls`.
# If you have only an existing root CA, provide:
#   root-ca.pem
#   root-ca.key
# If you have an existing root and intermediate CA, provide:
#   root-ca.pem
#   intermediate-ca.pem
#   intermediate-ca.key

set -e
umask 027

TIMESTAMP=$(date +%s)
PRODUCT="SCQL"
DATA_DIR="./tls"

if type -t openssl-3 &>/dev/null; then
  OPENSSL=openssl-3
else
  OPENSSL=openssl
fi

echo "Using $(type -p ${OPENSSL}): $(${OPENSSL} version)"

if ! ${OPENSSL} ecparam -name prime256v1 -genkey -noout -out /dev/null &>/dev/null; then
  echo "openssl not found or missing Elliptic Curve (ecparam) support."
  exit 1
fi

${OPENSSL} version | grep -qF 'OpenSSL 3' && OPENSSL_GENRSA_FLAGS=-traditional

mkdir -p "${DATA_DIR}"
cd "${DATA_DIR}"

# Set up temporary openssl configuration
mkdir -p ".ca/certs"
trap "rm -rf .ca" EXIT
touch .ca/index
openssl rand -hex 8 > .ca/serial
cat >.ca/config <<'EOF'
[ca]
default_ca = ca_default
[ca_default]
dir = ./.ca
database = $dir/index
serial = $dir/serial
new_certs_dir = $dir/certs
default_md = sha256
policy = policy_anything
[policy_anything]
commonName = supplied
[req]
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_ca]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
basicConstraints = critical, CA:true
keyUsage = critical, digitalSignature, keyEncipherment, keyCertSign
subjectAltName = @sans
[sans]
DNS.1 = localhost
DNS.2 = scdb
DNS.3 = engine_alice
DNS.4 = engine_bob
DNS.5 = engine_carol
DNS.6 = broker_alice
DNS.7 = broker_bob
DNS.8 = broker_carol
IP.1 = 127.0.0.1
EOF

# Use existing root CA if present
if [[ -e root-ca.pem ]]; then
  echo "Using existing root certificate"
else
  echo "Generating root certificate authority RSA key and certificate"
  ${OPENSSL} genrsa ${OPENSSL_GENRSA_FLAGS:-} -out root-ca.key 4096
  ${OPENSSL} req -x509 -new -nodes -sha256 -days 7300 \
                 -subj "/CN=${PRODUCT}-root-ca@${TIMESTAMP}" \
                 -key root-ca.key \
                 -out root-ca.pem \
                 -config .ca/config \
                 -extensions v3_ca
fi
cat root-ca.pem > root-ca.crt

# Use existing intermediate CA if present
if [[ -e intermediate-ca.pem ]]; then
  echo "Using existing intermediate certificate"
else
  if [[ ! -e root-ca.key ]]; then
    echo "Cannot generate intermediate certificate without root certificate private key"
    exit 1
  fi

  echo "Generating intermediate certificate authority RSA key and certificate"
  ${OPENSSL} genrsa ${OPENSSL_GENRSA_FLAGS:-} -out intermediate-ca.key 4096
  ${OPENSSL} req -new -nodes \
                 -subj "/CN=${PRODUCT}-intermediate-ca@${TIMESTAMP}" \
                 -key intermediate-ca.key |
  ${OPENSSL} ca  -batch -notext -days 3700 \
                 -in /dev/stdin \
                 -out intermediate-ca.pem \
                 -keyfile root-ca.key \
                 -cert root-ca.pem \
                 -config .ca/config \
                 -extensions v3_ca
fi
cat intermediate-ca.pem root-ca.pem > intermediate-ca.crt

if [[ ! -e intermediate-ca.key ]]; then
  echo "Cannot generate leaf certificates without intermediate certificate private key"
  exit 1
fi

# Generate new leaf CAs for SCDB and all SCQLEngines
for TYPE in scdb engine_alice engine_bob engine_carol broker_alice broker_bob broker_carol; do
  CERT_NAME="${PRODUCT}-$(echo ${TYPE} | tr / -)-ca"
  echo "Generating ${CERT_NAME} leaf certificate authority EC key and certificate"
  ${OPENSSL} ecparam -name prime256v1 -genkey -noout -out ${TYPE}-ca.key
  ${OPENSSL} req -new -nodes \
                 -subj "/CN=${CERT_NAME}@${TIMESTAMP}" \
                 -key ${TYPE}-ca.key |

  ${OPENSSL} ca  -batch -notext -days 3700 \
                 -in /dev/stdin \
                 -out ${TYPE}-ca.pem \
                 -keyfile intermediate-ca.key \
                 -cert intermediate-ca.pem \
                 -config .ca/config \
                 -extensions v3_ca
      
  cat ${TYPE}-ca.pem \
      intermediate-ca.pem \
      root-ca.pem > ${TYPE}-ca.crt
done

echo
echo "CA certificate generation complete. Required files are now present in: ${DATA_DIR}"
