#!/usr/bin/env bash
# Generates test certificates for mTLS integration tests.
# Safe to re-run — existing files are overwritten.
#
# Output:
#   ca.crt / ca.key                         — trusted CA
#   server.crt / server.key                 — server cert (SANs: localhost, 127.0.0.1)
#   client.crt / client.key                 — client cert (signed by trusted CA)
#   untrusted-ca.crt / untrusted-ca.key     — separate CA (for rejection tests)
#   untrusted-client.crt / untrusted-client.key — client cert the server will reject

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

DAYS=90
CURVE=prime256v1  # ECDSA P-256

echo "Generating test certificates in $DIR"

# ---------------------------------------------------------------------------
# Trusted CA
# ---------------------------------------------------------------------------
openssl ecparam -name "$CURVE" -genkey -noout -out ca.key
openssl req -new -x509 \
  -key ca.key \
  -out ca.crt \
  -days "$DAYS" \
  -subj "/CN=celeriant-test-ca" \
  -addext "basicConstraints=critical,CA:true" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

echo "  ca.crt / ca.key done"

# ---------------------------------------------------------------------------
# Server cert — SANs required so TLS clients validate the hostname
# ---------------------------------------------------------------------------
openssl ecparam -name "$CURVE" -genkey -noout -out server.key
openssl req -new \
  -key server.key \
  -out server.csr \
  -subj "/CN=celeriant-node"

openssl x509 -req \
  -in server.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -out server.crt \
  -days "$DAYS" \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")

rm server.csr
echo "  server.crt / server.key done"

# ---------------------------------------------------------------------------
# Trusted client cert
# ---------------------------------------------------------------------------
openssl ecparam -name "$CURVE" -genkey -noout -out client.key
openssl req -new \
  -key client.key \
  -out client.csr \
  -subj "/CN=celeriant-client"

openssl x509 -req \
  -in client.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -out client.crt \
  -days "$DAYS" \
  -extfile <(printf "extendedKeyUsage=clientAuth\n")

rm client.csr
echo "  client.crt / client.key done"

# ---------------------------------------------------------------------------
# Untrusted CA — used to sign the rejected client cert
# ---------------------------------------------------------------------------
openssl ecparam -name "$CURVE" -genkey -noout -out untrusted-ca.key
openssl req -new -x509 \
  -key untrusted-ca.key \
  -out untrusted-ca.crt \
  -days "$DAYS" \
  -subj "/CN=celeriant-untrusted-ca" \
  -addext "basicConstraints=critical,CA:true" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

echo "  untrusted-ca.crt / untrusted-ca.key done"

# ---------------------------------------------------------------------------
# Untrusted client cert — signed by untrusted CA, server must reject it
# ---------------------------------------------------------------------------
openssl ecparam -name "$CURVE" -genkey -noout -out untrusted-client.key
openssl req -new \
  -key untrusted-client.key \
  -out untrusted-client.csr \
  -subj "/CN=celeriant-untrusted-client"

openssl x509 -req \
  -in untrusted-client.csr \
  -CA untrusted-ca.crt \
  -CAkey untrusted-ca.key \
  -CAcreateserial \
  -out untrusted-client.crt \
  -days "$DAYS" \
  -extfile <(printf "extendedKeyUsage=clientAuth\n")

rm untrusted-client.csr
echo "  untrusted-client.crt / untrusted-client.key done"

# Clean up serial files
rm -f ca.srl untrusted-ca.srl

echo ""
echo "Done. Certificates valid for $DAYS days."
echo "Start the TLS server with: docker compose -f docker-compose.tls.yml up"
