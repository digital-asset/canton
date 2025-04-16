#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

# Setup paths
# If we're running from the repo, use the protobuf from the repo
if git rev-parse --is-inside-work-tree &>/dev/null || false; then
    ROOT_PATH=$(git rev-parse --show-toplevel)
    COMMUNITY_PROTO_ROOT="$ROOT_PATH/community/base/src/main/protobuf"
    export TOPOLOGY_PROTO="$COMMUNITY_PROTO_ROOT/com/digitalasset/canton/protocol/v30/topology.proto"
    export CRYPTO_PROTO="$COMMUNITY_PROTO_ROOT/com/digitalasset/canton/crypto/v30/crypto.proto"
    export VERSION_WRAPPER_PROTO="$COMMUNITY_PROTO_ROOT/com/digitalasset/canton/version/v1/untyped_versioned_message.proto"
else
    # From the release artifact use the buf image
    BUF_PROTO_IMAGE="../../scripts/offline-root-key/root_namespace_buf_image.bin"
    export TOPOLOGY_PROTO="$BUF_PROTO_IMAGE"
    export CRYPTO_PROTO="$BUF_PROTO_IMAGE"
    export VERSION_WRAPPER_PROTO="$BUF_PROTO_IMAGE"
fi

SCRIPTS_ROOT="$(dirname "$0")/../../scripts/offline-root-key"
OUTPUT_DIR="${OUTPUT_DIR:=$(mktemp -d)}"
echo "Using $OUTPUT_DIR as temporary directory to write files to."
# File containing the delegation public signing key generated by Canton
CANTON_NAMESPACE_DELEGATION_PUB_KEY="$1"
PRIVATE_KEY="$OUTPUT_DIR/root_private_key.der"
PUBLIC_KEY="$OUTPUT_DIR/root_public_key.der"
ROOT_NAMESPACE_PREFIX="$OUTPUT_DIR/root_namespace"
INTERMEDIATE_NAMESPACE_PREFIX="$OUTPUT_DIR/intermediate_namespace"

# Generate root key pair with openssl
openssl ecparam -name prime256v1 -genkey -noout -outform DER -out "$PRIVATE_KEY"
openssl ec -inform der -in "$PRIVATE_KEY" -pubout -outform der -out "$PUBLIC_KEY" 2> /dev/null

# Prepare certificates for root namespace and namespace delegation
"$SCRIPTS_ROOT/prepare-certs.sh" --root-delegation --root-pub-key "$PUBLIC_KEY" --target-pub-key "$PUBLIC_KEY" --output "$ROOT_NAMESPACE_PREFIX"
"$SCRIPTS_ROOT/prepare-certs.sh" --root-pub-key "$PUBLIC_KEY" --canton-target-pub-key "$CANTON_NAMESPACE_DELEGATION_PUB_KEY" --output "$INTERMEDIATE_NAMESPACE_PREFIX"

# Sign both hashes with the public key
openssl pkeyutl -rawin -inkey "$PRIVATE_KEY" -keyform DER -sign < "$ROOT_NAMESPACE_PREFIX.hash" > "$ROOT_NAMESPACE_PREFIX.signature"
openssl pkeyutl -rawin -inkey "$PRIVATE_KEY" -keyform DER -sign < "$INTERMEDIATE_NAMESPACE_PREFIX.hash" > "$INTERMEDIATE_NAMESPACE_PREFIX.signature"

# Assemble signature and transaction
"$SCRIPTS_ROOT/assemble-cert.sh" --prepared-transaction "$ROOT_NAMESPACE_PREFIX.prep" --signature "$ROOT_NAMESPACE_PREFIX.signature" --signature-algorithm ecdsa256 --output "$ROOT_NAMESPACE_PREFIX.cert"
"$SCRIPTS_ROOT/assemble-cert.sh" --prepared-transaction "$INTERMEDIATE_NAMESPACE_PREFIX.prep" --signature "$INTERMEDIATE_NAMESPACE_PREFIX.signature" --signature-algorithm ecdsa256 --output "$INTERMEDIATE_NAMESPACE_PREFIX.cert"
