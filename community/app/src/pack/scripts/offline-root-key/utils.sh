#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

CURRENT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)
BUF_PROTO_IMAGE=${BUF_PROTO_IMAGE:="${CURRENT_DIR}/root_namespace_buf_image.json.gz"}

# Source the transaction utility script
source "$CURRENT_DIR/../topology/topology_util.sh"

# Check for required tools and print their versions
check_tool() {
  local tool="$1"
  local version_command="$2"
  if ! command -v "$tool" &>/dev/null; then
    echo "Error: Required tool '$tool' is not installed."
    exit 1
  fi
  version=$(eval "$version_command" 2>&1 | head -n 1)
  echo "$tool version: $version"
}

# Function to detect key spec automatically from the public key
detect_key_spec() {
  local der_file="$1"
  local curve

  local output

  output=$(openssl pkey -pubin -inform DER -in "$der_file" -text -noout 2>/dev/null)

  # Check for ML-DSA-65 first (OID: 2.16.840.1.101.3.4.3.17)
  if echo "$output" | grep -qi "ml-dsa-65\|mldsa-65\|2.16.840.1.101.3.4.3.17"; then
    echo "SIGNING_KEY_SPEC_ML_DSA_65"
    return
  fi

  # Check for ED25519
  if echo "$output" | grep -qi "ED25519"; then
    echo "SIGNING_KEY_SPEC_EC_CURVE25519"
    return
  fi

  # Extract ASN1 OID for elliptic curves
  curve=$(echo "$output" | grep 'ASN1 OID' | awk -F': ' '{print $2}')

  case "$curve" in
    prime256v1)
      echo "SIGNING_KEY_SPEC_EC_P256"
      ;;
    secp384r1)
      echo "SIGNING_KEY_SPEC_EC_P384"
      ;;
    secp256k1)
      echo "SIGNING_KEY_SPEC_EC_SECP256K1"
      ;;
    *)
      echo "UNKNOWN"
      ;;
  esac
}

# Writes to stderr
err() {
  echo "$1" >&2
}
