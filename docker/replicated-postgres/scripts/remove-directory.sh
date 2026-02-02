#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

if [[ $REQUEST_CONFIRMATION_FOR_SUDO_RM == "yes" ]] ; then
  sudo_rm_args="-I"
else
  sudo_rm_args="-f"
fi

rm -rf "$1" || (echo -e "Deleting ${GREEN}$1${NC} ${RED}as root${NC}..." && sudo rm -r "$sudo_rm_args" "$1")
