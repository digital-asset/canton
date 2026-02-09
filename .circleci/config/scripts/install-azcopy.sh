#!/usr/bin/env bash

echo "Checking AzCopy is installed"

if [ "$(azcopy)" ]; then
  echo "azcopy already installed"
else
  set +e
  echo "Installing AZcopy using dpkg"
  curl -sSL -O https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb
  sudo dpkg -i packages-microsoft-prod.deb
  rm packages-microsoft-prod.deb
  sudo apt-get update
  sudo apt-get install azcopy
  set -e
fi
