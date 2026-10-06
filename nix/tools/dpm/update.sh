#!/usr/bin/env nix-shell
#!nix-shell -i bash -p jq nix
# Usage: ./update.sh <new-version>
set -euo pipefail

cd "$(dirname "$0")"
version="${1:?usage: $0 <version>}"
tmp="$(mktemp)"
cp sources.json "$tmp"

for system in $(jq -r '.platforms | keys[]' sources.json); do
  oci="$(jq -r --arg s "$system" '.platforms[$s].oci' sources.json)"
  url="https://github.com/digital-asset/dpm/releases/download/${version}/dpm-${version}-${oci}.tar.gz"
  echo "Fetching $system ($url)" >&2
  hash="$(nix-prefetch-url --type sha256 "$url")"
  jq --arg s "$system" --arg h "$hash" '.platforms[$s].sha256 = $h' "$tmp" > "$tmp.new" && mv "$tmp.new" "$tmp"
done

jq --arg v "$version" '.version = $v' "$tmp" > sources.json
rm -f "$tmp"
echo "Updated sources.json to $version" >&2
