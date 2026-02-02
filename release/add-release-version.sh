#!/bin/bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

################################################################################

# takes care of hardcoding the release version on demo files before bundling
# this is useful so that users don't need to specify the version when running, instead it will default to the release version

set -u

# get the full path to the directory where this script is
absDir="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

releaseVersion=${1:-dev}

cantonRoot="$absDir/.."
communityPack="$cantonRoot/community/app/src/pack"
tmpPack="$absDir/tmp/pack"

replacementString='s/${CANTON_VERSION:-dev}/${CANTON_VERSION:-'"$releaseVersion"'}/g'

rm -rf "$tmpPack"
mkdir -p "$tmpPack"

if [[ ("$releaseVersion" == *"-SNAPSHOT") || ("$releaseVersion" == "dev") ]]
then
  echo "Skipping version hardcoding for version $releaseVersion"
  exit 0
fi

grep -rlw communityPack -e '${CANTON_VERSION:-dev}' | while read -r file ; do
    relativeFileName=${file#"$communityPack/"}
    sourceFile="communityPack/$relativeFileName"
    generatedFile=$tmpPack/$relativeFileName
    mkdir -p "$(dirname "$generatedFile")"
    sed "$replacementString" "$sourceFile" > "$generatedFile"
done

