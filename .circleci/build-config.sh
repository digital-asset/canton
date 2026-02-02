#!/usr/bin/env bash

set -eu -o pipefail

# get the full path to the directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
CONF="${ABSDIR}/config.yml"
chmod u+w $CONF
echo '# GENERATED FILE, DO NOT EDIT' > $CONF
echo '# If you need to change the CircleCI configuration, edit the relevant fragment in the .circleci/config directory and run `.circleci/build-config.sh`' >> $CONF
circleci config pack "${ABSDIR}/config" >> $CONF
chmod ug-w $CONF
circleci config validate $CONF --org-slug github/DACH-NY
