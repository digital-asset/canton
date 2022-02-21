#!/bin/bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

cd $(dirname $0)
cd ../../../src/pack/deployment/docker

export CANTON_VERSION=${CANTON_VERSION:-"dev"}
export BASE_PORT="80"

# figure out what our current SDK version is
EXPECTED_VERSION=$(cat ../../../../../../project/project/DamlVersions.scala | grep "val version =" | awk '{print $4}' | tr -d '"')
if [ -z $EXPECTED_VERSION ]; then
    echo "unable to determine expected version"
    exit 1
fi

echo "Expected versions Daml: $EXPECTED_VERSION Canton: $CANTON_VERSION"

# fixing permissions for docker on CI
cd data
mkdir -p shared
mkdir -p logs
chmod 777 shared logs
rm -f shared/*
cd ..

# wipe any preexisting state from docker; primarily useful when running locally - not in CI
docker container prune -f && docker volume prune -f

docker-compose -version

# pull non canton images. canton images will be generated locally in CI.
docker-compose ps --services | grep -Ev 'connect.(node|cleanup)' | xargs docker-compose pull

# if you need to deploy what is happening here, remove the `-d`. the test will fail but you will see the errors
# alternatively, have a look at the log artifacts
docker-compose up -d

cd data

echo "waiting for system to come up"
./utils/wait_for_file.sh shared/json.port
echo "json api appeared"

# --------------------------------------------
# test json api and full deployment by creating a contract
# --------------------------------------------
function find_party {
  local party=$1
  grep -E "^${party}" shared/parties.txt | tail -n 1 | awk '{print $2}'
}

MYSTRING="My greatest test so far."

party=$(find_party "alice")
cat > cmd.txt <<EOF
{
  "templateId" : "PingPong:Cycle",
  "payload" : {
    "owner" : "${party}",
    "id" : "${MYSTRING}"
  }
}
EOF

curl -X POST \
  -H "Content-Type: application/json" -H "Authorization: Bearer $(cat shared/alice.jwt)" \
  -d @- localhost:${BASE_PORT}01/v1/create < cmd.txt

function test_contract_presence {
  local retries=$1
  ret=1
  while [ $ret -ne 0 ]; do
    sleep 1
    curl -X GET \
      -H "Content-Type: application/json" -H "Authorization: Bearer $(cat shared/alice.jwt)" \
      localhost:${BASE_PORT}01/v1/query > tmp.txt
    grep -q "${MYSTRING}" tmp.txt
    ret=$?
    if [ $ret -ne 0 ]; then
      echo "contract not found yet"
      let retries=$retries-1
      if [ $retries -eq 0 ]; then
        echo "giving up!"
        exit 1
      fi
    fi
  done
  echo "contract has been found ..."

}

test_contract_presence 30

rm tmp.txt cmd.txt

# ------------------------------------------------
# check if navigator and trigger service are running
# ------------------------------------------------
./utils/wait_until_alive.sh localhost ${BASE_PORT}00
./utils/wait_until_alive.sh localhost ${BASE_PORT}02
./utils/wait_until_alive.sh localhost ${BASE_PORT}11
./utils/wait_until_alive.sh localhost ${BASE_PORT}32

cd ../

# --------------------------------------------------
# check that we are running the right SDK version
# --------------------------------------------------
USED_VERSION=$(docker-compose exec connect.navigator daml version | grep default | awk '{print $1}')
if [ "$USED_VERSION" != "$EXPECTED_VERSION" ]; then
    echo -e "\033[0;31m ⚠️⚠️⚠️ WARNING: Until #8353 is addressed, docker-compose is still referring to SDK version $USED_VERSION, while the project is based on $EXPECTED_VERSION ⚠️⚠️⚠️ \033[0m"
    # TODO(#8353): daml-repo ("split") releases contain neither the daml-sdk nor the daml-sdk docker images used by this test
    # exit 1
fi


docker-compose down

# ------------------------------------------------
# check if we can restart the system
# ------------------------------------------------
docker-compose up -d

cd data

./utils/wait_for_file.sh shared/json.port

./utils/wait_until_alive.sh localhost ${BASE_PORT}00
./utils/wait_until_alive.sh localhost ${BASE_PORT}02

echo "checking if the contract is still there"
test_contract_presence 0

docker-compose down
