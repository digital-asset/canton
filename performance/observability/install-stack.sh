#!/usr/bin/env bash

set -ueo pipefail

SSH_USERNAME=$1
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ENV=db-testing
STACK_FOLDER=/home/canton/observability

cd $SCRIPT_DIR

./prepare-dashboards.sh

ssh $SSH_USERNAME@$ENV.da-int.net -f "sudo mkdir -m 777 -p $STACK_FOLDER"
rsync -avz $SCRIPT_DIR $SSH_USERNAME@$ENV.da-int.net:$STACK_FOLDER
