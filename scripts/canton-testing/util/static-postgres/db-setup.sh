#!/bin/bash

cd $(dirname ${BASH_SOURCE[0]})
source ./db-settings.env


../../../../community/app/src/pack/config/utils/postgres/db.sh reset
