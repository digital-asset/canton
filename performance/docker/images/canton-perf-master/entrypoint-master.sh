#!/bin/bash

performance/generate-remote-config.sh additional-config.conf
cat additional-config.conf
./entrypoint.sh
