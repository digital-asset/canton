# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM europe-docker.pkg.dev/da-images/public/docker/da-base-image:full-1.0.20@sha256:fba2bea111dc3fc5963bbb44b9eeb9d14b21e82dde0d92b24f809d72885c0bfa

# Create repeatable-migrations directory
WORKDIR /app/repeatable-migrations
WORKDIR /app

COPY target/canton .

COPY LICENSE.txt monitoring.conf parameters.conf storage.conf entrypoint.sh bootstrap-entrypoint.sc tools.sh logback.xml /app/

ARG base_version=latest

# point entrypoint to the amulet executable
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
