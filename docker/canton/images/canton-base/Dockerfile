# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM europe-docker.pkg.dev/da-images/public/docker/da-base-image:full-1.0.17@sha256:61d41e5cd9ea35dd386b05251b8b8d7362736821d542cd6c07d76c7317e817f3

# Create repeatable-migrations directory
WORKDIR /app/repeatable-migrations
WORKDIR /app

COPY target/canton .

COPY LICENSE.txt monitoring.conf parameters.conf storage.conf entrypoint.sh bootstrap-entrypoint.sc tools.sh logback.xml /app/

ARG base_version=latest

# point entrypoint to the amulet executable
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
