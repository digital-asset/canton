# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM europe-docker.pkg.dev/da-images/public/docker/da-base-image:full-1.0.18@sha256:aa4690faba43c1edd814318a5f095b698fe42c37faa28c69eac02fe40aec63e1

# Create repeatable-migrations directory
WORKDIR /app/repeatable-migrations
WORKDIR /app

COPY target/canton .

COPY LICENSE.txt monitoring.conf parameters.conf storage.conf entrypoint.sh bootstrap-entrypoint.sc tools.sh logback.xml /app/

ARG base_version=latest

# point entrypoint to the amulet executable
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
