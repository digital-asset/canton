# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM europe-docker.pkg.dev/da-images/public/docker/da-base-image:dev-1.0.1@sha256:3b56300baaac65591a9acb6c94ea0bc6416dfee73508c13e587e462093efb590

# Create repeatable-migrations directory
WORKDIR /app/repeatable-migrations
WORKDIR /app

COPY target/canton .

COPY LICENSE.txt monitoring.conf parameters.conf storage.conf entrypoint.sh bootstrap-entrypoint.sc tools.sh logback.xml /app/

ARG base_version=latest

LABEL org.opencontainers.image.ref.name=canton-base:${base_version}

# Add label for security scanners, compliance tools, and auditing purposes
LABEL org.opencontainers.image.base.name="europe-docker.pkg.dev/da-images/public/docker/da-base-image:dev-1.0.1"
LABEL org.opencontainers.image.base.digest="sha256:3b56300baaac65591a9acb6c94ea0bc6416dfee73508c13e587e462093efb590"

# point entrypoint to the amulet executable
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
