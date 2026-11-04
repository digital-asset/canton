# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM europe-docker.pkg.dev/da-images/public/docker/da-base-image:full-1.0.19@sha256:48cca37f4c0ec2b85fe54b9b572877444fe84ad78387858cfcdc2b14adf56f83

# Create repeatable-migrations directory
WORKDIR /app/repeatable-migrations
WORKDIR /app

COPY target/canton .

COPY LICENSE.txt monitoring.conf parameters.conf storage.conf entrypoint.sh bootstrap-entrypoint.sc tools.sh logback.xml /app/

ARG base_version=latest

# point entrypoint to the amulet executable
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
