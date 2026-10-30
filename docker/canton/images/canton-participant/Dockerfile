# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG base_version=latest
ARG oci_path

FROM ${oci_path}canton-base:${base_version}

ARG base_version=latest

EXPOSE 5001
EXPOSE 5002
EXPOSE 7575
EXPOSE 10013

COPY app.conf /app/

# -Y off: busybox wget ignores no_proxy, so a proxy injected via the environment would break the probe.
HEALTHCHECK --start-period=10m \
  CMD wget -Y off -q -T 5 -O /dev/null http://localhost:5071/health/ready || exit 1
