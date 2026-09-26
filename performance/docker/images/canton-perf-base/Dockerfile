# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG base_version
ARG oci_path

FROM europe-docker.pkg.dev/da-images/public-unstable/docker/canton-base:3.7

WORKDIR /

RUN mkdir -p /app/performance
COPY target/performance /app/performance
COPY entrypoint-base.sh /app/

USER root
RUN chown -R nonroot:nonroot /app/performance
RUN chown -R nonroot:nonroot /app/entrypoint-base.sh
USER nonroot

# point entrypoint to this shell to deal with pwd issues
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint-base.sh"]
