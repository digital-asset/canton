# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG base_version=latest
ARG oci_path

FROM ${oci_path}canton-perf-base:${base_version}

ARG base_version=latest
LABEL org.opencontainers.image.base.name=canton-perf-base:${base_version}
LABEL org.opencontainers.image.ref.name=canton-perf-master:${base_version}

COPY app.conf /app/
