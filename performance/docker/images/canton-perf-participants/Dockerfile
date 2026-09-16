# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG base_version=3.6
ARG oci_path

FROM ${oci_path}canton-perf-base:${base_version}

RUN cp -v /app/performance/xfer-participants.canton /app/bootstrap.sc

COPY app.conf /app/

USER root
RUN chown -R nonroot:nonroot /app/app.conf
USER nonroot

