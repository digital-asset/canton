# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


FROM ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.48@sha256:49353749a3c1bc45fb749aa3eb483b03632ec3d91bc7eb204ad1fa00958e7743 AS grpc-health-probe

FROM eclipse-temurin:21-jdk-noble@sha256:efec1fca48fed530d4727c1ecd9c48d955153bad24067ee43ccf55e6e0d727c7


# Install:
# - screen for running the console in a headless server
# - tini for handling signals and reaping zombie processes
# - libjemalloc2 for debugging memory issues
RUN apt-get update \
   && DEBIAN_FRONTEND=noninteractive apt-get install -y screen tini libjemalloc2 \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/*

COPY --from=grpc-health-probe /ko-app/grpc-health-probe /bin/grpc-health-probe

# create and switch to a working directory
RUN mkdir -p /app/repeatable-migrations
WORKDIR /app

COPY target/canton .

COPY LICENSE.txt monitoring.conf parameters.conf storage.conf entrypoint.sh bootstrap-entrypoint.sc tools.sh logback.xml /app/

ARG base_version=latest

LABEL org.opencontainers.image.ref.name=canton-base:${base_version}


# Add label for security scanners, compliance tools, and auditing purposes
LABEL org.opencontainers.image.base.name="eclipse-temurin:21-jdk-noble@sha256:efec1fca48fed530d4727c1ecd9c48d955153bad24067ee43ccf55e6e0d727c7"

RUN addgroup --gid 1001 nonroot && adduser --uid 1001 --gid 1001 nonroot
RUN chown -R nonroot:nonroot /app
RUN chown nonroot:nonroot bin
USER nonroot

# point entrypoint to the amulet executable
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
