# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


FROM fullstorydev/grpcurl:v1.9.3@sha256:085e183ca334eb4e81ca81ee12cbb2b2737505d1d77f5e33dabc5d066593d998 AS grpcurl

FROM eclipse-temurin:21-jdk-noble@sha256:71f2d18eea2e834fad8b6b8d5c7ae6effca37b97f3a3da2df5de932ee540c060


# Install:
# - screen for running the console in a headless server
# - tini for handling signals and reaping zombie processes
# - libjemalloc2 for debugging memory issues
RUN apt-get update \
   && DEBIAN_FRONTEND=noninteractive apt-get install -y screen tini libjemalloc2 \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/*

COPY --from=grpcurl /bin/grpcurl /bin/grpcurl

# create and switch to a working directory
RUN mkdir /app
WORKDIR /app

COPY target/canton .

COPY LICENSE.txt monitoring.conf parameters.conf storage.conf entrypoint.sh bootstrap-entrypoint.sc tools.sh logback.xml /app/

ARG base_version=latest

LABEL org.opencontainers.image.ref.name=canton-base:${base_version}


# Add label for security scanners, compliance tools, and auditing purposes
LABEL org.opencontainers.image.base.name="eclipse-temurin:21-jdk-noble@sha256:5a65f334da5a91a66076735d78e3ae30483a2593ac108f830dcd59521f2535cd"

RUN addgroup --gid 1001 nonroot && adduser --uid 1001 --gid 1001 nonroot
RUN chown -R nonroot:nonroot /app
RUN chown nonroot:nonroot bin
USER nonroot

# point entrypoint to the amulet executable
ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
