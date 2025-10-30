..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _download-docker-images:

Obtaining the Docker Images
===============================

(See :ref:`Install With Docker<install-with-docker>` for instructions on running the Canton Docker images.)

You can download the Canton docker images using:

.. code-block:: bash

    docker pull \
        europe-docker.pkg.dev/da-images/public/docker/canton-base:3.4.0
    docker pull \
        europe-docker.pkg.dev/da-images/public/docker/canton-participant:3.4.0
    docker pull \
        europe-docker.pkg.dev/da-images/public/docker/canton-sequencer:3.4.0
    docker pull \
        europe-docker.pkg.dev/da-images/public/docker/canton-mediator:3.4.0

Visit https://europe-docker.pkg.dev/v2/da-images/public/docker/canton-participant/tags/list to see available tags.

These docker images are published starting with Canton version 3.4.0.

Snapshot releases are available at `/da-images/public-unstable/docker/` instead of `/da-images/public/docker/`, but they are not recommended for any production usage and they are periodically cleaned up.
