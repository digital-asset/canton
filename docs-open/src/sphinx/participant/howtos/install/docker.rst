..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Link to howto for downloading docker images.
    Link to hardware and software requirements.
    Explain how to run canton from docker.

.. _install-with-docker:

Work With the Docker Containers
===============================================================

Using the Participant Image
--------------------------------
As a convenience, we provide a pre-configured participant image that can be used to start a Canton participant with sensible defaults.

Using this image requires setting several environment variables:

.. code-block:: bash

    docker run \
        -e AUTH_TARGET_AUDIENCE="audience" \
        -e AUTH_JWKS_URL="fake.com/jwks" \
        -e CANTON_PARTICIPANT_POSTGRES_SERVER="canton-postgres" \
        -e CANTON_PARTICIPANT_POSTGRES_PORT="5432" \
        -v "$(pwd)/bootstrap.sc:/app/bootstrap.sc" \
        --rm -it \
        europe-docker.pkg.dev/da-images/public-unstable/docker/canton-participant:3.5.0-ad-hoc.20251021.17321.0.v2cdf16447

Note that you'll need a valid postgres instance.

This image requires you to initialize the participant via a bootstrap script. See :ref:`Manual identity initialization <setup-manual-identity>` for more details.

To see all configuration defaults and possible env vars, run

.. code-block:: bash

    cid=$(docker create europe-docker.pkg.dev/da-images/public-unstable/docker/canton-participant:3.5.0-ad-hoc.20251021.17321.0.v2cdf1644) && docker cp "$cid":/app/app.conf ./app.conf && docker rm "$cid"
    cat app.conf

Logging
-------------------------------------
Logs are JSON encoded and sent to stdout. The log level is set via `-e LOG_LEVEL_STDOUT=INFO`. It defaults to DEBUG.

Bound Ports
-----------
`canton-participant` image binds:

* Ledger API port: 5001
* Admin API port: 5002
* HTTP Ledger API port: 7575
* GRPC Health Server port: 5061

`canton-mediator` image binds:

* Admin API port: 5007
* GRPC Health Server port: 5061

`canton-sequencer` image binds:

* Public API: 5008
* Admin API: 5009
* GRPC Health Server port: 5061

Supplying custom configuration and DARs
---------------------------------------

To supply custom configuration, either

1. add it via `ADDITIONAL_CONFIG` environment variable, or
2. mount `/app/additional-config.conf` into the container.

Dars must be added dynamically. This is done via remote console or the admin API.

To run a participant console with custom configuration:

.. code-block:: bash

    docker run -e ADDITIONAL_CONFIG="canton.participants.participant1 {
          storage.type = memory
          admin-api.port = 5012
          ledger-api.port = 5011
          http-ledger-api.port = 5013
        }" \
        --rm -it \
        europe-docker.pkg.dev/da-images/public-unstable/docker/canton-participant:3.5.0-ad-hoc.20251021.17321.0.v2cdf1644 \
        --console

The `--console` flag starts canton in interactive console mode.

