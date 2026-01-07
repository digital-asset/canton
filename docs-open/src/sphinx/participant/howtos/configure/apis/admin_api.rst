..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Review and update with missing Admin API specific configuration.
    Link to secure howto for JWT token support for Admin API and service-based restrictions on Admin API tokens.
    Add "Mutual TLS (mTLS) is recommended for the Admin API." with link on how to configure it

.. _admin-api-configuration:

Admin API Configuration
========================

Administration API
------------------
The nature and scope of the Admin API on participant nodes and the Admin API on synchronizers has some overlap. As an example,
you will find the same key management commands on the synchronizer and the participant node API, whereas
the participant has different commands to connect to several synchronizers.

The configuration is currently simple (see the TLS example below) and specifically takes an address and a port.
The address defaults to ``127.0.0.1`` and a default port is assigned if not explicitly configured.

You should not expose the admin-api publicly in an unsecured way as it serves administrative purposes only.

