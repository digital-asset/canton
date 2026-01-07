..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _declarative_configuration:

Declarative Configuration
=========================

.. note::
    Declarative configuration is an alpha feature and initially only available for participant nodes.

By default, Canton at runtime is administered using API requests for scalability purposes.
For smaller-sized deployments, Canton also supports configuration settings for dynamic changes.
This configuration can add or remove DARs, parties, synchronizers, users, and identity providers,
and can be used as follows:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/declarative-config.conf

Please note that all configuration files will be checked regularly for file modifications, triggering
a reload and applying the changes to the node.

The metrics ``daml.declarative_api.items`` and ``daml.declarative_api.errors`` provide insights
into the amount of items managed through the state configuration file. Negative error codes mean
that something fundamental with the sync did not succeed (-1 if the config file cannot be parsed,
-2 if something failed during the preparation of the sync, and -3 if the sync itself failed fundamentally),
while positive error codes point to a number of items that failed. Anything other than 0 errors means
something is wrong.

Some settings can only be applied if certain preconditions are met. Assigning a party to a user
is only possible if a synchronizer is already registered. Therefore, some of the changes will
only appear once these preconditions are met. However, it is safe to define a target state
with parties before the participant is connected to a synchronizer.

It is also possible to mix declarative and imperative configurations. Excess items defined through
the API will not be deleted unless explicitly configured to do so.
