..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Cover caching in Ledger API and participant config.

.. _optimize_caching:

Configure Caching
=================

Ledger API Caches
^^^^^^^^^^^^^^^^^

The ``max-contract-state-cache-size`` and ``max-contract-key-state-cache-size`` parameters control the size of the
Ledger API contract and contract key caches, respectively. Modifying these parameters changes the likelihood that a
transaction using a contract or a contract key that was recently accessed (created or read) can still find it in the
memory, rather than needing to query it from the database. Larger caches might be of interest when there is a big pool of
ambient contracts that are consistently fetched or used for non-consuming exercises. Larger caches can also benefit
use cases where a big pool of contracts rotates through a create -> archive -> create-successor cycle.
Consider adjusting these parameters explicitly if the performance of your specific workflow depends on large caches.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/large-ledger-api-cache.conf

