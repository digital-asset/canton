..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _ledger-api-configuration:

gRPC Ledger API Configuration
=============================

The configuration of the gRPC Ledger API is similar to the Admin API configuration, except that the
group starts with ``ledger-api`` instead of ``admin-api``.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/ledger-api-config.conf

To configure the gRPC Ledger API connectivity, you should specify **address** and **port**. All other attributes are
optional and have intuitive default values.

- Define **tls** parameter group to to configure :ref:`TLS <tls-configuration>`
- Define **auth-services** parameter group to to configure :ref:`JWT authorization <jwt-authnz-api-configuration>`
- Define **jwt-timestamp-leeway** parameter group to to configure :ref:`JWT leeway <jwt-leeway-configuration>`
- Define **keep-alive-server** parameter group to to configure :ref:`gRPC keepalive <keepalive-configuration>`
- Define **max-inbound-message-size** parameter to control the :ref:`max size of gRPC messages <max-inbound-message-size-configuration>`

Ledger API Caches
~~~~~~~~~~~~~~~~~

The ``max-contract-state-cache-size`` and ``max-contract-key-state-cache-size`` parameters control the sizes of the
Ledger API contract and contract key caches, respectively. Modifying these parameters changes the likelihood that a
transaction using a contract or a contract key that was recently accessed (created or read) can still find it in the
memory, rather than needing to query it from the database. Larger caches might be of interest when there is a big pool of
ambient contracts that are consistently being fetched or used for non-consuming exercises. Larger caches can also benefit
use cases where a big pool of contracts rotates through a create -> archive -> create-successor cycle.
Consider adjusting these parameters explicitly if the performance of your specific workflow depends on large caches.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/large-ledger-api-cache.conf

Max Transactions in the In-Memory Fan-Out
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``max-transactions-in-memory-fan-out-buffer-size`` parameter controls the size of the in-memory fan-out buffer.
This buffer allows serving the transaction streams from memory as they are finalized, rather than from the database.
Make sure this buffer is large enough so applications are less likely to have to stream transactions
from the database. In most cases, a 10s buffer works well. For example, if you expect a throughput of 20 tx/s,
set this number to 200. The new default setting of 1000 assumes 100 tx/s.
Consider adjusting these parameters explicitly if the performance of your workflow foresees transaction rates larger
than 100 tx/s.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/large-in-memory-fan-out.conf
