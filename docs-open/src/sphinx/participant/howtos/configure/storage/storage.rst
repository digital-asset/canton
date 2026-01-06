..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _storage-config:

Configure storage
=================

Canton needs to persist data to operate. Participant Nodes, Mediator Nodes, and Synchronizer Nodes all require storage configuration.

.. note::

    Canton creates, manages and upgrades the database schema directly on startup.

Configure production storage
-----------------------------
For production deployments, the only supported storage is `PostgreSQL <https://www.postgresql.org/>`_.  Please see :ref:`Configure Canton with Postgres <postgres-config>` for details on how to configure it.

Other storage configurations
----------------------------

.. warning::

   The following storage configurations are not supported for production deployments. They are provided for testing, development, and experimental purposes only.

Configure in-memory storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, in-memory storage is used if no other storage configuration is provided. With in-memory storage, in place of a relational database, in-memory data structures store the data. A consequence of this is that all data is lost when the node is stopped.

This example shows explicit in-memory configuration for a Sequencer, Mediator, and Participant Node:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/storage-in-memory.conf

Configure H2 storage
~~~~~~~~~~~~~~~~~~~~

This example shows `H2 <https://www.h2database.com/>`_ storage configuration for a Sequencer, Mediator, and Participant Node:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/storage-h2.conf

The ``jdbc:`` prefixed ``url`` can be `configured in many ways <https://www.h2database.com/html/features.html#database_url>`_. The example above configures:

Embedded file based storage
    On canton startup embedded H2 databases will be created in the ``./data`` directory relative to the Canton node's working directory and will have the file suffix ``.mv.db``.
    When using H2 `embedded connection mode <https://www.h2database.com/html/features.html#connection_modes>`_ only a single process can access the database at a time, so to inspect the database, it will be necessary to stop Canton first.
    Once no other process is accessing the database, it can be inspected using the H2 tools, such as the `H2 Console <https://www.h2database.com/html/quickstart.html#h2-console>`_.

MODE=PostgreSQL
    This is essential as the SQL dialect used by Canton is PostgreSQL. This setting enables `PostgreSQL compatibility mode <https://www.h2database.com/html/features.html#postgresql_mode>`_.

LOCK_TIMEOUT=10000
    This gives a thread 10 seconds to acquire a database lock before timing out.

DB_CLOSE_DELAY=-1
    This setting keeps the database open until the Canton process terminates.



