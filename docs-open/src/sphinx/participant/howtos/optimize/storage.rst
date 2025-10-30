..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Add query cost logging.

.. _optimize_storage:

Optimize Storage
================

General Settings
----------------

.. _max_connection_settings:

Max Connection Settings
~~~~~~~~~~~~~~~~~~~~~~~

The storage configuration can further be tuned using the following additional setting:

::

    canton.participants.<service-name>.storage.parameters.max-connections = X

This allows you to set the maximum number of DB connections used by a Canton
node. If the value is None or non-positive, the value will be the number of
processors. The setting has no effect if the number of connections is already
set via slick options (i.e. storage.config.numThreads).

If you are unsure how to size your connection pools,
`this article <https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing>`_
may be a good starting point.

Generally, the number of connections should be up to two times the number of CPUs on the database machine.

The number of parallel indexer connections can be configured via

::

    canton.participants.<participant-name>.parameters.ledger-api-server-parameters.indexer.ingestion-parallelism = Y

The number ``Z`` of the connections used by the exclusive sequencer writer component is the final parameter that can be controlled.

::

    canton.sequencers.<sequencer-name>.sequencer.high-availability.exclusive-storage.max-connections = Z

A Canton participant node will establish up to ``X + Y + 2`` permanent connections with the database, whereas a synchronizer
will use up to ``X`` permanent connections, except for a sequencer with HA setup that will allocate up to ``2X`` connections. During
startup, the node will use an additional set of at most ``X`` temporary connections during database initialisation.

The number ``X`` represents an upper bound of permanent connections and is divided internally for different purposes,
depending on the implementation. Consequently, the actual size of the write connection pool, for example, could be smaller.
Some of the allotted connections will be taken by the *read* pool, some will be taken by the *Write* pool, and a single additional connection
will be reserved to a dedicated *main* connection responsible for managing the locking mechanism.

The following table summarizes the detailed split of the connection pools in different Canton nodes. ``R`` signifies a *Read* pool, ``W``
a *Write* pool, ``A`` a *Ledger API* pool, ``I`` an *Indexer* pool, ``RW`` a combined *Read/Write* pool, and ``M`` the *Main* pool.

+-----------------------+--------------------+--------------------+--------------------+
|  Node Type            | Enterprise Edition | Enterprise Edition | Community Edition  |
|                       | with Replication   |                    |                    |
+=======================+====================+====================+====================+
| Participant           | | A = X / 2        | | A = X / 2        | | A = X / 2        |
|                       | | R = X / 4        | | R = X / 4        | | RW = X / 2       |
|                       | | W = X / 4 - 1    | | W = X / 4 - 1    | | I = Y            |
|                       | | M = 1            | | M = 1            |                    |
|                       | | I = Y            | | I = Y            |                    |
+-----------------------+--------------------+--------------------+--------------------+
| Mediator              | | R = X / 2        | N/A                | N/A                |
|                       | | W = X / 2 - 1    |                    |                    |
|                       | | M = 1            |                    |                    |
+-----------------------+--------------------+--------------------+--------------------+
| Sequencer             | RW =  X            | N/A                | N/A                |
+-----------------------+--------------------+--------------------+--------------------+
| Sequencer writer      | | R = X / 2        | N/A                | N/A                |
|                       | | W = X / 2 - 1    |                    |                    |
|                       | | M = 1            |                    |                    |
+-----------------------+--------------------+--------------------+--------------------+
| Sequencer             | | R = Z / 2        | N/A                | N/A                |
| exclusive writer      | | W = Z / 2        |                    |                    |
+-----------------------+--------------------+--------------------+--------------------+
| Synchronizer           | N/A                | RW = X             | RW = X            |
+-----------------------+--------------------+--------------------+--------------------+

The results of the divisions are always rounded down unless they yield a zero. In that case, a minimal pool
size of 1 is ascertained.

The values obtained from that formula can be overridden using explicit configuration settings for the *Ledger API* ``A``,
the *Read* ``R``, the *Write* ``W`` pools.

::

    canton.participants.<participant-name>.storage.parameters.connection-allocation.num-reads = R-overwrite
    canton.participants.<participant-name>.storage.parameters.connection-allocation.num-writes = W-overwrite
    canton.participants.<participant-name>.storage.parameters.connection-allocation.num-ledger-api = A-overwrite

Similar parameters exist also for other Canton node types:

::

    canton.sequencers.sequencer.storage.parameters.connection-allocation...
    canton.mediators.mediator.storage.parameters.connection-allocation...

Where a node operates a combined *Read/Write* connection pool, the numbers for ``R`` and ``W`` overwrites are added
together to determine the overall pool size.

The effective connection pool sizes are reported by the Canton nodes at startup.

::

    INFO  c.d.c.r.DbStorageMulti$:participant=participant_b - Creating storage, num-reads: 5, num-writes: 4

.. _queue_size:

Queue Size
~~~~~~~~~~

Canton may schedule more database queries than the database can handle. As a result, these queries
will be placed into the database queue. By default, the database queue has a size of 1000 queries.
Reaching the queueing limit will lead to a ``DB_STORAGE_DEGRADATION`` warning. The impact of this warning
is that the queuing will overflow into the asynchronous execution context and slowly degrade the processing,
which will result in fewer database queries being created. However, for high-performance
setups, such spikes might occur more regularly. Therefore, to avoid the degradation warning
appearing too frequently, the queue size can be configured using:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/storage-queue-size.conf


.. _postgres-configuration:

Postgres
--------

Postgres Configuration
~~~~~~~~~~~~~~~~~~~~~~

For Postgres, `the PGTune online tool <https://pgtune.leopard.in.ua/>`_ is a good starting point for finding reasonable parameters
(use online transaction processing system), but you need to increase the settings of ``shared_buffers``, ``checkpoint_timeout``
and ``max_wal_size``, as explained below.

Beyond the initial configuration, note that most indexes Canton uses are "hash based".
Therefore, read and write access to these indexes is uniformly distributed. However, Postgres reads and writes indexes in
pages of 8kb, while a simple index might only be a couple of writes. Therefore, it is very important to be able to keep the
indexes in memory and only write updates to the disk from time to time; otherwise, a simple change of 32 bytes requires 8kb I/O
operations.

Configuring the ``shared_buffers`` setting to hold 60-70% of the host memory is recommended, rather than the default
suggestion of 25%, as the Postgres caching appears to be more effective than the host-based file access caching.

Also increase the following variables beyond their default: Increase the ``checkpoint_timeout`` so that
the flushing to disk includes several writes and not just one per page, accumulated over time, together with
a higher ``max_wal_size`` to ensure that the system does not prematurely flush before reaching the ``checkpoint_timeout``.
Monitor your system during load testing and tune the parameters accordingly to your use case.
The downside of changing the checkpointing parameters is that crash recovery takes longer.

.. _postgres-performance-tuning:

Sizing and Performance
~~~~~~~~~~~~~~~~~~~~~~

Note that your Postgres database setup requires appropriate tuning to achieve the desired performance. Canton
is database-heavy. This section should give you a starting point for your tuning efforts. You may want to consult
the :ref:`troubleshooting section <how_to_measure_db_performance>` on how to analyze whether the database is a limiting factor.

This guide can give you a starting point for tuning. Ultimately, every use case is different and the exact resource requirements cannot
be predicted, but have to be measured.

First, ensure that the database you are using is appropriately sized for your use case. The number of cores depends on your
throughput requirements. The rule of thumb is:

- 1 db core per 1 participant core.
- 1 participant core for 30-100 ledger events per second (depends on the complexity of the commands).

The memory requirements depend on your data retention period and the size of the data you are storing. Ideally, you monitor the
database index cache hit/miss ratio. If your instance needs to keep on loading indexes from the disk, performance suffers.
It might make sense to start with 128GB, run a long-running scale & performance test, and `monitor the cache hit/miss ratio <https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STATIO-ALL-INDEXES-VIEW>`__.

Most Canton indexes are contract-id based, which means that the index lookups are randomly distributed. Solid state drives with
high throughput perform much better than spinning disks for this purpose.

.. _shared_env_performance:

Predictability of Shared Environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The throughput and latency of a Canton node depends on the performance of the database.
Sharing hardware or software saves cost and better utilizes available resources, but it comes
with some drawbacks: If the database is operated in a shared environment such as the Cloud, where other applications are using the same
database or are operated on the same hardware, the performance of the Canton node varies due to
contention on shared resources. This is a natural effect of shared environments and cannot be entirely
avoided. It can be difficult to diagnose as a user of the shared environment due to lack of visibility into
the other applications and the host system.

If you are operating in a shared environment, you should monitor the performance of the database and expect
a higher variance in latency and throughput.

