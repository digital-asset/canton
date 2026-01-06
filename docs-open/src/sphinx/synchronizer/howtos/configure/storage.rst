..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Link to general storage config in participant docs.
    Cover synchronous replication for sequencers.

.. _sync-storage-config:

Configure Storage
=================

Visit :externalref:`storage-config` to learn how to set up basic storage configuration for the synchronizer.

Synchronous replication (Postgres)
----------------------------------

As explained at :ref:`synchronizer-database_replication_dr`, it is important that you set up synchronous replication
for the synchronizer.

To set up synchronous replication, configure the primary server to accept connections from standby servers
and specify which standbys should be synchronous. This ensures that transactions are committed only after confirmation
from those standbys.

Refer to the official `Postgres documentation <https://www.postgresql.org/docs/current/warm-standby.html#SYNCHRONOUS-REPLICATION>`_
to configure synchronous replication in your environment.

