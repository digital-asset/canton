..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Review and update.

.. _canton_upgrading:

Upgrade To a New Release
========================

This section covers the processes to upgrade Canton participant nodes and synchronizers.
Upgrading Daml applications is covered `elsewhere <https://docs.daml.com/upgrade/upgrade.html>`_.

As elaborated in the :ref:`versioning guide <canton_versioning>`, new features, improvements
and fixes are released regularly. To benefit from these changes, the Canton-based system
must be upgraded.

There are two key aspects that need to be addressed when upgrading a system:

- :ref:`Upgrading the Canton binary<upgrade_canton_binary>` that is used to run a node.
- :ref:`Upgrading the protocol version<change_pv>` that defines how nodes interact with each other.

Canton is a distributed system, where no single operator controls all nodes. Therefore,
we must support the situation where nodes are upgraded individually, providing a safe upgrade
mechanism that requires the minimal amount of synchronized actions within a network.

A Canton binary supports :ref:`multiple protocol versions <protocol_version>`, and new protocol
versions are introduced in a backward-compatible way with a new binary
(see :ref:`version table <release-version-to-protocol-version-table>`). Therefore, any upgrade
of a protocol used in a distributed Canton network is done by individually upgrading all binaries
and subsequently changing the protocol version used among the nodes to the
desired one.

The following is a general guide. Before upgrading to a specific version, please check the
individual notes for each version.

This guide also assumes that the upgrade is a minor or a patch release. Major release upgrades might
differ and will be covered separately if necessary.

.. warning::

    Upgrading requires care and preparation.
      * **Please back up your data before any upgrade.**

        Note that the :ref:`order of the backups <order-of-backups>` is important.

      * **Please test your upgrade thoroughly before attempting to upgrade your production system.**

      * **Once a migration has started it cannot be aborted or rolled back.**

        If you encounter an issue, you must restore the old version using the backups.

.. _upgrade_canton_binary:

Upgrade Canton Binary
---------------------

A Canton node consists of one or more processes, where each process is defined by

- A Java Virtual Machine application running a versioned JAR of Canton.
- A set of configuration files describing the node that is being run.
- An optional bootstrap script passed via ``--boostrap``, which runs on startup.
- A database (with a specific schema), holding the data of the node.

To upgrade the node,

#. Replace the Canton binary (which contains the Canton JAR).
#. Test that the configuration files can still be parsed by the new process.
#. Test that the bootstrap script you are using is still working.
#. Upgrade the database schema.

Generally, all changes to configuration files should be backward compatible, and therefore not
be affected by the upgrade process. In rare cases, there might be a minor change to the configuration
file necessary in order to support the upgrade process. Sometimes, fixing a substantial bug
might require a minor breaking change to the API. The same applies to Canton scripts.

The schema in the database is versioned and managed using `Flyway <https://flywaydb.org/>`_. Detecting
and applying changes is done by Canton using that library. Understanding this background can be helpful
to troubleshoot issues.

Preparation
~~~~~~~~~~~

First, please download the new Canton binary that you want to upgrade to and store it on the test
system where you plan to test the upgrade process.

Then, obtain a recent backup of the database of the node and deploy it to a database server
of your convenience, such that **you can test the upgrade process without affecting your production system**.
While we extensively test the upgrade process ourselves, we cannot exclude the eventuality that you are using the system
in a non-anticipated way. Testing is cumbersome, but breaking a production system is worse.

If you are upgrading a participant, then we suggest that you also use an in-memory synchronizer which you can
tear down after you have tested that the upgrade of the participant is working. You might do that by adding
a simple synchronizer definition as a configuration mixin to your participant configuration.

Generally, if you are running a high-availability setup, please take all nodes offline before
performing an upgrade. If the update requires a database migration (check the release notes), avoid
running older and newer binaries in a replicated setup, as the two binaries might expect a different
database layout.

You can upgrade the binaries of a microservice-based synchronizer in any order, as long as you upgrade
the binaries of nodes accessing the same database at the same time. For example, you could upgrade
the binary of a replicated mediator node on one weekend and an active-active database sequencer on
another weekend.

Back Up Your Database
~~~~~~~~~~~~~~~~~~~~~

Before you upgrade the database and binary, please ensure that you have backed up your data,
such that you can roll back to the previous version in case of an issue. You can back up your
data by cloning it. In Postgres, the command is:

.. code:: sql

    CREATE DATABASE newdb WITH TEMPLATE originaldb OWNER dbuser;

When doing this, you need to change the database name and user name in above command to match
your setup.

.. _test-your-config:

Test your Configuration
~~~~~~~~~~~~~~~~~~~~~~~

Test that the configuration still works

.. code-block:: bash

    ./bin/canton -v -c storage-for-upgrade-testing.conf -c mynode.conf --manual-start

Here, the files ``storage-for-upgrade-testing.conf`` and ``mynode.conf`` need to be adjusted
to match your case.

If Canton starts and shows the command prompt of the console, then the configuration was
parsed successfully.

The command line option ``--manual-start`` prevents the node from starting up automatically,
as we first need to migrate the database.

.. _migrating_the_database:

Migrating the Database
~~~~~~~~~~~~~~~~~~~~~~

Canton does not perform a database migration automatically. Migrations
need to be forced. If you start a node that requires a database migration, you will
observe the following Flyway error:

.. snippet:: migrating_participant
    .. failure:: participant.start()

The database schema definitions are versioned and hashed. This error informs us about the current
database schema version and how many migrations need to be applied. This check runs at
startup, so if the node starts, the migrations was successful.

We can now force the migration to a new schema using:

.. snippet:: migrating_participant
    .. success:: participant.db.migrate()

You can also :ref:`configure the migrations to be applied automatically <migrate_and_start_mode>`.
Please note that you need to ensure that the user account the node is using to access the database
allows to change the database schema. How long the migration takes depends on the version
of the binary (see migration notes), the size of the database and the performance of the database server.

We recommend cleaning up your database before you start your node. On Postgres, run

.. code:: sql

    VACUUM FULL;

Otherwise, the restart may take a long time while the database is cleaning itself up.

Subsequently, you can successfully start the node

.. snippet:: migrating_participant
    .. success:: participant.start()

Please note that the procedure remains the same for all other types of nodes,
with a participant node used here as an example.

Test Your Upgrade
~~~~~~~~~~~~~~~~~

Once your node is up and running, you can test it by running a ping. If you are testing
the upgrade of your participant node, then you might want to connect to the test synchronizer

.. snippet:: migrating_participant
    .. success:: participant.synchronizers.connect_local(sequencer1, "testsynchronizer")

If you did the actual upgrade of the production instance, then you would just reconnect
to the current synchronizer before running the ping:

.. snippet:: migrating_participant
    .. success:: participant.synchronizers.reconnect_all()

You can check that the synchronizer is up and running using

.. snippet:: migrating_participant
    .. success:: participant.synchronizers.list_connected()
    .. assert:: RES.length == 1

Finally, you can ping the participant to see if the system is operational

.. snippet:: migrating_participant
    .. success:: participant.health.ping(participant)

The ping command creates two contracts between the admin parties, then exercises and archives them -- providing an end-to-end test of ledger functionality.

Version Specific Notes
~~~~~~~~~~~~~~~~~~~~~~

Currently, nothing.

.. _change_pv:

Change the Canton Protocol Version
----------------------------------

The Canton protocol is defined by the semantics and the wire format used by the nodes
to communicate to each other. In order to process transactions, all nodes must be able
to understand and speak the same protocol.

Therefore, a new protocol can be introduced only once all nodes have been upgraded
to a binary that can run the version.

.. _canton_synchronizer_protocol_version_upgrading:

Upgrade the Synchronizer to a new Protocol Version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A synchronizer is tied to a protocol version. This protocol version is configured when
the synchronizer is initialized and cannot be changed afterward. Therefore, **you can
not upgrade the protocol version of a synchronizer**. Instead, you deploy a new synchronizer
side by side with the old synchronizer process.

This applies to all synchronizer services, be it sequencer, mediator, or topology manager.

Please note that currently, the synchronizer id cannot be preserved during upgrades.
The new synchronizer must have a different synchronizer id because the participant
associates a synchronizer connection with a synchronizer id, and that association
must be unique.

Therefore, the protocol upgrade process boils down to:

- Deploy a new synchronizer next to the old synchronizer. Ensure that the new synchronizer is using the desired protocol version.
  Also make sure to use different databases (or at least different schemas in the same database)
  for the synchronizer services (mediator, sequencer node, and topology manager), channel names, smart contract addresses, etc.
  The new synchronizer must be completely separate, but you can reuse your DLT backend as long
  as you use different sequencer contract addresses or Fabric channels.
- Instruct the participants individually using the hard synchronizer migration to use the new synchronizer.

Note: to use the same database with different schemas for the old and the new synchronizer, set the `currentSchema` either in the JDBC URL or as a parameter in `storage.config.properties`.

Hard Synchronizer Connection Upgrade
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A hard synchronizer connection upgrade can be performed using the :ref:`respective migration command <repair.migrate_synchronizer>`.
Again, please ensure that you have appropriate backups in place and that you have tested this procedure before applying
it to your production system. You will have to enable these commands using a special config switch:

.. code:: text

    canton.features.enable-repair-commands=yes

Assuming that you have several participants all connected to a synchronizer named ``oldsynchronizer``,
ensure that there are no pending transactions.
You can do that by either controlling your applications, or by
:ref:`setting the resource limits <resources.set_resource_limits>` to 0 on all participants:

.. snippet:: migrating_protocol
    .. assert:: { participant.db.migrate(); true }
    .. assert:: { participant.start(); true }
    .. assert:: { participant.synchronizers.connect_local(sequencer2, "oldsynchronizer"); true }
    .. success:: participant.resources.set_resource_limits(ResourceLimits(Some(0), Some(0)))

This rejects any new command and finishes processing the pending commands. Once you are sure that
your participant node is idle, disconnect the participant node from the old synchronizer:

.. snippet:: migrating_protocol
    .. success:: participant.synchronizers.disconnect("oldsynchronizer")

Test that the synchronizer is disconnected by checking the list of active connections:

.. snippet:: migrating_protocol
    .. success:: participant.synchronizers.list_connected()
    .. assert:: participant.synchronizers.list_connected().isEmpty

This is a good time to perform a backup of the database before proceeding:

.. code:: sql

    CREATE DATABASE newdb WITH TEMPLATE originaldb OWNER dbuser;

Next, we want to run the migration step. For this, we need to run the ``repair.migrate_synchronizer`` command.
The command expects two input arguments: The alias of the source synchronizer and a synchronizer connection
configuration describing the new synchronizer.

In order to build a synchronizer connection config, we can just type

.. snippet:: migrating_protocol
    .. success(output=5):: val config = SynchronizerConnectionConfig("newsynchronizer", GrpcSequencerConnection.tryCreate("https://127.0.0.1:5018"))

where the URL should point to the correct synchronizer. If you are testing the upgrade
process locally in a single Canton process using a target synchronizer named ``newsynchronizer`` (which is
what we are doing in this example), you can grab the connection details using

.. snippet:: migrating_protocol
    .. success(output=5):: val config = SynchronizerConnectionConfig("newsynchronizer", SequencerConnections.single(sequencer3.sequencerConnection))

Now, using this configuration object, we can trigger the hard synchronizer connection migration using

.. snippet:: migrating_protocol
    .. success:: participant.repair.migrate_synchronizer("oldsynchronizer", config)

This command will register the new synchronizer and re-associate the contracts tied to ``oldsynchronizer`` to
the new synchronizer.

Once all participants have performed the migration, they can reconnect to the synchronizer

.. snippet:: migrating_protocol
    .. success:: participant.synchronizers.reconnect_all()

Now, the new synchronizer should be connected:

.. snippet:: migrating_protocol
    .. success:: participant.synchronizers.list_connected()
    .. assert:: participant.synchronizers.list_connected().map(_.synchronizerAlias.unwrap) == Seq("newsynchronizer")

As we've previously set the resource limits to 0, we need to reset this back

.. snippet:: migrating_protocol
    .. success:: participant.resources.set_resource_limits(ResourceLimits(None, None))

Finally, we can test that the participant can process a transaction by running a ping on the new synchronizer

.. snippet:: migrating_protocol
    .. success:: participant.health.ping(participant)

.. note::

    Note that currently, the hard migration is the only supported way to migrate a production system.
    This is because unique contract keys are restricted to a single synchronizer.

While the synchronizer migration command is mainly used for upgrading, it can also be used to recover
contracts associated with a broken synchronizer. Synchronizer migrations can be performed back and forth,
allowing you to roll back in case of issues.

After the upgrade, the participants may report a mismatch between commitments during the first commitment
exchange, as they might have performed the migration at slightly different times. The warning should
eventually stop once all participants are back up and connected.

Expected Performance
~~~~~~~~~~~~~~~~~~~~

Performance-wise, we can note the following: when we migrate contracts, we write directly into
the respective event logs. This means that on the source synchronizer, we insert a transfer-out, while
we write a transfer-in and the contract into the target synchronizer. Writing this information is substantially
faster than any kind of transaction processing (several thousand migrations per second on a
single CPU/16-core test server). However, with very large datasets, the process can
still take quite some time. Therefore, we advise you to measure the time the migration takes during
the upgrade test to understand the necessary downtime required for the migration.

Furthermore, upon reconnecting, the participant needs to recompute the new set of commitments. This can take
a while for large numbers of contracts.

Soft Synchronizer Connection Upgrade
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note ::

    The soft synchronizer connection upgrade is currently only supported as an alpha feature.

The hard synchronizer connection upgrade requires coordination among all participants in a network. The
soft synchronizer connection upgrade is operationally much simpler, and can be leveraged using multi-synchronizer
support (which exists as a pre-alpha feature only for now). By turning off non-unique contract keys,
participants can connect to multiple synchronizers and transfer contracts between synchronizers. This allows us to avoid using the ``repair.migrate_synchronizer`` step.

Assuming the same setup as before, where the participant is connected to the old synchronizer,
we can just connect it to the new synchronizer

.. snippet:: soft_migration_with_transfer
    .. assert:: { participant.db.migrate(); true }
    .. assert:: { participant.start(); true }
    .. assert:: { participant.synchronizers.connect_local(sequencer2, "oldsynchronizer"); true }
    .. success:: participant.synchronizers.connect_local(sequencer3, "newsynchronizer")
    .. assert:: participant.synchronizers.list_connected().map(_.synchronizerAlias.unwrap).toSet == Set("newsynchronizer", "oldsynchronizer")

Give the new connection precedence over the old connection by changing the ``priority`` flag of the new
synchronizer connection:

.. snippet:: soft_migration_with_transfer
    .. success:: participant.synchronizers.modify("newsynchronizer", _.copy(priority=10))

You can check the priority settings of the synchronizers using

.. snippet:: soft_migration_with_transfer
    .. success:: participant.synchronizers.list_registered().map { case (c, _, _) => (c.synchronizerAlias, c.priority) }

Existing contracts will not automatically move over to the new synchronizer. The synchronizer router will
pick the synchronizer by minimizing the number of transfers and the priority. Therefore, most contracts
will remain on the old synchronizer without additional action. However, by using the

.. todo:: `#22917: Fix broken ref <https://github.com/DACH-NY/canton/issues/22917>`_
    ref:`transfer command <transfer.execute>`

, contracts can be moved over to the new synchronizer
one by one, such that eventually, all contracts are associated with the new synchronizer, allowing
the old synchronizer to be decommissioned and turned off.

The soft upgrade path provides a smooth user experience that does not require a hard migration of the synchronizer connection to be
coordinated across all participants. Instead, participants upgrade individually, whenever they
are ready, allowing them to reverse the process if needed.
