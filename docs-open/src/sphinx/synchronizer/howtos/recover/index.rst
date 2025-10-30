..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Deduplicate and link with backup&restore in participant docs.
    Cover synchronous replication in storage config for sequencers.

.. _synchronizer-backup-and-restore:

Backup and Restore
==================

It is recommended that your database is frequently backed up so that the data can be restored in case of a disaster.

In the case of a restore, a participant can replay missing data from the synchronizer
as long as the synchronizer's backup is more recent than that of the participant's.

.. todo::
  #. `Ability to recover from partial data loss on a synchronizer <https://github.com/DACH-NY/canton/issues/4839>`_.

.. _synchronizer-order-of-backups:

Order of backups
----------------

It is important that the participant's backup is not more recent than that of
the sequencer's, as that would constitute a ledger fork. Therefore, if you back up
both participant, mediator and sequencer databases sequentially, the following constraints apply:

- Back up the mediators and participants before the sequencer;
  otherwise, they may not be able to reconnect to the sequencer (``ForkHappened``).
  The relative order of mediators, and participants does not matter.


If you perform a complete system backup in a single step (for example, using
a cloud RDS), make sure no component writes to the database while the backup is in progress.

In case of a synchronizer restore from a backup, if a participant is ahead of the
synchronizer the participant will refuse to connect to the synchronizer (``ForkHappened``) and you must
either:

- restore the participant's state to a backup before the disaster of the synchronizer, or
- roll out a new Synchronizer as a repair strategy in order to :externalref:`recover from a lost Synchronizer <recovering_from_lost_synchronizer>`

The state of applications that interact with a participant's Ledger API must be
backed up before the participant, otherwise the application state has to be
reset.

.. _synchronizer-restore_caveats:

Restore caveats
---------------

When restoring Canton nodes from a backup, the following caveats apply due to
the loss of data between the point of backup and latest state of the nodes.

Incomplete Command Deduplication State
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After the restore, the participant's in-flight submission tracking will be out
of sync with what the participant has sent to the sequencer after the backup was
taken. If an application resubmits a duplicate command it may get accepted even
though it should have been deduplicated by the participant.

This tracking will be in sync again when:

 - the participant has processed all events from the sequencer, and
 - no queue on the sequencer includes any submission request of a transfer/transaction
   request from before the restore that could be sequenced again

Such submission requests have a max sequencing time of the ledger time plus the
ledger-time-record-time-tolerance of the synchronizer. It should be enough to observe
a timestamp from the synchronizer that is after the time when the participant was
stopped before the restore by more than the tolerance. Once such a timestamp is
observed, the in-flight submission tracking is in sync again and applications
can resume submitting commands with full command deduplication guarantees.

Application State Reset
^^^^^^^^^^^^^^^^^^^^^^^

If the application's state is newer than the participant's state, either because
the application was backed up after the participant or because the application is run by
a different organization and wasn't restored from a backup, then the application
state has to be reset. Otherwise the application has already requested and
processed transactions that were lost by the participant due to the gap between when
the backup was taken and when the node disaster happened.

This includes all applications that are Ledger API clients of the participant.

Private Keys
^^^^^^^^^^^^

Assume a scenario in which a node needs to rotate its cryptographic private key, which is
currently stored in the database of the node. If the key rotation has been
announced in the system before a backup has been performed, the new key will not
be available on a restore, but all other nodes in the system expect the new key
to be used.

To avoid this situation, perform the key rotation steps in this order:

#. Generate the new private key and store it in the database
#. Back up the database
#. Once the backup is complete, revoke the previous key


Postgres example
----------------

If you are using Postgres to persist the Participant Node or Synchronizer data, you can create backups to a file and restore it using Postgres's utility commands ``pg_dump`` and ``pg_restore`` as shown below:

Backing up Postgres database to a file:

.. code-block:: bash

    pg_dump -U <user> -h <host> -p <port> -w -F tar -f <fileName> <dbName>

Restoring Postgres database data from a file:

.. code-block:: bash

    pg_restore -U <user> -h <host> -p <port> -w -d <dbName> <fileName>

Although the approach shown above works for small deployments, it is not recommended for larger deployments.
For that, we suggest looking into incremental backups and refer to the resources below:

- `PostgreSQL Documentation: Backup and Restore <https://www.postgresql.org/docs/current/backup.html>`_
- `How incremental backups work in PostgreSQL <https://kcaps.medium.com/how-incremental-backups-work-in-postgresql-and-how-to-implement-them-in-10-minutes-d3689e8414d9>`_

.. _synchronizer-database_replication_dr:

Database Replication for Disaster Recovery
------------------------------------------

Synchronous replication
^^^^^^^^^^^^^^^^^^^^^^^

We recommend that in production at least the synchronizer should be run with offsite
synchronous replication to ensure that the state of the synchronizer is always newer
than the state of the participants. However to avoid similar
:externalref:`caveats as with backup restore <restore_caveats>` the participants should either use synchronous
replication too or as part of the manual disaster recovery failure procedure the
caveats have to be addressed.

A database backup allows you to recover the ledger up to the point when the last backup was created.
However, any command accepted after creation of the backup may be lost in case of a disaster.
Therefore, restoring a backup will likely result in data loss.

If such data loss is unacceptable, you need to run Canton against a replicated
database, which replicates its state to another site. If the original site is
down due to a disaster, Canton can be started in the other site based on the
replicated state in the database. It is crucial that there are no writers left
in the original site to the database, because the database mechanism used in
Canton to avoid multiple writers and thus avoid data corruption does not work
across sites.

For detailed instructions on how to setup a replicated database and how to perform failovers, we refer to the database system documentation,
e.g. `the high availability documentation <https://www.postgresql.org/docs/current/high-availability.html>`_ of PostgreSQL.

**It is strongly recommended to configure replication as synchronous.**
That means, the database should report a database transaction as successfully committed only after it has been persisted to all database replicas.
In PostgreSQL, this corresponds to the setting ``synchronous_commit = on``.
If you do not follow this recommendation, you may observe data loss and/or a
corrupt state after a database failover. Enabling synchronous replication
may impact the performance of Canton depending on the network latency between
the primary and offsite database.

For PostgreSQL, Canton strives to validate the database replication configuration and fail with an error, if a misconfiguration is detected.
However, this validation is of a best-effort nature; so it may fail to detect an incorrect replication configuration.
For Oracle, no attempt is made to validate the database configuration.
Overall, you should not rely on Canton detecting mistakes in the database configuration.
