..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _participant-node-pruning-howto:

Participant Node pruning
========================

Pruning helps bound the size of the database storage. Participant Node pruning refers to the selective removal of archived
contracts and old transactions. Select one of two options, choosing between ease-of-use and control:

1. Automatic pruning instructs the Participant Node to perform pruning according to a regular schedule and retention period.
2. Manual pruning provides explicit control over the ledger offset to prune up to. In addition, manual pruning allows
   integration of pruning with operational procedures such as database backups and defragmentation.

As a prerequisite to pruning, put in place backups and ensure that a backup is taken each time you prune the Participant Node.
Refer to the :externalref:`backup and restore <backup-and-restore>` for specifics.

Enable automatic pruning
------------------------

Enable automatic pruning by specifying a pruning schedule consisting of the following:

- A cron expression that designates regular pruning begin times.
- A maximum duration specifying pruning end times relative to the begin times of the cron expression.
- A retention period to specify how far to prune relative to the current time.
- An optional indication of whether to prune internal stores only, or by default, to prune Ledger API visible archived
  contracts and updates as well.

For example, to run pruning every Saturday starting at 8 AM until 4 PM (both in UTC):

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/PruningDocumentationTest.scala
   :start-after: user-manual-entry-begin: AutoPruneParticipantNode
   :end-before: user-manual-entry-end: AutoPruneParticipantNode
   :dedent:

On Participant Nodes configured for high availability that share a common database, the methods modifying the pruning schedule
have to be invoked on the active Participant Node replica.

Refer to :externalref:`automatic pruning <ledger-pruning-automatic>` for a reference to the other available automatic pruning methods.

Perform manual pruning
----------------------

Manually pruning Participant Nodes allows composing pruning with database maintenance operations,
but requires identifying an explicit ledger offset up to which the ledger should be pruned.

1. Identify the numeric ledger offset up to which to prune the ledger by specifying the time up to which to prune:

   .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/ledgerapi/LedgerApiParticipantPruningTest.scala
      :start-after: user-manual-entry-begin: ManualPruneParticipantNodeSafeOffsetLookup
      :end-before: user-manual-entry-end: ManualPruneParticipantNodeSafeOffsetLookup
      :dedent:

   The `find_safe_offset` method returns `None` if no offset corresponds to the specified time.

2. Invoke manual pruning on the Participant Node.

   In almost all cases, choose the comprehensive `prune` method that frees up the most storage, but also reduces the portion
   of the ledger visible via the Ledger API.

   .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/ledgerapi/LedgerApiParticipantPruningTest.scala
      :start-after: user-manual-entry-begin: ManualPruneParticipantNodePrune
      :end-before: user-manual-entry-end: ManualPruneParticipantNodePrune
      :dedent:

   In some cases, you might elect to use the `prune_internally` method in addition to the `prune` method. Typically, you
   invoke the `prune_internally` method after the `prune` method and with a larger offset. For example, this way you can
   retain three months of the Ledger API history, but prune the internal stores up to one month.

   .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/ledgerapi/LedgerApiParticipantPruningTest.scala
      :start-after: user-manual-entry-begin: ManualPruneParticipantNodeInternalPrune
      :end-before: user-manual-entry-end: ManualPruneParticipantNodeInternalPrune
      :dedent:

   On Participant Nodes that are configured for high availability and share a common database, the pruning methods have
   to be invoked on the active Participant Node replica.

The `prune` and `prune_internally` methods might appear to be hanging unless the ledger offset is iteratively increased
in sufficiently small increments for piecemeal pruning via multiple methods calls. In addition, these manual methods have
no built-in mechanism to resume on another node after a high-availability failover.

Defragment the database
-----------------------

Defragment the database after pruning. Pruning deletes data from the database, freeing up space, but it does not
resize tables. Refer to the PostgreSQL documentation on `VACUUM` and `VACUUM FULL` for more information on how to
optimally reclaim the space freed up by pruning.

Monitor pruning progress
------------------------

Monitor the pruning state to determine that the pruning schedule allows pruning to keep up with ledger growth, and that
automatic pruning is not stuck for one of the reasons described below as pruning limitations.

Monitor the `daml_pruning_max_event_age` metric describing the age of the "oldest, un-pruned" event (in hours).
The `max-event-age` metrics should not exceed the value of the pruning schedule `retention` plus the length of the
interval. For example, if your schedule specifies a retention of 30 days and a cron that calls for weekly
pruning, `max-event-age` must remain below 37 days. If for any node the `max-event-age` metric exceeds this upper limit,
consider allocating more time for pruning by reducing the interval between pruning windows, or by increasing the
`maximum duration` pruning schedule setting.

.. TODO(#25876): expose the Ledger API GetLatestPrunedOffsets endpoint to query the pruning state via the canton console
