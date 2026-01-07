..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _repairing-howto:

Repair procedures
=================

Choose one of the following repair procedures based on the type of Participant Node fault in need of repair.

Repairing Participant Nodes is dangerous and complex. Therefore, you are discouraged from attempting to repair
Participant Nodes on your own unless you have expert knowledge and experience. Instead, you are strongly advised
to only repair Participant Nodes with the help of technical support.

.. _recovering_from_lost_synchronizer:

Recovering from a lost Synchronizer
-----------------------------------

If a Synchronizer is irreparably lost or no longer trusted, the Participant Nodes previously connected to and sharing
active contracts via that Synchronizer can recover and migrate their contracts to a new Synchronizer. The Participant
Node administrators need to coordinate as follows:

1. Disconnect all Participant Nodes from the Synchronizer deemed lost or untrusted.
2. Identify the Synchronizer configuration of the newly provisioned Synchronizer.
3. All Participant Node administrators invoke the `migrate_synchronizer` method specifying the "old" Synchronizer as
   "source" and the "new" synchronizer. Additionally, set the "force" flag to true given that the old Synchronizer
   is either lost or untrusted in this scenario.

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/repair/ParticipantMigrateSynchronizerIntegrationTest.scala
   :start-after: user-manual-entry-begin: MigrateSynchronizerForRecovery
   :end-before: user-manual-entry-end: MigrateSynchronizerForRecovery
   :dedent:

4. The Participant Nodes connect to the new Synchronizer.
5. The Synchronizer loss may have resulted in active contract set inconsistencies. In such cases the
   Participant Nodes administrator need to agree on whether contracts in an inconsistent state on the different
   Participant Nodes should be removed or added. Refer to :ref:`troubleshooting ACS commitments<troubleshoot-commitments>`
   to identify and repair ACS differences.

   As these methods are powerful but dangerous, you should not attempt to repair your Participant Nodes on your own as
   you risk severe data corruption, but rather in the context of technical support.

Fully rehydrating a Participant Node
------------------------------------

Fully rehydrating a Participant Node means recovering the Participant Node after its database has been fully emptied
or lost. If a Participant Node needs to be rehydrated, but no Participant Node backup is available or the backup is faulty,
you may be able to rehydrate the Participant Node from a Synchronizer as long as the Synchronizer has never been
pruned, and you have local console access to the Participant Node. You can preserve the Participant Node's identity
and secret keys.

If you are running your production Participant Node in a container, you need to create a new configuration file
that allows you to access the database of the Participant Node from an interactive console. Make sure that the
Participant Node process is stopped and that nothing else is accessing the same database. Ensure that database
replication is turned on if the Participant Node has been configured for high availability. Also, make sure that
the Participant Nodes are configured to not perform auto-initialization to prevent creating a new identity
by setting disabling the auto-init configuration option:

.. literalinclude:: CANTON/community/app/src/test/resources/participant1-manual-init.conf
   :start-after: user-manual-entry-begin: ManualParticipantNodeIdentityConfig
   :end-before: user-manual-entry-end: ManualParticipantNodeIdentityConfig

Then start Canton interactively and with the using the ``--manual-start`` option to prevent the Participant Node
from reconnecting to the Synchronizers:

.. code-block:: bash

    ./bin/canton -c myconfig --manual-start

Then, download the identity state of the Participant Node to a directory on the machine you are running the
process:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/sequencer/RehydrationIntegrationTest.scala
   :language: scala
   :start-after: architecture-handbook-entry-begin: RepairMacroCloneIdentityDownload
   :end-before: architecture-handbook-entry-end: RepairMacroCloneIdentityDownload
   :dedent:

This stores the topology state, the identity, and, if the Participant Node is not using a Key Management System,
the secret keys on the disk in the specified directory.

The ``dars.download`` command is a convenience command to download all Daml archive files (DARs) that have been
added to the participant via the console command ``participant.dars.upload``. DARs that
were uploaded through the Ledger API need to be manually re-uploaded to the new Participant Node.

After downloading, stop the Participant Node, back up the database, and then truncate the database. Then, restart
the Participant Node and upload the data again:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/sequencer/RehydrationIntegrationTest.scala
   :language: scala
   :start-after: architecture-handbook-entry-begin: RepairMacroCloneIdentityUpload
   :end-before: architecture-handbook-entry-end: RepairMacroCloneIdentityUpload
   :dedent:

.. todo::
   Once RepairMacros.identity.download and upload support KMS, add a note to re-register the existing KMS keys <https://github.com/DACH-NY/canton/issues/25974>

Reconnect the Participant Node to the Synchronizer using a normal connect:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/sequencer/RehydrationIntegrationTest.scala
   :language: scala
   :start-after: architecture-handbook-entry-begin: RepairMacroCloneIdentityConnect
   :end-before: architecture-handbook-entry-end: RepairMacroCloneIdentityConnect
   :dedent:

Note that this replays all transactions from the Synchronizer. However, command deduplication
is only fully functional once the Participant Node catches up with the Synchronizer. Therefore, you
need to ensure that applications relying on command deduplication do not submit commands
during recovery.

Unblocking a Participant Node Synchronizer connection
-----------------------------------------------------

If a Participant Node is unable to process events from a Synchronizer, and the Participant Node process continuously
crashes when reconnecting to the Synchronizer, it may be necessary to "ignore" the problematic event. This procedure
describes how to unblock the Participant Node.

1. Ensure that the cause is not a more common issue such as lack of database connectivity. Only proceed with the
   following steps if the Participant Node logs rule out more common issues. Inspect the Participant Node logs for
   "internal" errors referring to the Synchronizer ID. If the logs show an `ApplicationHandlerException`,
   note the first and last "sequencing timestamps".

2. Disconnect the Participant Node from the Synchronizer whose events you want to ignore, and restart the Participant Node
   after enabling the repair commands and internal state inspection.

   .. literalinclude:: CANTON/community/app/src/pack/examples/07-repair/enable-preview-commands.conf
      :start-after: user-manual-entry-begin: EnableRepairAndStateInspectionConfig
      :end-before: user-manual-entry-end: EnableRepairAndStateInspectionConfig
      :dedent:

   Make sure that the Participant Node is disconnected from the Synchronizer.

3. Determine the "Sequencer counters" to ignore.

   - If in step 1 you identified the first and last sequencing timestamps, translate the timestamps to first and last
     sequencer counters:

     .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/repair/IgnoreSequencedEventsIntegrationTest.scala
        :language: scala
        :start-after: user-manual-entry-begin: LookUpSequencedEventToIgnoreByTimestamp
        :end-before: user-manual-entry-end: LookUpSequencedEventToIgnoreByTimestamp
        :dedent:

   - Otherwise you may choose to ignore the last event that the Participant Node has processed from the Synchronizer
     and look up the sequencer counter as follows:

     .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/repair/IgnoreSequencedEventsIntegrationTest.scala
        :language: scala
        :start-after: user-manual-entry-begin: LookUpLastSequencedEventToIgnore
        :end-before: user-manual-entry-end: LookUpLastSequencedEventToIgnore
        :dedent:

4. Use the `repair.ignore_events` command to ignore the sequenced events that the Participant Node is unable to process.
   The command takes the Synchronizer ID and the first and last sequencer counters of the events to ignore as parameters.
   You should not set the force flag.

   .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/repair/IgnoreSequencedEventsIntegrationTest.scala
      :language: scala
      :start-after: user-manual-entry-begin: RepairIgnoreEvents
      :end-before: user-manual-entry-end: RepairIgnoreEvents
      :dedent:

   If you find that you have made a mistake and chose the wrong sequencer counters, you can invoke `repair.unignore_events`
   to "unignore" the events:

   .. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/repair/IgnoreSequencedEventsIntegrationTest.scala
      :language: scala
      :start-after: user-manual-entry-begin: RepairUnignoreEvents
      :end-before: user-manual-entry-end: RepairUnignoreEvents
      :dedent:

5. Next, reconnect the Participant Node to the Synchronizer and check that the Participant Node is able to process
   sequenced events again. If the Participant Node is still blocked, you may need to repeat the previous steps.

6. Once the Participant Node is unblocked consuming events from the Synchronizer, Participant Nodes may have an inconsistent
   ACS. Look for errors in the log that begin with "ACS_COMMITMENT", for example "ACS_COMMITMENT_MISMATCH". Inspect not
   only the repaired Participant Node, but also the other Participant Nodes that were also involved in the transactions
   behind the ignored events.

7. If there are ACS inconsistencies, refer to :ref:`troubleshooting ACS commitments<troubleshoot-commitments>` to identify
   and repair ACS differences.

8. Disable repair commands and internal state inspection by removing the configurations added in step 2 and restarting
   the Participant Node. This is important to prevent accidental misuse of the repair commands in the future.

As the above steps are powerful but dangerous, you should perform the procedure in the context of technical support.
