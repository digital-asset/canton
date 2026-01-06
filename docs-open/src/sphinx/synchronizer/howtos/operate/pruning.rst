..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Link to explanation of the trade-off between retention period length to allow offline participants to catch up and storage costs.
    Explain how ``CantonParameters.retentionPeriodDefaults`` is used

.. _synchronizer-pruning:

Synchronizer Pruning
====================

Prune the Mediator state
------------------------

You can set up scheduled automatic pruning for each of your Mediators as explained :externalref:`here <ledger-pruning-automatic>`.

You can also directly prune the Mediator without using a schedule by calling the following command:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/MediatorPruningIntegrationTest.scala
   :start-after: user-manual-entry-begin: MediatorPrune
   :end-before: user-manual-entry-end: MediatorPrune
   :dedent:

This operation prunes processed sequenced events and finalized confirmation response aggregations based on the configured default retention period.

This value is seven days by default; you can configure it as follows:

.. code-block:: none

    parameters {
      retention-period-defaults {
        mediator = "7 days"
      }
    }

To choose another value, you can either change the config above or directly specify a retention period
with the command ``mediator.pruning.prune_with_retention_period``
or even the exact timestamp to prune at, using ``mediator.pruning.prune_at``.

.. _sequencer-pruning:

Prune the Sequencer state
-------------------------

You can prune the Sequencer by calling the following command:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/SequencerPruningIntegrationTest.scala
   :start-after: user-manual-entry-begin: SequencerPrune
   :end-before: user-manual-entry-end: SequencerPrune
   :dedent:

This command uses the configured Sequencer default retention period to compute the timestamp from which to prune sequenced events and returns a description of what was pruned.

This value is seven days by default; you can configure it as follows:

.. code-block:: none

    parameters {
      retention-period-defaults {
        sequencer = "7 days"
      }
    }

Alternatively, you can directly specify a retention period
with the command ``sequencer.pruning.prune_with_retention_period``
or even the exact timestamp to prune at, using ``sequencer.pruning.prune_at``.

.. _sequencer-inactive-client-limitation:

Unblock pruning due to inactive sequencer members
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. TODO(#9950): sequencer pruning can be blocked

All Sequencer clients, such as Participants or Mediators, periodically acknowledge the timestamp of the latest event
they have received from the Sequencer. These acknowledgements, which all Sequencers can see, allow the Sequencers to compute
the highest timestamp that all clients have achieved. This timestamp serves as a safe pruning point.

You can check the Sequencer pruning status as follows:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/SequencerPruningIntegrationTest.scala
   :start-after: user-manual-entry-begin: SequencerPruningStatus
   :end-before: user-manual-entry-end: SequencerPruningStatus
   :dedent:

The status contains the latest acknowledgement timestamp for all active clients, and you can check the computed safe pruning point
by calling ``status.safePruningTimestamp``. The Sequencer can only perform pruning earlier than that point.
Otherwise, one or more clients would be unable to continue operation.

If a Sequencer client goes inactive for some time, then all Sequencers are blocked from pruning
past this client's latest acknowledged timestamp.
To unblock a Sequencer from pruning at more recent timestamps, either the client must come back and acknowledge newer events or
you must disable that client on the Sequencer.

The Sequencer has force-prune commands. The difference between these and the regular prune commands is that the force-prune
commands disable members that are preventing pruning from happening at the given timestamp.

You can force prune at a given timestamp as follows:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/SequencerPruningIntegrationTest.scala
   :start-after: user-manual-entry-begin: SequencerForcePruning
   :end-before: user-manual-entry-end: SequencerForcePruning
   :dedent:

Setting ``dryRun`` to ``true`` produces a list of the clients that would be disabled, if any, as part of the operation,
without actually performing the pruning. To perform the pruning operation, run it with ``dryRun`` set to ``false``.

If you've identified a problematic client you don't need to serve, you can directly disable it
by calling the repair command ``repair.disable_member(client)``.

Note that when you disable a client on a Sequencer, this is a local operation;
the client is still active on other Sequencers that have not performed the same operation.

BFT Orderer Pruning
^^^^^^^^^^^^^^^^^^^

The :ref:`BFT Orderer <bft-orderer-arch>` layer of the Sequencer is where distributed consensus on the order of transactions is reached across all Sequencer nodes.
It has its own separate set of database tables and different considerations regarding pruning.

The BFT Orderer serves ordered events up to the Sequencer layer, which stores them and subsequently serves them to
Sequencer clients. The BFT Orderer also needs to retain this data after it is served to the Sequencer layer, because it may need to assist
other BFT Orderer nodes that are behind in catching up.

You must pick a pruning retention period long enough for BFT Orderer nodes to be able to
catch up after crashing and coming back.

See below how to manually prune using an admin command and also check the status.

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/BftOrdererPruningIntegrationTest.scala
   :language: none
   :start-after: user-manual-entry-begin: BftSequencerPruning
   :end-before: user-manual-entry-end: BftSequencerPruning
   :dedent:

You can also set up scheduled automatic pruning for the BFT Orderer using the commands shown below as well as the ones
explained :externalref:`here <ledger-pruning-automatic>`.

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/pruning/BftOrdererPruningIntegrationTest.scala
   :language: none
   :start-after: user-manual-entry-begin: BftSequencerScheduledPruning
   :end-before: user-manual-entry-end: BftSequencerScheduledPruning
   :dedent:
