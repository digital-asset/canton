..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _troubleshoot-commitments:

Troubleshoot ACS Commitments
============================

A participant node generates commitments to the active contract set (ACS) at regular intervals and exchanges them
with its counter-participants.
These commitments are used to establish non-repudiation on the common ACS state between participants, which is
necessary for pruning the participant state.
More information on commitments is available in the :externalref:`Pruning overview section <pruning-overview>`.

Error codes
------------

A participant node logs warning messages when it detects a fork with the ACS of a counter-participant, when commitment
computation is slow, or when received commitments show evidence of tampering.
The following error codes are relevant for troubleshooting commitments, which are described in detail in the
:ref:`Error codes reference section <error_codes>`:

* ``ACS_COMMITMENT_ALARM``: This warning indicates malicious behavior. It occurs when an ACS commitment received
  from a counter-participant has an invalid signature, and when the participant receives two correctly signed but
  different commitments from the same counter-participant covering the same interval.

* ``ACS_COMMITMENT_MISMATCH``: This warning indicates a fork in the common ACS state of the participant and one or more
  counter-participants. Please consult the :ref:`runbook for inspecting commitment mismatches <troubleshooting-forks>`.

* ``ACS_MISMATCH_NO_SHARED_CONTRACTS``: This warning indicates a special case of a fork, where a counter-participant
  sent a commitment for a period, while this participant does not think that there are any shared active contracts at
  the end of that period. The counter-participant will log an ``ACS_COMMITMENT_MISMATCH``.

* ``ACS_COMMITMENT_DEGRADATION``: This warning indicates that the participant node is behind some of its counter-participants
  in commitment computation, perhaps due to heavy load, and enters :ref:`catch-up mode <commitments-optimizations>`.
  Counter-participants might blacklist this participant, because it prevents them from pruning.

* ``ACS_COMMITMENT_DEGRADATION_WITH_INEFFECTIVE_CONFIG``: This error code indicates that there is a degradation in
  commitment computation and the catch-up mode started, however the catch-up mode configuration is invalid and will
  not improve performance.

Inspection Tools
-----------------

Inspection tools enable the operator to understand to what extent its participant node agrees on its ACS state with
its counter-participants. This is especially useful in two situations:

* The operator suspects that agreement is slower than it could or should be, e.g., because :ref:`commitment monitoring <observe-commitments>`
  alerts the operator of slowdowns when sending or receiving commitments.
  Inspection tools enable the operator to investigate the reason for the alert: Which counter-participants are behind,
  how far they are behind, or whether the participant itself is behind.

* There is a fork between the participant's ACS state and the state of one or more counter-participants. The participant
  node operator wants to understand the context of the fork, e.g., was the fork transient or not, does the fork span
  several counter-participants or just one, etc.

Troubleshoot Slow Commitments
******************************

Assume that the operator observes via :ref:`commitment monitoring <observe-commitments>` that the participant's own
commitments or a counter-participants' commitments are slow, or when the operator observes
``ACS_COMMITMENT_DEGRADATION`` warnings in the logs.
The participant operator can use the command ``commitments.lookup_received_acs_commitments`` in the
:ref:`admin console <canton_console_reference>` (or via gRPC) to understand
which counter-participant is ahead of its own participant and how far ahead it is. If several counter-participants are
ahead, then the operator can try to correlate the periods of the counter-participants' commitments when the slowdown
started with any changes in the participant's load, connectivity, etc.

.. todo::
   Refine when addressing `#27011 <https://github.com/DACH-NY/canton/issues/27011>`_

The following examples show how the operator of ``participant1`` inspects the received commitments from all counter-participants,
for specific time periods on synchronizer ``synchronizerId``, filtered by buffered commitments, which means
that the participant received but has not computed the commitments for that period yet:

.. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentToolingIntegrationTest.scala
  :start-after: user-manual-entry-begin: InspectReceivedCommitments
  :end-before: user-manual-entry-end: InspectReceivedCommitments
  :dedent:

The output indicates that counter-participant ``participant2`` has sent commitments for the periods
``1970-01-01T00:00:20Z to 1970-01-01T00:00:25Z`` and ``1970-01-01T00:00:25Z to 1970-01-01T00:00:30Z``, shows the received
commitment hashes but not the send commitments because the participant has not computed them yet, and the state that these
commitments are buffered.

.. todo::
   It would be better to have the output of the commands generated from an actual integration test. Our docs tooling
   supports that (see SphinxDocumentationGenerator and its implementations). It might be worth to convert the tooling
   integration test accordingly. `#27011 <https://github.com/DACH-NY/canton/issues/27011>`_

.. code-block:: none

  Map(synchronizer1::122025ee4d83... ->
    Vector(
      ReceivedAcsCmt(CommitmentPeriod(fromExclusive = 1970-01-01T00:00:20Z, toInclusive = 1970-01-01T00:00:25Z),PAR::participant2::122080ac62a3...,Some(SHA-256:9a5a5575876d...),None,Buffered),
      ReceivedAcsCmt(CommitmentPeriod(fromExclusive = 1970-01-01T00:00:25Z, toInclusive = 1970-01-01T00:00:30Z),PAR::participant2::122080ac62a3...,Some(SHA-256:ac9142943183...),None,Buffered)
    )
  )

.. _lookup_sent_acs_commitments:

As another example, assume that the operator observes an ``ACS_COMMITMENT_MISMATCH`` in the logs. The operator can use
``commitments.lookup_sent_acs_commitments`` in the :ref:`admin console <canton_console_reference>` (or via gRPC) as a first
step to inspect the mismatch. Concretely, the operator can use this command to understand with which counter-participants
the participant has mismatches, and for what periods. The operator could then  correlate the periods of the mismatches
with, e.g., potential repair commands on the participant, or even uncover misbehaving counter-participants.

.. todo::
   Refine when addressing `#27011 <https://github.com/DACH-NY/canton/issues/27011>`_

The following examples show how the operator of ``participant1`` inspects the commitments sent to all counter-participants,
for specific time periods on synchronizer ``synchronizerId``, filtered by mismatches commitments:

.. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentToolingIntegrationTest.scala
  :start-after: user-manual-entry-begin: InspectSentCommitments
  :end-before: user-manual-entry-end: InspectSentCommitments
  :dedent:


The output indicates that participant ``participant1`` has sent a commitment for the period
``1970-01-01T00:00:40Z to 1970-01-01T00:00:45Z`` to ``participant2``, shows the commitment bytestring for the sent and
the received commitment, which are different, hence the mismatch state.

.. code-block:: none

  Map(synchronizer1::1220087eeae4... ->
    Vector(
      SentAcsCmt(CommitmentPeriod(fromExclusive = 1970-01-01T00:00:40Z, toInclusive = 1970-01-01T00:00:45Z),PAR::participant2::12208336b38e...,Some(SHA-256:1d7e803bed16...),Some(SHA-256:c72753e6adc6...),Mismatch)
    )
  )

Handle slow counter-participant
---------------------------------

If a participant operator observes that a counter-participant is slow in sending commitments, then the operator can
opt not to wait for commitments from specific counter participants across multiple synchronizers.
The configuration place takes effect immediately.
Pruning the participant can continue even if the participant misses commitments from these counter-participants, or if
the commitments it receives do not match.
Although the participant does not expect commitments from these counter-participants,
it still computes and sends commitments to them, and it does process potential received commitments, e.g., logs mismatch warnings.

.. note::
  Disabling waiting for commitments disregards these counter-participants w.r.t. non-repudiation when the participant
  is pruned, but increases pruning resilience to failures and slowdowns of those counter-participants and/or the network.

The following shows how the operator of ``participant1`` can mark as no-wait counter-participant ``participant2``
on synchronizer ``synchronizerId1``. This command, as well as the commands in this section, also accept multiple counter-participants
and synchronizers.

.. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentNoWaitCounterParticipantIntegrationTest.scala
  :start-after: user-manual-entry-begin: SetNoWaitCommitment
  :end-before: user-manual-entry-end: SetNoWaitCommitment
  :dedent:

The operator can also reset the no-wait configuration, i.e., wait for commitments, for counter-participant ``participant2``
on synchronizer ``synchronizerId1`` using a similar command:

.. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentNoWaitCounterParticipantIntegrationTest.scala
  :start-after: user-manual-entry-begin: ResetNoWaitCommitment
  :end-before: user-manual-entry-end: ResetNoWaitCommitment
  :dedent:

To retrieve the current no-wait configuration for synchronizer ``synchronizerId1`` and all participants known to
``participant1`` on synchronizer ``synchronizerId1``, whether or not they are connected to ``synchronizerId1``,
the operator can use the command below.
The command allows for filtering by specific counter-participants and/or synchronizers.

.. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentNoWaitCounterParticipantIntegrationTest.scala
  :start-after: user-manual-entry-begin: GetNoWaitCommitment
  :end-before: user-manual-entry-end: GetNoWaitCommitment
  :dedent:

The output shows that ``participant2`` is marked as no-wait on ``synchronizerId1`` for ``participant1``, and that there
are no participants from whom ``participant1`` currently waits for commitments on ``synchronizerId1``.

.. code-block:: none

  (Vector
    (NoWaitCommitments
      (no-wait counter-participant = PAR::participant2::12206a279664..., synchronizers = synchronizer1::1220034cf0b0...)
    ),
  Vector()
  )

.. note::
  A participant added to the no-wait will not affect the *Default* or *Distinguished*
  :ref:`counter-participant latency monitoring <observe-commitments>`.

Handle slow commitment computation
-----------------------------------

:ref:`Catch-up mode <commitments-optimizations>` is a behavior that automatically trigger when a participants detects
itself being falling behind.
The synchronizer operator can configure catch-up mode on a per-synchronizer basis.

Once a participant detects itself being a number of periods behind (``nrIntervalsToTriggerCatchUp`` with default value 2), then
it activates skipping periods (``catchUpIntervalSkip`` with default value 5) in order to perform less computation and thereby catch up.
This does mean that all periods in between will not be subject to fork detection and only the interval skip will be compared.
If no forks are detected at the skip, then any potential forks have resolved themselves anyway.

An alternative configuration in the synchronizer is ``reconciliationInterval`` (default value one minute), this dictates
how often the participant perform the commitment exchange with counter participants.
A lower value means more frequent and quicker fork detection.
A higher value means less computation, but longer time before noticing a fork.
Catch-up mode is directly affected by the ``reconciliationInterval``. With default values, catch-up will trigger once a participant
observes itself being 10 minutes behind (``reconciliationInterval`` * ``nrIntervalsToTriggerCatchUp`` * ``catchUpIntervalSkip``).

To configure the parameters above, please refer to the synchronizer management section on
:ref:`Manage dynamic synchronizer parameters <dynamic_synchronizer_parameters>`.


.. _troubleshooting-forks:

Troubleshoot Forks (Commitment Mismatches)
*******************************************

When the operator observes an ``ACS_COMMITMENT_MISMATCH`` in the logs, it indicates that the participant's ACS state
diverged from the counter-participant's state. In other words, the two participants do not agree on the shared contracts.
In the example below, ``participant1`` received a commitment from ``participant2`` for the period
``1970-01-01T00:01:35Z to 1970-01-01T00:01:40Z`` on ``synchronizer1``, but the received commitment does not
match the local commitment for that period.

.. code-block:: none

  ACS_COMMITMENT_MISMATCH(5,246a29fb): The local commitment does not match the remote commitment
  err-context:
    {
      local=Vector((CommitmentPeriod(fromExclusive = 1970-01-01T00:01:35Z, toInclusive = 1970-01-01T00:01:40Z), SHA-256:cfd7fd917fb9...)),
      remote=AcsCommitment(
        synchronizerId = synchronizer1::1220b9dd1a9f199b898522ec31a8ce3ff098a7ce05b0cd3ce76e0a544959fe58c8e9::34-0,
        sender = PAR::participant2::1220b2063ef1...,
        counterParticipant = PAR::participant1::1220cc59a221...,
        period = CommitmentPeriod(fromExclusive = 1970-01-01T00:01:35Z, toInclusive = 1970-01-01T00:01:40Z),
        commitment = SHA-256:e2c0cf0361cb...
      ),
      synchronizerId=synchronizer1::1220b9dd1a9f...
    }


.. note::
   Troubleshooting a mismatch requires that the two participant operators collaborate in order to identify what caused the
   mismatch. Mismatches can happen due to misconfiguration, e.g., when changing contract stores via the repair service, bugs, or
   malicious behavior. The :ref:`non-repudiation section <pruning-non-repudiation>` provides more detail on forks.
   For example, one of the participants might not know of a contract the other participant claims to be shared at the mismatch
   timestamp, or it might have known about the contract in the past but deactivated it before the mismatch timestamp, or
   or it might have that contract active but on a different synchronizer. Thus, the outcome of troubleshooting a mismatch
   gives the type of mismatch for each contract that only one participant believes it shares with the other participant.

If the participants are mutually distrustful, they may want to validate the data they exchange during the troubleshooting
process. Thus, depending on the trust level, these validation steps are optional.

We troubleshoot the mismatch as follows:

1. **Inspect the local shared contract metadata**
   This data represents the set of contracts that the ``participant1`` believes it shares with the counter-participant
   ``participant2`` at timestamp ``1970-01-01T00:01:40Z`` on ``synchronizer1``. The operator of `participant1` can obtain
   this set by first obtaining the commitment it sent using :ref:`lookup_sent_acs_commitments <lookup_sent_acs_commitments>`, and then opening
   the commitment using the command ``commitments.open_commitment`` in the :ref:`admin console <canton_console_reference>`
   (or via gRPC) as follows:

   .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMismatchInspectionRunbookIntegrationTest.scala
     :start-after: user-manual-entry-begin: OpenCommitment
     :end-before: user-manual-entry-end: OpenCommitment
     :dedent:

   where ``mismatchTimestamp = 1970-01-01T00:01:40Z``. If the local and
   the remote commitments have different period ends, we take the period end of the local commitment.

   When the operator uses the console command, the operator can choose to optionally write the output into a file,
   in this case ``openCmtLocalFilename``, which is useful for the next steps.
   The output in the ``openCmtLocalFilename`` file contains the local shared contract IDs and their reassignment counters:

   .. code-block:: none

     {"cid":"00749df26491a08f246d4cd262307ea0c9676adde3861d6d2c80e081ddd8160430ca1112202942877d5c9e8d6f79d9167f9841fd4fd092ed2c97c47459c2c40f18a236b239","reassignmentCounter":"0"}
     {"cid":"008403c268ce244fda036c7916b1f33885df153e6a4b0f60e3c8724277100b4362ca111220f5a052b1c66024133c844a8e6f67dec9a1d3c6e0cd3cb0023843b10cdeff4126","reassignmentCounter":"0"}
     {"cid":"002e46d505d569ad80638831c5ad043d281960121f8b6a564e18937819cf1c8536ca111220985ed1546f8a45e39626290d80d0640c8663ed13d078f9761b0e1e2bd0c99ec5","reassignmentCounter":"0"}
     {"cid":"00f22bb479621372e70bc1b9d83ea0a6741282ab014319ef1a7c4ae0b82f442785ca1112207e6d62fda4b9ae58fdc686332b4c7c688814cab840283d1f3245d7fc6c026fd1","reassignmentCounter":"0"}
     {"cid":"00e577eb32be4082705edcccc231657551b83bef6076fafcd0f3a89f63f09645f3ca1112208604884e3c2d4711bae0ab6178238b36de6ba1e5f5913be6dce2016e6371725a","reassignmentCounter":"0"}
     {"cid":"002d6217d1ea6ff05ca0d089a8988465197e6c5518a9f83cb0dc69611cf0572acbca1112202329ef987c25375e0130580e289df681581dc9439737cd53f3223cb22e7c2ff9","reassignmentCounter":"0"}

   .. note::
      The files used for commitment mismatch inspection use a ``.json`` format; for some files, each line is a `JSON` object, rather than
      the file as a whole.
      We recommend, nonetheless, to use the provided tooling for reading / writing these files, as the contents of the files might evolve.

2. **Inspect the counter-participant's shared contract metadata from the counter-participant**
   This data represents the set of contracts that the counter-participant believes it shares with the participant
   at timestamp ``1970-01-01T00:01:40Z`` on ``synchronizer1``. The operator of ``participant1`` asks the operator of
   ``participant2`` to run ``commitments.open_commitment``, which the counter-participant operator probably intends to
   do anyway, as it also observes a mismatch warning in its logs.

   .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMismatchInspectionRunbookIntegrationTest.scala
     :start-after: user-manual-entry-begin: OpenCommitmentCounter
     :end-before: user-manual-entry-end: OpenCommitmentCounter
     :dedent:

   where this time the operator of ``participant2`` retrieves the commitment it sent to ``participant1``
   at ``mismatchTimestamp = 1970-01-01T00:01:40Z``. If the local and
   the remote commitments had different period ends, we'd take the period end of the remote commitment.

   As before, the output in the ``openCmtRemoteFilename`` file contains the local shared contract IDs and their reassignment counters:

   .. code-block:: none

     {"cid":"00749df26491a08f246d4cd262307ea0c9676adde3861d6d2c80e081ddd8160430ca1112202942877d5c9e8d6f79d9167f9841fd4fd092ed2c97c47459c2c40f18a236b239","reassignmentCounter":"0"}
     {"cid":"008403c268ce244fda036c7916b1f33885df153e6a4b0f60e3c8724277100b4362ca111220f5a052b1c66024133c844a8e6f67dec9a1d3c6e0cd3cb0023843b10cdeff4126","reassignmentCounter":"0"}
     {"cid":"002e46d505d569ad80638831c5ad043d281960121f8b6a564e18937819cf1c8536ca111220985ed1546f8a45e39626290d80d0640c8663ed13d078f9761b0e1e2bd0c99ec5","reassignmentCounter":"0"}
     {"cid":"00e577eb32be4082705edcccc231657551b83bef6076fafcd0f3a89f63f09645f3ca1112208604884e3c2d4711bae0ab6178238b36de6ba1e5f5913be6dce2016e6371725a","reassignmentCounter":"0"}
     {"cid":"002d6217d1ea6ff05ca0d089a8988465197e6c5518a9f83cb0dc69611cf0572acbca1112202329ef987c25375e0130580e289df681581dc9439737cd53f3223cb22e7c2ff9","reassignmentCounter":"0"}

   The operator of ``participant2`` can send the ``openCmtRemoteFilename`` file to the operator of ``participant1``.

3. **Optional: validate counter-participant data**
   The operator of ``participant1`` can validate that the contract metadata received from ``participant2`` matches the
   commitment received from ``participant2``:

   .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMismatchInspectionRunbookIntegrationTest.scala
     :start-after: user-manual-entry-begin: ValidateContractMetadata
     :end-before: user-manual-entry-end: ValidateContractMetadata
     :dedent:

   where ``openCmtRemoteFilename`` is the file the operator of ``participant1`` received from the operator of ``participant2``
   in step 2.

   If this step fails, then the counter-participant sent either the wrong commitment, or the wrong contract metadata, or perhaps
   both. In this case, it appears that the operator of ``participant1`` cannot reliably identify the mismatch cause
   because it cannot trust the input of the operator of ``participant2``, and should not proceed with the troubleshooting steps below.

4. **Identify mismatching contracts**
   The operator of ``participant1`` can identify the mismatching contracts by checking its own opened commitment output
   in the ``openCmtLocalFilename`` file from step 1 against the counter-participant's opened commitment output in the
   ``openCmtRemoteFilename`` file received in step 2.

   .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMismatchInspectionRunbookIntegrationTest.scala
     :start-after: user-manual-entry-begin: MismatchingContracts
     :end-before: user-manual-entry-end: MismatchingContracts
     :dedent:

   As shown in the code excerpt, the operator can optionally write the output into a file, in this case
   ``mismatchingContractsFilename``, which is useful for the next step.

   The output shows the mismatch: There is one contract that only ``participant1`` considers active. The output would
   also show if ``participant2`` had contracts that it considers active but ``participant1`` does not, and if there are
   contracts that both participants consider active but with different reassignment counters.

   .. code-block:: none

     {
       "cidsOnlyLocal": [
         "00f22bb479621372e70bc1b9d83ea0a6741282ab014319ef1a7c4ae0b82f442785ca1112207e6d62fda4b9ae58fdc686332b4c7c688814cab840283d1f3245d7fc6c026fd1"
       ],
       "cidsOnlyRemote": [],
       "differentReassignmentCounters": []
     }

5. **Inspect mismatch cause**
   The operator of ``participant1`` can analyze the source of the mismatches by inspecting why contract
   ``00e773decfb7f537880972943980da..`` is active. The operator can optionally write the output in a file,
   in this case ``inspectContractsFilename``, which it can exchange with the operator of ``participant2`` if needed
   for repairing the ACS state.

   .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMismatchInspectionRunbookIntegrationTest.scala
     :start-after: user-manual-entry-begin: InspectMismatchCause
     :end-before: user-manual-entry-end: InspectMismatchCause
     :dedent:

   The command returns the contract states (created, assigned, unassigned, archived, unknown) of the given contracts on
   all synchronizers the participant knows from the beginning of time until the present time on each synchronizer.
   In this case, ``inspectContractsFilename`` contains the contract states of contract
   ``00e773decfb7f537880972943980da..`` on all synchronizers the participant knows:

   .. code-block:: none

     {
       "cid":"00f22bb479621372e70bc1b9d83ea0a6741282ab014319ef1a7c4ae0b82f442785ca1112207e6d62fda4b9ae58fdc686332b4c7c688814cab840283d1f3245d7fc6c026fd1",
       "activeOnExpectedSynchronizer":true,
       "contract":"CgMyLjES4QQKRQDKmKJSNKcVjBs2yEfu9TDXuGxVS8/FDK3F3QxK8tOs58oREiA3S1IQ7INuTTYMHjMRm/fNx6VaEaI/t61bwhf5XN5NexIOQ2FudG9uRXhhbXBsZXMaTApAODIwYTA5MjEzYzgyYmRlNjdmZjUxODEyMWViMTFhYjdjYTUwNTA3MTAzNDIyMDMzNzEyN2UyNjhkMTVkZWI1NhIDSW91GgNJb3Ui3AFq2QEKVgpUOlJwYXJ0aWNpcGFudDE6OjEyMjA4OWRlZjczZGEzYTAzOTkzMDIxZmFjM2Q1NDY5ZDQ5YWJmMzc1NWFhNzJlNTFhNzc0MTEwNWJlMjFmOWVlZGJlClYKVDpScGFydGljaXBhbnQyOjoxMjIwYjdhMzBmMDYwODIxNzU1YzZmNDFlNTg2MDU3NDBiZGE0NWM0ZjM5ZmEzMTVjODAwOThkYTNhZjAwM2FhNzFjOAohCh9qHQoSChAyDjEwMC4wMDAwMDAwMDAwCgcKBUIDVVNECgQKAloAKlJwYXJ0aWNpcGFudDE6OjEyMjA4OWRlZjczZGEzYTAzOTkzMDIxZmFjM2Q1NDY5ZDQ5YWJmMzc1NWFhNzJlNTFhNzc0MTEwNWJlMjFmOWVlZGJlMlJwYXJ0aWNpcGFudDI6OjEyMjBiN2EzMGYwNjA4MjE3NTVjNmY0MWU1ODYwNTc0MGJkYTQ1YzRmMzlmYTMxNWM4MDA5OGRhM2FmMDAzYWE3MWM4OQEtMQEAAAAAQioKJgokCAESICk3eHUEqY8nX3bxxmkk5wkSkfDtDxlKUI52dC38tZU5EB4=",
       "state":[
         {
           "synchronizerId":"synchronizer1::12207195bde33b4c24dac3f1d7105873ff6c6724d9160491386d17d97e910f3155e3",
           "contractState":{"$type":"ContractCreated"}
         }
       ]
     }

   The operator of ``participant2`` can perform the commands in steps 4 and 5 to inspect the mismatch cause on its side,
   exchange the results with the operator of ``participant1``, and jointly identify the root cause of the mismatch.
