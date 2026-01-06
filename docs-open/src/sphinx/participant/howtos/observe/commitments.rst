..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _observe-commitments:

Monitor ACS Commitments
=======================

A participant that fails to send commitments in a timely manner is problematic for its counter-participants:
Counter-participants cannot prune their state, because they have no proof that their state is the
same as the state of the participant. More information on commitments is available in the :externalref:`Pruning overview section <pruning-overview>`.

This page describes the monitoring options for ACS commitments. Commitment monitoring supports participant node
operators in several ways. First, monitoring provides insight into commitment generation performance, allowing the
participant node operator to troubleshoot and fix potential performance problems. For example, monitoring metrics
indicate potential performance bottlenecks, which the operator can use as input for configuring commitment generation.

Second, monitoring provides insights into the status of commitments from counter-participants. This is relevant for
the participant node operator because a counter-participant
that runs behind in commitment generation, either because it is faulty or because the network is slow, prevents pruning
on the participant: The participant does not know whether its state and the counter-participant's state diverged, and
cannot prune because it might need to investigate a potential fork. The operator can use the monitoring metrics
to identify slow counter-participants and potentially blacklist them.

Monitoring own commitments
**************************

We provide the following metrics for commitment generation, which are described in detail in the
:ref:`Metrics reference section <reference-metrics>`:

* ``daml.participant.sync.commitments.compute``: Measures the time that the participant node spends
  computing commitments.
* ``daml.participant.sync.commitments.sequencing-time``: Measures the time between the end of a
  commitment period, and the time when the sequencer observes the corresponding commitment.
* ``daml.participant.sync.commitments.catchup-mode-enabled``: Measures how many times the catch-up
  mode has been triggered.


Monitoring counter-participant commitments
******************************************

The operator can monitor the status of commitments from the counter-participants through latency metrics.
These metrics can reveal slow counter-participants, which are behind in sending commitments, and
enable operators to configure thresholds defining when a counter-participant is considered slow.

The operator can group counter-participants into three categories, which affect metric reporting:

* *Default*
* *Distinguished*
* *Individually monitored*

An *Individually monitored* counter-participant always shows that participant's commitment latency.
*Distinguished* and *Default* groupings of counter-participants only show the largest latency in
the group. Inspection tools and direct monitoring can then be used to identify slow counter-participant(s).

All metrics below are described in detail in the :ref:`Metrics reference section <reference-metrics>`.

* *Default*: All counter-participants that are not distinguished or individually monitored belong to
  this group by default. We publish one aggregated metric for all participants in this group:
  ``daml.participant.sync.commitments.synchronizer.largest-counter-participant-latency`` which
  represents the highest latency in milliseconds for commitments from counter-participants outstanding
  for more than a threshold number of reconciliation intervals.

* *Distinguished*: The operator has the option to upgrade some default counter-participants to the distinguished group,
  for example, counter-participants with whom it has important business relations. We produce one aggregate metric for all
  distinguished participants, published under ``daml.participant.sync.commitments.synchronizer.largest-distinguished-counter-participant-latency``
  Just as for the Default group, the metric represents the highest latency in milliseconds for commitments outstanding
  for more than a `thresholdDistinguished` number of reconciliation intervals.

  The following examples show how the operator of ``participant1`` adds counter-participant ``participant4`` to
  the distinguished group on synchronizer ``synchronizer2Id``, and removes counter-participant ``participant2`` from the
  distinguished group on synchronizer ``synchronizer1Id``:

  .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMetricsIntegrationTest.scala
   :start-after: user-manual-entry-begin: DistinguishedParticipantAddMetric
   :end-before: user-manual-entry-end: DistinguishedParticipantAddMetric
   :dedent:

  .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMetricsIntegrationTest.scala
   :start-after: user-manual-entry-begin: DistinguishedParticipantRemoveMetric
   :end-before: user-manual-entry-end: DistinguishedParticipantRemoveMetric
   :dedent:

* *Individually monitored*: The operator can optionally select counter-participants whose commitment status it wants to monitor
  individually, for example because they recently presented intermittent failures and have just recovered, or because
  the operator observes a slowdown in one of the other groups and wants to locate the cause. Each participant gets its
  own unique label under ``daml.participant.sync.commitments.synchronizer.counter-participant-latency.<participantId>``
  Individual alerting can be set based on the business relations.

  .. note::
    Any participant, whether *Default* or *Distinguished*, can be added to *Individually monitored*. A distinguished participant
    remains in the *Distinguished* group even if it is *Individually monitored*. In contrast, a *Default* participant that is
    added to *Individually monitored* is removed from the Default group.

  The following examples show how the operator of ``participant1``  adds/removes counter-participant ``participant3`` to
  be *Individually monitored* on the synchronizer ``synchronizerId``:

  .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMetricsIntegrationTest.scala
   :start-after: user-manual-entry-begin: IndividualMonitoringAdd
   :end-before: user-manual-entry-end: IndividualMonitoringAdd
   :dedent:

  .. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMetricsIntegrationTest.scala
   :start-after: user-manual-entry-begin: IndividualMonitoringRemove
   :end-before: user-manual-entry-end: IndividualMonitoringRemove
   :dedent:

The operator of a participant can set the monitoring configuration at once on multiple synchronizers, including thresholds
for the *Default* and *Distinguished* groups, as well as for the *Individually monitored*. The example below
shows how the operator of ``participant1`` can apply a monitoring configuration to synchronizers ``synchronizer1Id`` and
``synchronizer2Id``.

.. literalinclude:: ../../../../../../community/app/src/test/scala/com/digitalasset/canton/integration/tests/AcsCommitmentMetricsIntegrationTest.scala
   :start-after: user-manual-entry-begin: SetMonitoringConfig
   :end-before: user-manual-entry-end: SetMonitoringConfig
   :dedent:


