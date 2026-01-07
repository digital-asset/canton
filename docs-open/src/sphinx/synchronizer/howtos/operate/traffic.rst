..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _sequencer-traffic:

Sequencer Traffic Management
============================

.. note::
    Currently traffic management is supported only on Byzantine Fault Tolerant (BFT) Synchronizers.

This page describes how to enable and configure :externalref:`traffic management<overview-explanation-traffic-management>` on a Synchronizer,
and how to inspect and manage the traffic balances of its members.

Inspecting how much traffic a Synchronizer member has from the member's perspective
(for Participant Nodes and Mediator Nodes) is discussed in
:externalref:`Manage Node traffic <node-traffic-management>`.



Enable traffic management on a Synchronizer
-------------------------------------------
Traffic management can be enabled or disabled on an existing Synchronizer via adjusting its dynamic synchronizer parameters.
Please refer to the :ref:`Change dynamic synchronizer parameters <dynamic-sync-params>` for more details.

First let's check the current traffic management status of a Synchronizer:

.. snippet:: howto_operate_traffic
    .. success:: sequencer1.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(sequencer1.synchronizer_id)

If the field ``trafficControl`` of type ``TrafficControlParameters`` is not set (absent in the output above)
or has ``enforceRateLimiting`` set to ``false`` then the traffic management is inactive.

To enable traffic management, you update the dynamic synchronizer parameters, setting the `TrafficControlParameters`
with the ``enforceRateLimiting = true`` and specifying desired values for the other parameters
documented in :brokenref:`configuration class Scaladoc reference`.

.. note::
    If you are using a Synchronizer with multiple owners, you need to ensure that the command
    to enable traffic management is submitted by at least the configured threshold of owners.

Assuming ``sequencer1`` being the only synchronizer owner, run the following command to enable traffic management:

.. snippet:: howto_operate_traffic
    .. success:: import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveNumeric}
      import com.digitalasset.canton.config.PositiveFiniteDuration
      import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
      val trafficControlParameters = TrafficControlParameters(
        enforceRateLimiting = true,
        maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(20000L),
        readVsWriteScalingFactor = PositiveNumeric.tryCreate(200),
        maxBaseTrafficAccumulationDuration = PositiveFiniteDuration.ofSeconds(10L),
        setBalanceRequestSubmissionWindowSize = PositiveFiniteDuration.ofMinutes(5L),
        baseEventCost = NonNegativeNumeric.tryCreate(500L),
      )
      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = sequencer1.synchronizer_id,
        _.update(trafficControl = Some(trafficControlParameters)),
      )

Let's confirm that the traffic management is now enabled by checking the synchronizer parameters again:

.. snippet:: howto_operate_traffic
    .. hidden:: utils.retry_until_true(
        sequencer1.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(sequencer1.synchronizer_id)
          .trafficControl.isDefined
      )
    .. success:: sequencer1.topology.synchronizer_parameters
      .get_dynamic_synchronizer_parameters(sequencer1.synchronizer_id)

Note the ``traffic control`` in the output above.



Check the latest traffic balances of Synchronizer members
---------------------------------------------------------

One can interactively inspect and modify the traffic balances of Synchronizer members using the Canton console
commands under ``sequencer.traffic_control``.

To inspect the traffic balances of all members of a Synchronizer, you can use the following command:

.. snippet:: howto_operate_traffic
    .. success:: val allMembersTrafficState = sequencer1.traffic_control.traffic_state_of_all_members()
      allMembersTrafficState

If you only want to check the traffic balance of a specific member, you can use:

.. snippet:: howto_operate_traffic
    .. success:: sequencer1.traffic_control.traffic_state_of_members(Seq(participant1))



Top up the traffic balance for a Synchronizer member
----------------------------------------------------

.. note::
    Traffic balance entitlements of members are decided by an external workflow and communicated
    via submitting the same traffic balance via one or multiple Sequencers.

.. note::
    Top ups must be submitted by a quorum of Sequencers to become effective. This is configured by the
    ``threshold`` parameter of the ``SequencerSynchronizerState`` topology mapping.

.. todo:: <https://github.com/DACH-NY/canton/issues/25832>
    #. Link to the reference documentation of the ``SequencerSynchronizerState`` topology mapping above.

Let's add some traffic for a member, for example, ``participant1``. First we need to know the current ``serial``
(a per-member monotonically increasing ``PositiveInt``), which corresponds
to the last traffic balance top up for that member.

.. snippet:: howto_operate_traffic
    .. success:: val nextSerial = allMembersTrafficState.trafficStates(participant1).serial
      .getOrElse(PositiveNumeric.tryCreate(1))
      .increment


Now we can submit a command to increase the traffic balance for ``participant1`` by ``newBalance``:

.. snippet:: howto_operate_traffic
    .. success:: sequencer1.traffic_control.set_traffic_balance(
        member = participant1,
        serial =  nextSerial,
        newBalance = NonNegativeNumeric.tryCreate(1000000L),
      )

Now the traffic balance for ``participant1`` has been updated. You can verify this by checking the traffic state again:

.. snippet:: howto_operate_traffic
    .. success:: utils.retry_until_true(
        sequencer1.traffic_control.traffic_state_of_members(Seq(participant1))
          .trafficStates(participant1)
          .serial
          .exists(_ >= nextSerial)
      )
    .. success:: val trafficStateBeforePing = sequencer1.traffic_control.traffic_state_of_members(Seq(participant1))
      trafficStateBeforePing

Now let's run ``ping`` between participants and observe the traffic consumption:

.. snippet:: howto_operate_traffic
    .. success:: participant1.health.ping(participant2)
    .. success:: participant2.health.ping(participant3)
    .. success:: sequencer1.traffic_control.traffic_state_of_all_members()

Observe the traffic balances for the Participants and the Mediator decrease.

For more information on traffic control and its configuration parameters, read the :externalref:`traffic control overview <overview-explanation-traffic-management>`.
