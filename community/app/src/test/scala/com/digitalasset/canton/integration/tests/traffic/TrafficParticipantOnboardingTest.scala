// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveLong}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.environment.CantonEnvironment
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.{
  BaseEnvironmentDefinition,
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.TrafficPurchasedRequestAsyncSendFailed
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration

class TrafficParticipantOnboardingTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with TrafficBalanceSupport {

  private val trafficControlParameters = TrafficControlParameters.default

  override protected def environmentDefinition
      : BaseEnvironmentDefinition[CantonConfig, CantonEnvironment] =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*
      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        _.update(trafficControl = Some(trafficControlParameters)),
      )
    }

  // Below PV37 an onboarding member cannot be represented in the sequencer snapshot, so purchasing
  // traffic for a member that is not registered yet is rejected outright.
  "reject a top up for an unregistered member" onlyRunLessThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        updateBalanceForMember(
          participant1,
          PositiveLong.one,
          () => {
            // Advance the clock just slightly so we can observe the new balance be effective
            env.environment.simClock.value.advance(Duration.ofMillis(1))
          },
        ),
        _.errorMessage should include(TrafficPurchasedRequestAsyncSendFailed.id),
      )
  }

  // An unregistered member is registered as `Onboarding` on the sequencer, which the v30 sequencer
  // snapshot used below PV37 cannot represent.
  "top up balance for an unregistered member" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val firstTopUpBalance = NonNegativeLong.tryCreate(50000)
      val topUpBalance = NonNegativeLong.tryCreate(100000)
      val member = participant1 // unregistered member
      val firstSerial = sendTopUp(member, firstTopUpBalance)

      // The traffic state of a member that is still onboarding is returned on request
      val firstState = eventually() {
        val (_, state) = sequencer1.traffic_control
          .traffic_state_of_members(Seq(member), includeOnboardingMembers = true)
          .trafficStates
          .loneElement
        state.serial shouldBe Some(firstSerial)
        state.extraTrafficPurchased shouldBe firstTopUpBalance
        state
      }
      sequencer1.traffic_control
        .traffic_state_of_all_members(includeOnboardingMembers = true)
        .trafficStates
        .keySet should contain(member.id: Member)

      // ... and left out by default
      sequencer1.traffic_control.traffic_state_of_members(Seq(member)).trafficStates shouldBe empty
      sequencer1.traffic_control
        .traffic_state_of_all_members()
        .trafficStates
        .keySet should not contain (member.id: Member)

      // The queried state gives the serial to use for the next top up
      val serial = sendTopUp(member, topUpBalance, Some(firstState.serial.value.increment.value))

      eventually() {
        val (_, state) = sequencer1.traffic_control
          .traffic_state_of_members(Seq(member), includeOnboardingMembers = true)
          .trafficStates
          .loneElement
        state.serial shouldBe Some(serial)
        state.extraTrafficPurchased shouldBe topUpBalance
      }

      // Start and connect the new member
      member.start()
      member.health.wait_for_running()
      member.synchronizers.connect_local(sequencer1, daName)

      // Ensure the balance update is seen by sequencer1 by checking the serial
      eventually() {
        val trafficStateSequencer =
          sequencer1.traffic_control.traffic_state_of_members(Seq(member))
        val (_, state) = trafficStateSequencer.trafficStates.loneElement
        state.serial shouldBe Some(serial)
        state.extraTrafficPurchased shouldBe topUpBalance

        // Also verify that the traffic state has reached the participant node
        val trafficStateParticipant =
          member.traffic_control.traffic_state(sequencer1.synchronizer_id)
        trafficStateParticipant.extraTrafficPurchased shouldBe topUpBalance
      }
  }
}
