// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.syntax.functor.*
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveLong,
}
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalSequencerReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality.Optional
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.topology.Member
import com.google.protobuf.ByteString
import org.scalatest.Assertion

import java.time.Duration

/** This trait provides helpers for the traffic management in the context of LSU tests. The main
  * goal is to improve readability of each tests by focusing on the behavior we want to test and
  * make it easier to write new tests.
  */
private[lsu] trait LsuTrafficManagement {
  self: CommunityIntegrationTest & TrafficBalanceSupport =>

  protected val baseEventCost = 500L
  protected val maxBaseTrafficAmount = 20_000L

  protected lazy val trafficControlParameters: TrafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(maxBaseTrafficAmount),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    // Enough to bootstrap the synchronizer and connect the participant after 1 second
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.tryCreate(baseEventCost),
    freeConfirmationResponses = true,
  )

  protected def initialTrafficPurchase(
      traffic: Map[InstanceReference, PositiveLong],
      sequencer: SequencerReference,
  )(implicit env: TestConsoleEnvironment): Assertion = {
    forAll(traffic) { case (node, traffic) =>
      updateBalanceForMember(node, traffic, sequencer)
    }

    val participantsTraffic = traffic.collect { case (p: ParticipantReference, v) => p -> v }

    eventually() {
      forAll(participantsTraffic) { case (p, traffic) =>
        p.traffic_control
          .traffic_state(env.daId)
          .extraTrafficPurchased
          .value shouldBe traffic.value
      }
    }
  }

  protected def nodeSeesExtraTrafficPurchased(
      sequencer: SequencerReference,
      expectedValues: Map[Member, Long],
  ): Assertion = {
    val sequencerTrafficStatus =
      sequencer.traffic_control
        .traffic_state_of_members_approximate(expectedValues.keySet.toSeq)
        .trafficStates
        .fmap(_.extraTrafficPurchased.value)

    forAll(expectedValues) { case (member, expectedValue) =>
      sequencerTrafficStatus.get(member).value shouldBe expectedValue
    }
  }

  protected def updateBalanceForMember(
      instance: InstanceReference,
      newBalance: PositiveLong,
      sequencer: SequencerReference,
  )(implicit env: TestConsoleEnvironment): Assertion =
    updateBalanceForMember(
      instance,
      newBalance,
      () => {
        // Advance the clock just slightly so we can observe the new balance be effective
        env.environment.simClock.value.advance(Duration.ofMillis(1))
      },
      sequencer = sequencer,
    )

  protected def eventuallyGetTraffic(sequencer: LocalSequencerReference): ByteString =
    eventually(retryOnTestFailuresOnly = false) {
      loggerFactory.assertLogsUnorderedOptional(
        sequencer.traffic_control.get_lsu_state(),
        (Optional, _.shouldBeCantonErrorCode(SequencerError.NotAtUpgradeTimeOrBeyond)),
      )
    }
}
