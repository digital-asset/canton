// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.syntax.functor.*
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
  PositiveLong,
}
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalSequencerReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality.Optional
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.{SequencerErrors, TrafficState}
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.synchronizer.sequencing.traffic.TrafficPurchasedManager
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.google.protobuf.ByteString
import org.scalatest.Assertion

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration

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

  protected lazy val generousTrafficControlParameters: TrafficControlParameters =
    TrafficControlParameters(
      maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(1_000_000_000_000L),
      readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
      // Enough to bootstrap the synchronizer and connect the participant after 1 second
      maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
      setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
      enforceRateLimiting = true,
      baseEventCost = NonNegativeLong.tryCreate(1L),
      freeConfirmationResponses = true,
    )

  /** A member that is never registered with a sequencer. Traffic is purchased for it before the
    * LSU, so that each test also covers the traffic purchased for a member that is not registered
    * yet.
    */
  protected lazy val prePurchaseMember: Member = ParticipantId("lsu-pre-purchase")

  /** The amount and the serial of the latest purchase made for [[prePurchaseMember]], if any */
  private val prePurchase = new AtomicReference[Option[(NonNegativeLong, PositiveInt)]](None)

  /** The traffic state as seen by the sequencer. The admin API cannot be used here, as it only
    * returns the members that are known in the topology.
    */
  private def prePurchaseMemberTraffic(
      sequencer: LocalSequencerReference,
      timestamp: CantonTimestamp,
  ): TrafficState =
    sequencer.underlying.value.sequencer.sequencer
      .getTrafficStateAt(prePurchaseMember, timestamp)
      .futureValueUS
      .value
      .value

  private def assertPrePurchase(
      trafficState: TrafficState,
      purchase: (NonNegativeLong, PositiveInt),
  ): Assertion = {
    val (amount, serial) = purchase
    trafficState.extraTrafficPurchased shouldBe amount
    trafficState.serial shouldBe Some(serial)
    trafficState.extraTrafficConsumed shouldBe NonNegativeLong.zero
  }

  /** Purchase traffic for [[prePurchaseMember]], on top of its latest purchase if any. Use it after
    * an LSU to check that the new sequencers still accept purchases for that member, and to have
    * the next LSU transfer a purchase made on a successor.
    *
    * Does nothing unless purchasing traffic for a member that is not registered is supported by
    * both the protocol version of the synchronizer and the tested protocol version. The latter is
    * needed because a test may move to a successor running the tested protocol version, even from a
    * synchronizer running a higher one, and such a successor drops the purchase.
    */
  protected def purchaseTrafficForUnregisteredMember(
      sequencer: LocalSequencerReference
  )(implicit env: TestConsoleEnvironment): Unit =
    if (
      Seq(testedProtocolVersion, sequencer.physical_synchronizer_id.protocolVersion)
        .forall(TrafficPurchasedManager.isPrePurchaseSupported)
    ) {
      val purchase = prePurchase
        .get()
        .fold(NonNegativeLong.tryCreate(100_000L) -> PositiveInt.one) { case (amount, serial) =>
          (amount + amount, serial.increment.value)
        }
      val (amount, serial) = purchase
      sendTopUp(prePurchaseMember, amount, sequencer, Some(serial))

      eventually() {
        // A timestamp in the future gives the latest purchase processed by the sequencer
        val timestamp = env.environment.clock.now.plusSeconds(60)
        assertPrePurchase(prePurchaseMemberTraffic(sequencer, timestamp), purchase)
      }
      prePurchase.set(Some(purchase))
    }

  /** Check that the latest purchase made for [[prePurchaseMember]] is part of the traffic state
    * exported by the old sequencers, and that the new sequencers have it once it is imported.
    */
  private def assertPrePurchaseTransferred(
      traffic: ByteString,
      newSequencers: Seq[LocalSequencerReference],
  ): Unit =
    prePurchase.get().foreach { purchase =>
      val exported = LsuTrafficState
        .fromTrustedByteString(traffic)
        .value
        .membersTraffic
        .get(prePurchaseMember)
        .value
      assertPrePurchase(exported, purchase)

      // The new sequencers serve the traffic state from the upgrade time
      forAll(newSequencers) { sequencer =>
        assertPrePurchase(
          prePurchaseMemberTraffic(sequencer, CantonTimestamp.MinValue),
          purchase,
        )
      }
    }

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

  /** Transfer traffic from old sequencers to new ones.
    *
    * Prerequisite:
    *   - Time is after upgrade time
    *
    * @param suppressLogs
    *   Whether errors in the log (NotAtUpgradeTimeOrBeyond) should be suppressed. Use false if the
    *   call to transferTraffic is already in a suppression logger block (since those cannot be
    *   nested).
    */
  protected def transferTraffic(
      oldSequencers: Seq[LocalSequencerReference],
      newSequencers: Seq[LocalSequencerReference],
      suppressLogs: Boolean,
      trafficTsOverride: Option[CantonTimestamp],
      timeUntilSuccess: FiniteDuration,
      maxPollInterval: FiniteDuration,
  ): Unit = {
    val trafficStates = if (suppressLogs) {
      loggerFactory.assertLogsUnorderedOptional(
        eventually(timeUntilSuccess, maxPollInterval, retryOnTestFailuresOnly = false) {
          oldSequencers.map(s => (s.id -> s.traffic_control.get_lsu_state(trafficTsOverride)))
        }.toMap,
        (
          LogEntryOptionality.OptionalMany,
          _.shouldBeCantonErrorCode(SequencerError.NotAtUpgradeTimeOrBeyond),
        ),
        (
          LogEntryOptionality.OptionalMany,
          _.shouldBeCantonErrorCode(SequencerErrors.PassedUpgradeTime),
        ),
      )
    } else {
      eventually(timeUntilSuccess, maxPollInterval, retryOnTestFailuresOnly = false) {
        oldSequencers.map(s => (s.id -> s.traffic_control.get_lsu_state(trafficTsOverride)))
      }.toMap
    }

    if (trafficStates.values.toSet.sizeIs != 1) {
      fail(s"Not all sequencers agree on traffic state: $trafficStates")
    }

    val traffic: ByteString = trafficStates.head._2
    newSequencers.foreach(_.traffic_control.set_lsu_state(traffic))
    assertPrePurchaseTransferred(traffic, newSequencers)
  }
}
