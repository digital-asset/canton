// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{LocalParticipantReference, SequencerReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.util.TestUtils
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.Database
import com.digitalasset.canton.synchronizer.sequencer.SequencerMemberStatus.Active
import com.digitalasset.canton.synchronizer.sequencer.{
  SequencerMemberStatus,
  SequencerPruningStatus,
  SequencerReaderConfig,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.util.ShowUtil.*
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.util.Random

trait SequencerPruningIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected var participant3Id: ParticipantId = _

  protected val sequencerClientAcknowledgementInterval =
    config.NonNegativeFiniteDuration.ofMillis(500)
  protected val participant3SequencerClientAcknowledgementInterval =
    sequencerClientAcknowledgementInterval * 10

  protected val dbSequencerConfig = Database(
    reader = SequencerReaderConfig(
      // effectively allow checkpointing every event
      checkpointInterval = config.NonNegativeFiniteDuration.ofMillis(1)
    )
  )

  // drastically reduce the ack time so we are able to quickly prune data they've acknowledged
  protected val reduceSequencerClientAcknowledgementInterval: ConfigTransform =
    ConfigTransforms.updateAllSequencerClientConfigs_(
      _.focus(_.acknowledgementInterval).replace(sequencerClientAcknowledgementInterval)
    )

  // disable the acknowledgement conflate window to prevent conflating acknowledgements
  protected val reduceSequencerAcknowledgementConflateWindow: ConfigTransform =
    ConfigTransforms.updateAllSequencerConfigs_(
      _.focus(_.acknowledgementsConflateWindow).replace(None)
    )

  // participant 3 has higher acknowledgement interval so we can simulate a member falling behind in acknowledgements
  protected val increaseParticipant3AcknowledgementInterval: ConfigTransform =
    ConfigTransforms.updateParticipantConfig("participant3") { config =>
      config
        .focus(_.sequencerClient.acknowledgementInterval)
        .replace(participant3SequencerClientAcknowledgementInterval)
    }

  private def earliestAckExcludingParticipant(
      status: SequencerPruningStatus,
      participantId: ParticipantId,
  ): Option[CantonTimestamp] =
    status.members
      .filterNot(_.member == participantId)
      .flatMap {
        case SequencerMemberStatus.Onboarding(_) => Nil
        case SequencerMemberStatus.Active(_, _, lastAcknowledged) =>
          lastAcknowledged.toList
        case SequencerMemberStatus.Disabled(_, _, lastAcknowledged) =>
          lastAcknowledged.toList
      }
      .minOption

  private def onboardParticipant(participant: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ) = {
    import env.*

    participant.synchronizers.connect_local(sequencer1, alias = daName)

    // Pruning status excludes members registered after the sequencer's clock time.
    //  BFT sequencing time can run ahead of the static clock, so advance it to a timestamp
    //  observed by the participant after registration.
    val synchronizerTime = participant.testing.fetch_synchronizer_time(daId)
    val simClock = environment.simClock.value
    simClock.advanceTo(simClock.now.max(synchronizerTime))

    eventually() {
      participant.synchronizers.list_connected().map(_.synchronizerAlias) should contain(daName)
      // user-manual-entry-begin: SequencerPruningStatus
      val status = sequencer1.pruning.status()
      // user-manual-entry-end: SequencerPruningStatus
      status.members.map(_.member) should contain(participant.id)
    }
  }

  protected def sendMsgToAllMembers()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val partyName = HexString.toHexString(Random.nextBytes(5))
    participant1.parties.enable(
      partyName
      // Also wait until the synchronizers have processed the receipt of the topology change

    )
  }

  protected def waitForAllEnabledMembersToAck(
      sequencer: SequencerReference,
      acknowledgementBound: CantonTimestamp,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    logger.info(s"Waiting for acknowledgement bound at $acknowledgementBound")
    eventually(timeUntilSuccess = 60.seconds) {
      // Acknowledgements are only sent when the sim clock ticks. The blocks carrying the required sequencing
      // time may be delivered after the caller's tick, so tick again on every retry.
      environment.simClock.value.advance(participant3SequencerClientAcknowledgementInterval.asJava)

      val status = sequencer.pruning.status()
      val members = status.members.collect { case m: Active => m }

      forAll(members) { member =>
        assert(member.lastAcknowledged.exists(_ >= acknowledgementBound))
      }
    }
  }

  protected val pruningRegexWithTrafficPurchase =
    """Removed at least ([1-9]\d*) events, at least (\d+) payloads"""

  protected val pruningRegex =
    """Removed at least ([1-9]\d*) events, at least (\d+) payloads"""

  protected val pruningNothing =
    """Removed at least 0 events, at least 0 payloads"""

  "prune only removes events up the point where all enabled clients have acknowledgements" in {
    implicit env =>
      import env.*

      // this is needed because in the Fabric Sequencer only events take the current time, while member registration
      // takes the latest event timestamp plus 1 micro. So we need to create these events so that the participant3
      // registration will add the member with the current timestamp.
      // A proper fix for this will be to actually use the current timestamp for member registration.
      participant1.health.ping(participant2)

      onboardParticipant(participant3)
      participant3Id = participant3.id

      // Submit two traffic purchases so pruning can remove the older entry.
      // The latest entry is always retained to preserve the member's traffic balance.
      sequencer1.traffic_control.set_traffic_balance(
        participant1,
        PositiveInt.one,
        NonNegativeLong.tryCreate(500),
      )
      sequencer1.traffic_control.set_traffic_balance(
        participant1,
        PositiveInt.two,
        NonNegativeLong.tryCreate(1000),
      )

      // Purchases are submitted asynchronously. Wait for the latest balance before the one-hour
      // clock jump, which could otherwise move an in-flight purchase past its sequencing deadline.
      // Generate sequencer activity so the updated traffic state becomes visible.
      sendMsgToAllMembers()
      eventually(timeUntilSuccess = 60.seconds) {
        sequencer1.traffic_control
          .traffic_state_of_members(Seq(participant1.id))
          .trafficStates
          .get(participant1.id)
          .map(_.extraTrafficPurchased.value) shouldBe Some(1000)
      }

      // Advance past participant3's acknowledgement interval so it can acknowledge earlier events.
      environment.simClock.value.advance(config.NonNegativeFiniteDuration.ofHours(1).asJava)

      // Generate activity after the pruning bound and wait for party enablement to complete
      // before advancing the clock again to trigger acknowledgements.
      val startTime = environment.simClock.value.now
      // A block proposed before the clock jump can still be ordered with a stale BFT time and carry the
      // party enablement. Wait until the sequencer has caught up with the jump so the activity below
      // is sequenced at or after startTime.
      TestUtils.waitForTargetTimeOnSynchronizerNode(startTime, logger)(sequencer1)
      sendMsgToAllMembers()

      // Trigger acknowledgements for the new activity, including participant3's slower timer.
      val acknowledgementBound = startTime
      environment.simClock.value.advance(participant3SequencerClientAcknowledgementInterval.asJava)

      // wait until all sequencer clients have acknowledged something
      waitForAllEnabledMembersToAck(sequencer1, acknowledgementBound)

      // initial pruning will remove events up until the safe point
      val result = sequencer1.pruning.prune_at(acknowledgementBound)
      result should fullyMatch regex pruningRegexWithTrafficPurchase

      // send a bunch of stuff so there's stuff to prune
      participant1.testing.bong(
        targets = Set(participant1.id, participant2.id),
        levels = 3,
        timeout = 60.seconds,
      )

      // advance multiples of the ack interval to ensure that sequencer clients should eventually ack where they're at
      environment.simClock.value.advance(
        sequencerClientAcknowledgementInterval.asJava.multipliedBy(2)
      )

      // pruning at this point should do nothing (this point being now - default retention period of 7 days)
      sequencer1.pruning.prune() shouldBe pruningNothing
  }

  "dry run of forced prune should tell us which members would be disabled" in { implicit env =>
    import env.*
    {
      sendMsgToAllMembers()
      // advance multiples of the ack interval to ensure that sequencer clients should eventually ack where they're at
      // except for participant 3 which has a higher acknowledgement interval
      val advanceBy = sequencerClientAcknowledgementInterval.asJava.multipliedBy(2)
      environment.simClock.foreach(_.advance(advanceBy))
    }

    // dry run of forced prune for now should say it would require disabling participant3
    eventually() {
      val status = sequencer1.pruning.status()

      for {
        earliestAck <- earliestAckExcludingParticipant(status, participant3.id)
        ackParticipant3 <- status.members.collectFirst {
          case member: Active if member.member == participant3.id =>
            member.safePruningTimestamp
        }
      } yield {
        assert(ackParticipant3.isBefore(earliestAck))
        val members = status.clientsPreventingPruning(earliestAck).members

        inside(members.toList.sortBy(_.toProtoPrimitive)) {
          case List(
                p3
              ) =>
            p3 shouldBe participant3.id
            sequencer1.pruning.force_prune_at(earliestAck, dryRun = true) shouldBe
              show"""To prune the Sequencer at $earliestAck we will disable:
                    |  - $p3 (member)
                    |To disable these clients to allow for pruning at this point run force_prune with dryRun set to false""".stripMargin
        }
      }
    }
  }

  "force pruning" in { implicit env =>
    import env.*

    // Record the current time to assert on the `earliestAck`
    val timeBeforeTraffic = environment.simClock.value.now
    // Create extra traffic to isolate this test case from the previous one
    sendMsgToAllMembers()

    // Advance the clock so participants 1 and 2 process and acknowledge the new traffic
    environment.simClock.value.advance(
      sequencerClientAcknowledgementInterval.asJava.multipliedBy(2)
    )

    // Wait for the healthy participants to actually acknowledge the new traffic
    val earliestAck = eventually() {
      val status = sequencer1.pruning.status()
      val probeEarliestAck = earliestAckExcludingParticipant(status, participant3Id).value

      assert(
        probeEarliestAck.isAfter(timeBeforeTraffic),
        s"Earliest ack ($probeEarliestAck) has not yet passed traffic generation time ($timeBeforeTraffic)",
      )
      probeEarliestAck
    }

    // Stopping participant3 which will get disabled to avoid it logging errors
    participant3.stop()

    // Prune using the asserted timestamp to prevent test flakiness
    // The exact number of records removed will depend on the number of deliver events received so use a regex
    // user-manual-entry-begin: SequencerForcePruning
    val result = sequencer1.pruning.force_prune_at(earliestAck, dryRun = false)
    // user-manual-entry-end: SequencerForcePruning

    result should fullyMatch regex
      s"""$pruningRegex
         |Disabled the following members:
         |  - $participant3Id
         |""".stripMargin
  }

  "moving past the default retention period after some activity" in { implicit env =>
    import env.*

    // send a bunch of stuff so there's stuff to prune
    participant1.testing.bong(
      targets = Set(participant1.id, participant2.id),
      levels = 4,
      timeout = 120.seconds,
    )

    val pastDefaultRetentionTimestamp =
      sequencer1.pruning
        .status()
        .now
        .add(environment.config.parameters.retentionPeriodDefaults.sequencer.asJava.plusMillis(1))

    // move past the default retention period
    environment.simClock.value.advanceTo(pastDefaultRetentionTimestamp)
    TestUtils.waitForTargetTimeOnSynchronizerNode(environment.simClock.value.now, logger)(
      sequencer1
    )

    // do some more stuff and move forward to convince some acks to occur
    participant1.health.ping(participant2)
    environment.simClock.value.advance(
      sequencerClientAcknowledgementInterval.asJava.multipliedBy(2)
    )
    TestUtils.waitForTargetTimeOnSynchronizerNode(environment.simClock.value.now, logger)(
      sequencer1
    )

    // Make sure we're all ahead of this default retention period.
    //  Advancing sim time triggers participant acknowledgements that are ordered asynchronously.
    //  Allow 60s for ordering to complete, matching the patience in waitForAllEnabledMembersToAck,
    //  instead of eventually's 20s default.
    eventually(timeUntilSuccess = 60.seconds) {
      earliestAckExcludingParticipant(
        sequencer1.pruning.status(),
        participant3Id,
      ).value shouldBe >=(
        pastDefaultRetentionTimestamp
      )
    }

    // now the default pruning should work as all enabled members should have acked
    // user-manual-entry-begin: SequencerPrune
    val result = sequencer1.pruning.prune()
    // user-manual-entry-end: SequencerPrune
    result should fullyMatch regex s"""$pruningRegex"""
  }

  "check members still work after a restart" in { implicit env =>
    import env.*

    import scala.jdk.DurationConverters.*

    // check things still work by restarting everything (which will certainly force participants to reconnect to the sequencer)
    stopAll()
    // advance a little to ensure the sequencer writer can start without having to wait for the prior watermark
    val simClock = environment.simClock.value
    simClock.advance(1.milli.toJava)

    nodes.local.start()
    participant1.health.wait_for_initialized()
    participant2.health.wait_for_initialized()

    Set(participant1, participant2).foreach(_.synchronizers.reconnect_all())

    // then doing a simple ping
    participant1.health.ping(participant2)
  }

}

trait SequencerNodePruningIntegrationTest extends SequencerPruningIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        reduceSequencerClientAcknowledgementInterval,
        reduceSequencerAcknowledgementConflateWindow,
        increaseParticipant3AcknowledgementInterval,
        ConfigTransforms.useStaticTime,
        setupSequencerConfig,
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer1, alias = daName)
      }

  private val setupSequencerConfig: ConfigTransform =
    ConfigTransforms.updateSequencerConfig("sequencer1") { config =>
      config
        .focus(_.sequencer)
        .replace(dbSequencerConfig)
    }
}

// TODO(#16089): Currently this test cannot work due to the issue. This is why it's commented out.
//class SequencerNodePruningIntegrationTestH2 extends SequencerNodePruningIntegrationTest {
//  // The synchronizer config by default uses in-memory, so the synchronizer id doesn't survive a synchronizer restart
//  registerPlugin(new UseH2(loggerFactory))
//}
