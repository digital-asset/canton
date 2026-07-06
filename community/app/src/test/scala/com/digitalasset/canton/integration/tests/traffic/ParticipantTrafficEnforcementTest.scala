// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.traffic

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.util.{TestUtils, TrafficControlUtils}
import com.digitalasset.canton.ledger.error.CommonErrors.ServiceNotRunning
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.platform.config.{
  TrafficEnforcementConfig,
  TrafficEnforcementServerConfig,
}
import com.digitalasset.canton.topology.ExternalParty
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.DurationInt

sealed trait ParticipantTrafficEnforcementTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  protected var aliceE: ExternalParty = _

  /** Participant config transforms to enable or disable participant-local traffic enforcement.
    */
  protected def participantConfigTransforms: Seq[ConfigTransform]

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(participantConfigTransforms*)
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.trafficConfig.pruningRetentionWindow)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(5))
            .focus(_.trafficConfig.trafficPurchasedCacheSizePerMember)
            .replace(PositiveInt.one)
        )
      )
      .withSetup { implicit env =>
        import env.*
        participants.local.foreach { participant =>
          participant.synchronizers.connect_local(sequencer1, alias = daName)
          participant.dars.upload(CantonExamplesPath, synchronizerId = daId)
        }

        aliceE = participant1.parties.testing.external.enable("Alice")
      }
      .withTrafficControl(
        TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
        trafficControlParameters = TrafficControlUtils.predictableTraffic,
        topUpAllMembers = true,
        disableCommitments = true,
      )

  protected def assertUnimplemented(entry: LogEntry): Assertion =
    entry.message should include(Status.Code.UNIMPLEMENTED.toString)
}

final class ParticipantTrafficEnforcementDisabledTest extends ParticipantTrafficEnforcementTest {
  override protected def participantConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement).replace(TrafficEnforcementConfig(enabled = false))
    )
  )

  "Participant" when {
    "traffic enforcement is disabled" should {
      "not expose the traffic service endpoints on the Ledger API" in { implicit env =>
        import env.*

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.traffic.get_account(aliceE.partyId.toProtoPrimitive),
          assertUnimplemented,
        )

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.traffic.update_account(
            aliceE.partyId.toProtoPrimitive,
            None,
          ),
          assertUnimplemented,
        )
      }
    }

    "support interactive submissions" in { implicit env =>
      import env.*

      // Pass some time to allow traffic re-fill the submission below
      environment.simClock.value.advance(Duration.ofSeconds(5L))

      // Prepare and execute should work seamlessly
      val prepared = participant1.ledger_api.interactive_submission.prepare(
        actAs = Seq(aliceE),
        commands = Seq(createCycleCommand(aliceE.partyId, "traffic")),
        hashingSchemeVersion = testedApiHashingSchemeVersion,
      )

      participant1.ledger_api.interactive_submission.execute_and_wait(
        prepared.getPreparedTransaction,
        Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
        UUID.randomUUID().toString,
        prepared.hashingSchemeVersion,
      )
    }
  }
}

final class ParticipantTrafficEnforcementEnabledTest extends ParticipantTrafficEnforcementTest {
  private val nonExistentTeaServerName = "non-existent-tea-server"

  override protected def participantConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateParticipantConfig("participant1")(
      _.focus(_.trafficEnforcement)
        .replace(
          TrafficEnforcementConfig(
            enabled = true,
            trafficEnforcementServer =
              TrafficEnforcementServerConfig.Internal(nonExistentTeaServerName),
          )
        )
    ),
    // Shorten network timeout so retries to the non-existent traffic service give up quickly
    _.focus(_.parameters.timeouts.processing.network)
      .replace(config.NonNegativeDuration.tryFromDuration(5.seconds)),
  )

  "Participant" when {
    "traffic enforcement is enabled" should {
      "serve traffic service operations" in { implicit env =>
        import env.*

        val alice = aliceE.partyId.toProtoPrimitive

        // Initially Alice has no balance
        participant1.ledger_api.traffic.get_account(alice).balance shouldBe 0L

        val aliceBalance = 1_000_000L

        // Update Alice's balance
        participant1.ledger_api.traffic.update_account(alice, balanceDelta = Some(aliceBalance))

        // Check correct balance for Alice
        participant1.ledger_api.traffic.get_account(alice).balance shouldBe aliceBalance

        // Now deduct some traffic from Alice's account
        val deductAmount = 100_000L
        participant1.ledger_api.traffic.update_account(alice, balanceDelta = Some(-deductAmount))

        // Check correct balance for Alice after deduction
        participant1.ledger_api.traffic
          .get_account(alice)
          .balance shouldBe (aliceBalance - deductAmount)
      }
    }

    "traffic enforcement is enabled but traffic enforcement server is not available" should {
      // TODO(#33681): Re-enable this test once we can test traffic enforcement enabled with a unavailable traffic enforcement server
      "return graceful errors on traffic and interactive submission service endpoints" ignore {
        implicit env =>
          import env.*

          def assertEntriesTeaUnavailable(entries: Seq[LogEntry]): Assertion =
            entries.foldLeft(succeed) { case (_, entry) =>
              entry.message should ((include(ServiceNotRunning.id) and include(
                "User traffic service is not running"
              )) or
                (include(Status.Code.UNAVAILABLE.toString) and include(
                  s"Could not find server: $nonExistentTeaServerName"
                ) or
                  include("Retry timeout has elapsed, giving up.")))
            }

          // GetAccount on P1 fails due to TEA not enabled
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.traffic.get_account(aliceE.partyId.toProtoPrimitive),
            assertEntriesTeaUnavailable,
          )

          // UpdateAccount on P1 fails due to TEA not enabled
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.traffic.update_account(
              aliceE.partyId.toProtoPrimitive,
              None,
            ),
            assertEntriesTeaUnavailable,
          )

          // Preparing on P1 fails due to TEA not enabled
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.interactive_submission.prepare(
              actAs = Seq(aliceE),
              commands = Seq(createCycleCommand(aliceE.partyId, "traffic")),
              hashingSchemeVersion = testedApiHashingSchemeVersion,
            ),
            assertEntriesTeaUnavailable,
          )

          // Prepare a transaction on P2 for Alice (P2 does not have traffic enabled so we can prepare)
          val prepared = participant2.ledger_api.interactive_submission.prepare(
            actAs = Seq(aliceE),
            commands = Seq(createCycleCommand(aliceE.partyId, "traffic")),
            hashingSchemeVersion = testedApiHashingSchemeVersion,
          )

          // Executing on P1 fails due to TEA not enabled
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.ledger_api.interactive_submission.execute_and_wait(
              prepared.getPreparedTransaction,
              Map(aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)),
              UUID.randomUUID().toString,
              prepared.hashingSchemeVersion,
              // Short timeout to reduce test time
              optTimeout = Some(5.seconds),
            ),
            _.foldLeft(succeed) { case (_, entry) =>
              entry.message should ((include(ServiceNotRunning.id) and include(
                "User traffic service is not running"
              )) or
                (include(Status.Code.UNAVAILABLE.toString) and include(
                  s"Could not find server: $nonExistentTeaServerName"
                )) or
                include("Retry timeout has elapsed, giving up.") or
                include("Failed to submit submission") or
                include("DEADLINE_EXCEEDED"))
            },
          )
      }
    }
  }
}
