// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Availability
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.BigDecimalImplicits.IntToBigDecimal
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.protocol.messages.{ConfirmationResultMessage, Verdict}
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.util.MaliciousParticipantNode
import org.slf4j.event.Level

/** Regression test for a participant crash loop: the message dispatcher only considered the
  * confirmation results addressed to its synchronizer, but the result processor re-extracted them
  * from the unfiltered batch and failed if there was more than one.
  */
final class ForeignSynchronizerConfirmationResultIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  private lazy val foreignConfirmationResult: SecurityTest = SecurityTest(
    property = Availability,
    asset = "participant node",
    attack = Attack(
      actor = "a malicious network participant",
      threat =
        "sends a batch with a confirmation result for the victim's synchronizer and one for a foreign synchronizer",
      mitigation =
        "the victim ignores the foreign result, processes the other one, and stays connected",
    ),
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)
      }

  registerPlugin(new UsePostgres(loggerFactory))

  "A participant" when {
    "receiving a batch with confirmation results for its own and a foreign synchronizer" should {
      "stay connected and survive a restart" taggedAs foreignConfirmationResult in { implicit env =>
        import env.*

        val victim = participant1
        val attacker = MaliciousParticipantNode(
          participant2,
          daId,
          testedProtocolVersion,
          testedTransactionLimits,
          timeouts,
          loggerFactory,
        )

        // Pick a real request that the victim has already finalized,
        // so the victim rejects the result for its synchronizer as a duplicate
        val requestId = {
          val iouCommand = new Iou(
            victim.adminParty.toProtoPrimitive,
            victim.adminParty.toProtoPrimitive,
            new Amount(1.toBigDecimal, "stones"),
            new java.util.ArrayList(),
          ).create.commands.loneElement
          val tx = victim.ledger_api.javaapi.commands
            .submit(actAs = Seq(victim.adminParty), commands = Seq(iouCommand))
          RequestId(CantonTimestamp.fromInstant(tx.getRecordTime).value)
        }

        def confirmationResult(psid: PhysicalSynchronizerId): ConfirmationResultMessage =
          ConfirmationResultMessage.create(
            psid,
            ViewType.TransactionViewType,
            requestId,
            RootHash(TestHash.digest(1)),
            Verdict.Approve.create(testedProtocolVersion),
            testedProtocolVersion,
          )

        val foreignPsid = SynchronizerId.tryFromString("foreign::synchronizer").toPhysical

        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
          attacker
            .submitConfirmationResults(
              Seq(
                confirmationResult(daId) -> Recipients.cc(victim.id),
                confirmationResult(foreignPsid) -> Recipients.cc(victim.id),
              )
            )
            .futureValueUS
            .value,
          logEntries => {
            // The dispatcher alarms about the foreign result...
            forAtLeast(1, logEntries)(
              _.warningMessage should (include("wrong synchronizer ids") and include(
                foreignPsid.toString
              ))
            )
            // ... and passes on the result for its own synchronizer,
            // which gets discarded because the request is no longer pending.
            forAtLeast(1, logEntries)(
              _.infoMessage should (include(s"request at $requestId: Received event at") and
                include("for request that is not pending"))
            )
            // No errors
            forAll(logEntries)(_.level should not be Level.ERROR)
          },
        )

        // The participant is still connected and processing events
        victim.health.ping(participant2)

        // Restarting the participant does not lead to a crash loop either
        victim.stop()
        victim.start()
        victim.synchronizers.reconnect_all(ignoreFailures = false)
        eventually() {
          victim.synchronizers.list_connected().map(_.physicalSynchronizerId) should contain(
            daId
          )
        }
        victim.health.ping(participant2)
      }
    }
  }
}
