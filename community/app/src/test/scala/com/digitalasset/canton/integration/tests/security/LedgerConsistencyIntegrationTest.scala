// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceAlarm,
  SyncServiceSynchronizerDisconnect,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
}
import com.digitalasset.canton.util.MaliciousParticipantNode
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

// Unstable for now, as committing after failed activeness check is not supported and
// can have surprising consequences.
// TODO(i12904): Mark stable, once this is supported.
@UnstableTest
abstract sealed class LedgerConsistencyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestHelpers
    with HasProgrammableSequencer
    with HasCycleUtils {

  // Using AtomicRef, because this gets read from various threads.
  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  var maliciousP1: MaliciousParticipantNode = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.parameters.commitAfterFailedActivenessCheck).replace(true)
        ),
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.commitAfterFailedActivenessCheck).replace(false)
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.synchronizers.connect_local(sequencer1, alias = daName)
        participants.local.dars.upload(CantonTestsPath)

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

  "A participant" can {
    "fail gracefully if an archived contract is used again" in { implicit env =>
      import env.*

      // Participant1 submits the duplicate archival and must fail gracefully, because commitAfterFailedActivenessCheck == true.
      val payer = participant1.adminParty

      // Participant2 must crash, because commitAfterFailedActivenessCheck == false.
      val owner = participant2.adminParty

      // Create an iou
      val iou = IouSyntax.createIou(participant1)(payer, owner)

      // Archive the iou twice
      val archiveCmds = iou.id
        .exerciseArchive()
        .commands()
        .asScala
        .toSeq
        .map(c => Command.fromJavaProto(c.toProtoCommand))

      def mkCmd: CommandsWithMetadata =
        CommandsWithMetadata(archiveCmds, Seq(payer), ledgerTime = environment.now.toLf)

      // The first attempt succeeds, as iou has been active.
      val (_, events1) = trackingLedgerEvents(Seq(participant1), Seq.empty) {
        maliciousP1.submitCommand(mkCmd).futureValueUS.valueOrFail("Submission failed")
      }
      events1.assertStatusOk(participant1)

      // The second attempt succeeds only because
      // - we are replacing the local verdict of participant1 by "approve"
      //   (participant2 does not need to approve, as it merely hosts an observer.)
      // - we have configured commitAfterFailedActivenessCheck
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        replacingConfirmationResponses(
          participant1,
          sequencer1,
          daId,
          withLocalVerdict(localApprove),
        ) {
          val (_, events2) = trackingLedgerEvents(Seq(participant1), Seq.empty) {
            maliciousP1.submitCommand(mkCmd).futureValueUS.valueOrFail("Submission failed")
          }

          events2.assertStatusOk(participant1)

          // Check that p2 is broken
          participant2.health.maybe_ping(participant2, 2.seconds) shouldBe empty
          participant2.synchronizers.list_connected() shouldBe empty
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                SyncServiceAlarm,
                _ shouldBe "Mediator approved a request that has been locally rejected.",
              ),
              "Unexpected mediator approval",
            ),
            (
              _.shouldBeCantonError(
                SyncServiceAlarm,
                _ should fullyMatch regex raw"Request RequestId\(\S+\) with failed activeness check is approved\.",
                loggerAssertion = _ should include("participant=participant2"),
              ),
              "Failed activeness check",
            ),
            (
              _.shouldBeCantonError(
                SyncServiceAlarm,
                _ should fullyMatch regex raw"Request RequestId\(\S+\) with failed activeness check is approved\.",
                loggerAssertion = _ should include("participant=participant2"),
              ),
              "Failed activeness check",
            ),
            (
              _.shouldBeCantonError(
                SyncServiceSynchronizerDisconnect,
                _ should (startWith(
                  "Synchronizer 'synchronizer1' fatally disconnected because of handler returned error:"
                ) and
                  include regex raw"Request RequestId\(\S+\) with failed activeness check is approved\."),
                loggerAssertion = _ should include("participant=participant2"),
              ),
              "Synchronizer disconnect",
            ),
          ),
          mayContain = Seq(
            // Tolerate arbitrary further messages from participant2.
            _.loggerName should include("participant=participant2")
          ),
        ),
      )
    }
  }
}

final class LedgerConsistencyIntegrationTestPostgres extends LedgerConsistencyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

// Need to test in memory too to cover all code paths.
final class LedgerConsistencyIntegrationTestInMemory extends LedgerConsistencyIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
