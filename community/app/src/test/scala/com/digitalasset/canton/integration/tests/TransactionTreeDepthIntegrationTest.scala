// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.manysimplecontracts.Creator
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.SecurityTestHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.util.MaliciousParticipantNode
import monocle.Monocle.toAppliedFocusOps
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

final class TransactionTreeDepthIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  private var mallory: Party = _
  private var alice: Party = _
  private var bob: Party = _

  private var maliciousNode: MaliciousParticipantNode = _

  private val maxTreeDepth: PositiveInt = PositiveInt.tryCreate(10)

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S1M1_Config
      .withNetworkBootstrap { implicit env =>
        import env.*

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        val defaultSsp = EnvironmentDefinition.defaultStaticSynchronizerParameters

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            overrideStaticSynchronizerParameters = Option(
              defaultSsp
                .focus(_.synchronizerLimits.transactionProtocolLimits.maxTransactionTreeDepth)
                .replace(maxTreeDepth)
            ),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonTestsPath)

        mallory = participant1.parties.testing.enable("mallory")
        alice = participant2.parties.testing.enable("alice")
        bob = participant3.parties.testing.enable("bob")

        maliciousNode = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          testedTransactionLimits,
          timeouts,
          loggerFactory,
        )
      }

  /* A count of 1<<6 results in a tree depth of 10 as follows:
   *
   * A shift of 1<<n will result in a tree n+1 deep, but we need to add to this:
   * - 2 for the root views (as the create-and-exercise create 2 separate root views)
   * - 1 because the plus one contract forces a new parent to be created
   *
   * So for n=6 we get 6+1+2+1 = 10
   */
  private val depth10Count = 1 << 6
  private val depth11Count = 1 << 7

  private def buildCommandWithCount(count: Int): Seq[Command] =
    new Creator(mallory.toLf, mallory.toLf).createAnd
      .exerciseSpawnPlusOne(alice.toLf, bob.toLf, count)
      .commands
      .asScala
      .toSeq

  s"nested transactions at depth limit are allowed" in { implicit env =>
    import env.*
    val commands = buildCommandWithCount(depth10Count)
    participant1.ledger_api.javaapi.commands.submit(actAs = Seq(mallory), commands = commands)
  }

  s"deeply nested transactions are rejected at submission time" in { implicit env =>
    import env.*

    val commands = buildCommandWithCount(depth11Count)

    // A non-malicious participant cannot submit a deeply nested transaction
    assertThrowsAndLogsCommandFailures(
      participant1.ledger_api.javaapi.commands.submit(
        actAs = Seq(mallory),
        commands = commands,
      ),
      log => {
        log.errorMessage should include(
          s"The parse depth of the transaction view exceeded $maxTreeDepth"
        )
      },
    )
  }

  /** Here we test that the mediator detects and rejects deeply nested transactions */
  s"maliciously published deeply nested transactions are rejected by the mediator" in { _ =>
    val commands = buildCommandWithCount(depth11Count)

    // A malicious participant can submit a deeply nested transaction, but it will be rejected by the other participants
    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      maliciousNode
        .submitJavaApiCommand(
          actAs = Seq(mallory),
          commands = commands,
        )
        .futureValueUS
        .value,
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          // Both mallory/p1 and alice/p2 detect deeply nested structures and reject
          // bob/p3 does not detect a deeply nested structure so processes the request
          (
            log => {
              log.loggerName should include regex ("com.digitalasset.canton.participant.protocol.TransactionProcessor:.*participant=participant1")
              log.warningMessage should include(
                "The nesting of the protobuf message exceeds the limit of 10"
              )
            },
            "L1",
          ),
          (
            log => {
              log.loggerName should include regex ("com.digitalasset.canton.participant.protocol.TransactionProcessor:.*participant=participant2")
              log.warningMessage should include(
                "The nesting of the protobuf message exceeds the limit of 10"
              )
            },
            "L2",
          ),
          (
            log => {
              log.mdc.get("participant") shouldBe Some("participant1")
              log.warningMessage should include regex ("LOCAL_VERDICT_MALFORMED_PAYLOAD.*The nesting of the protobuf message exceeds the limit of 10.*")
            },
            "L3",
          ),
          (
            log => {
              log.mdc.get("participant") shouldBe Some("participant2")
              log.warningMessage should include regex ("LOCAL_VERDICT_MALFORMED_PAYLOAD.*The nesting of the protobuf message exceeds the limit of 10.*")
            },
            "L4",
          ),
          // The mediator receives a deeply nested informee message and discards
          (
            log => {
              log.mdc.get("mediator") shouldBe Some("mediator1")
              log.warningMessage should include regex ("Received an envelope at.*that cannot be opened. Discarding envelope.*Reason: NestingTooDeep\\(10\\)")
            },
            "L5",
          ),
          // It also discards the confirmation messages from mallory/p1 and alice/p2
          // bob/p3 does not send a response because he observes a well formed request and is not a confirmer
          (
            log => {
              log.mdc.get("mediator") shouldBe Some("mediator1")
              log.warningMessage should include regex ("Received a confirmation response at.*by PAR::participant1::.*with an unknown request id RequestId.*Discarding response")
            },
            "L6",
          ),
          (
            log => {
              log.mdc.get("mediator") shouldBe Some("mediator1")
              log.warningMessage should include regex ("Received a confirmation response at.*by PAR::participant2::.*with an unknown request id RequestId.*Discarding response")
            },
            "L7",
          ),
        ),
        // These are additionally created for PV=dev
        mayContain = Seq(
          log => {
            log.loggerName should include(
              "com.digitalasset.canton.participant.protocol.TransactionProcessor:TransactionTreeDepthIntegrationTest/participant=participant1/psid=synchronizer1"
            )
            log.warningMessage should include regex ("InvalidSubviewReferenceError.*is invalid because a subview is invalid")
          },
          log => {
            log.mdc.get("participant") shouldBe Some("participant1")
            log.warningMessage should include regex ("Received a request with id RequestId.*where the view at List\\(R\\) is missing")
          },
        ),
      ),
    )

  }

}
