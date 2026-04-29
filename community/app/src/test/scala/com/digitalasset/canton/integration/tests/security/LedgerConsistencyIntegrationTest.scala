// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.data.OptionT
import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.MediatorError.DuplicateConfirmationRequest
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.integration.util.MultiSynchronizerFeatureFlag
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceAlarm,
  SyncServiceSynchronizerDisconnect,
}
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.CreatesExistingContracts
import com.digitalasset.canton.protocol.messages.LocalVerdict
import com.digitalasset.canton.protocol.{
  ContractInstance,
  GenContractInstance,
  LfContractId,
  NewContractInstance,
}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{LedgerCommandId, LedgerUserId, LfPartyId, ReassignmentCounter}
import com.digitalasset.daml.lf.transaction.CreationTime
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
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
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
        // participant1 does support commit after failed activeness check
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.parameters.commitAfterFailedActivenessCheck).replace(true)
        ),
        // participant2 is supposed to crash on commit after failed activeness check
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.commitAfterFailedActivenessCheck).replace(false)
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.synchronizers.connect_local(sequencer1, alias = daName)
        participants.local.dars.upload(CantonTestsPath)

        // Enable acme and multi-sync for participant1
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant1.dars.upload(CantonTestsPath, synchronizerId = Some(acmeId))
        MultiSynchronizerFeatureFlag.enable(Seq(participant1), daId)
        MultiSynchronizerFeatureFlag.enable(Seq(participant1), acmeId)

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
          resolveContractOverride = cid => OptionT.fromOption(p1ExtraContracts.get(cid)),
        )
      }

  "A participant" when {
    "a contract is non-existent" can {

      var payer: PartyId = null
      var owner: PartyId = null

      var createdContract: ContractInstance = null

      def setupNonExistent()(implicit env: TestConsoleEnvironment): Unit =
        if (createdContract == null) {
          import env.*

          payer = participant1.adminParty
          owner = participant1.adminParty

          val transactionTree = createIouTransactionTree(payer, owner)
          createdContract = createdContractInstance(transactionTree)
        }

      "archive the contract" in { implicit env =>
        import env.*

        setupNonExistent()

        val (_, events) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          withExtraContracts(createdContract) {
            replacingLocalVerdict(localApprove) {
              trackingLedgerEvents(Seq(participant1), Seq.empty) {
                maliciousP1
                  .submitCommand(mkArchiveCmd(createdContract.contractId.coid, payer))
                  .futureValueUS
                  .valueOrFail("Submission failed")
              }
            }
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
                _.loggerName should include("ParallelIndexerSubscription"),
                "Indexer warnings",
              ),
            )
          ),
        )

        events.assertStatusOk(participant1)
      }

      "unassign the contract" in { implicit env =>
        import env.*

        setupNonExistent()

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          replacingLocalVerdict(localApprove) {
            trackingLedgerEvents(Seq(participant1), Seq.empty) {
              maliciousP1
                .submitUnassignmentRequest(mkUnassignmentTree(createdContract, payer))
                .futureValueUS
            }
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
                _.loggerName should include("ParallelIndexerSubscription"),
                "Indexer warnings",
              ),
            )
          ),
        )
      }
    }

    "a contract is archived" can {
      "create the contract" in { _ =>
        // TODO(i31579): fill-in
      }

      "archive the contract again" in { implicit env =>
        import env.*

        // Participant1 submits the duplicate archival and must fail gracefully, because commitAfterFailedActivenessCheck == true.
        val payer = participant1.adminParty

        // Participant2 must crash, because commitAfterFailedActivenessCheck == false.
        val owner = participant2.adminParty

        // Create an iou
        val iou = IouSyntax.createIou(participant1)(payer, owner)

        // The first attempt succeeds, as iou has been active.
        val (_, events1) = trackingLedgerEvents(Seq(participant1), Seq.empty) {
          maliciousP1
            .submitCommand(mkArchiveCmd(iou.id.contractId, payer))
            .futureValueUS
            .valueOrFail("Submission failed")
        }
        events1.assertStatusOk(participant1)

        // The second attempt succeeds only because
        // - we are replacing the local verdict of participant1 by "approve"
        //   (participant2 does not need to approve, as it merely hosts an observer.)
        // - we have configured commitAfterFailedActivenessCheck
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          replacingLocalVerdict(localApprove) {
            val (_, events2) = trackingLedgerEvents(Seq(participant1), Seq.empty) {
              maliciousP1
                .submitCommand(mkArchiveCmd(iou.id.contractId, payer))
                .futureValueUS
                .valueOrFail("Submission failed")
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
            mayContain =
              // Tolerate arbitrary further messages from participant2.
              Seq(
                _.loggerName should include("participant=participant2")
              ),
          ),
        )
      }

      "assign the contract" in { _ =>
        // TODO(i31579): fill-in
      }

      "unassign the contract" in { implicit env =>
        import env.*

        val payer = participant1.adminParty
        val owner = participant1.adminParty

        // Create an iou
        val iou = IouSyntax.createIou(participant1)(payer, owner)

        val iouInstance = participant1.testing
          .acs_search(daName, exactId = iou.id.contractId, limit = PositiveInt.one)
          .loneElement

        // Archive the iou
        IouSyntax.archive(participant1)(iou, payer)

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          replacingLocalVerdict(localApprove) {
            trackingLedgerEvents(Seq(participant1), Seq.empty) {
              maliciousP1
                .submitUnassignmentRequest(mkUnassignmentTree(iouInstance, payer))
                .futureValueUS
            }
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  SyncServiceAlarm,
                  _ shouldBe "Mediator approved a request that has been locally rejected.",
                ),
                "Unexpected mediator approval",
              )
            )
          ),
        )

        assertPingSucceeds(participant1, participant1, synchronizerId = Some(daId))
      }
    }

    "a contract has been unassigned" can {
      "create the contract" in { _ =>
        // TODO(i31579): fill-in
      }

      "archive the contract" in { _ =>
        // TODO(i31579): fill-in
      }

      "unassign the contract again" in { _ =>
        // TODO(i31579): fill-in
      }
    }

    "a contract has been created" can {
      "not create the contract again" in { implicit env =>
        import env.*

        val payer = participant1.adminParty
        val owner = participant1.adminParty

        val transactionTree = createIouTransactionTree(payer, owner)

        val dummyCmd = CommandsWithMetadata(Seq.empty, Seq(payer))

        // Create the iou for the first time
        val (_, events1) =
          trackingLedgerEvents(Seq(participant1), Seq.empty) {
            maliciousP1
              .submitCommand(dummyCmd, transactionTreeInterceptor = _ => transactionTree)
              .futureValueUS
          }
        events1.assertStatusOk(participant1)

        // Create the iou for the second time
        loggerFactory.assertLogsSeq(LevelAndAbove(Level.INFO))(
          replacingLocalVerdict(localApprove, _ => Set(payer.toLf)) {
            trackingLedgerEvents(Seq(participant1), Seq.empty) {
              maliciousP1
                .submitCommand(dummyCmd, transactionTreeInterceptor = _ => transactionTree)
                .futureValueUS
            }
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  DuplicateConfirmationRequest,
                  _ should fullyMatch regex raw"The request UUID \(\S+\) is a duplicate of a previous request with an identical UUID\. It cannot be re-used until \S+\.",
                ),
                "Mediator deduplication",
              )
            ),
            mayContain = Seq(_ => succeed),
          ),
        )
      }

      "assign the contract" in { _ =>
        // TODO(i31579): fill-in
      }
    }

    "a contract has been assigned" can {
      "create the contract" in { implicit env =>
        import env.*

        val payer = participant1.adminParty
        val owner = participant1.adminParty

        // Create an iou
        val transactionTree = createIouTransactionTree(payer, owner)
        val iou = createdContractInstance(transactionTree)

        // Assign
        val unassignmentData = UnassignmentData(
          ReassignmentSubmitterMetadata(
            payer.toLf,
            participant1.id,
            LedgerCommandId.assertFromString("kartoffelsuppe"),
            None,
            LedgerUserId.assertFromString("dummyUserId"),
            None,
          ),
          ContractsReassignmentBatch(
            iou,
            Source(Iou.PACKAGE_ID),
            Target(Iou.PACKAGE_ID),
            ReassignmentCounter(1),
          ),
          Set(participant1.id),
          Source(acmeId),
          Target(daId),
          Target(participant1.testing.fetch_synchronizer_time(daId)),
          participant1.testing.fetch_synchronizer_time(acmeId),
        )

        participant1.testing.acs_search(daName, iou.contractId.coid) shouldBe empty

        val (_, assignEvents) = replacingLocalVerdict(localApprove) {
          trackingLedgerEvents(Seq(participant1), Seq.empty) {
            maliciousP1.submitAssignmentRequest(payer.toLf, unassignmentData).futureValueUS
          }
        }
        assignEvents.assertStatusOk(participant1)

        participant1.testing.acs_search(daName, iou.contractId.coid).loneElement

        // Now run the real creation

        val (_, createEvents) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          replacingLocalVerdict(localApprove, _ => Set(payer.toLf)) {
            trackingLedgerEvents(Seq(participant1), Seq.empty) {
              val dummyCmd = CommandsWithMetadata(Seq.empty, Seq(payer))
              maliciousP1
                .submitCommand(dummyCmd, transactionTreeInterceptor = _ => transactionTree)
                .futureValueUS
            }
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  CreatesExistingContracts,
                  _ shouldBe "Rejected transaction would create contract(s) that already exist ",
                ),
                "Creation of existing contract",
              ),
              (
                _.shouldBeCantonError(
                  SyncServiceAlarm,
                  _ shouldBe "Mediator approved a request that has been locally rejected.",
                ),
                "Unexpected mediator approval",
              ),
            )
          ),
        )
        createEvents.assertStatusOk(participant1)
      }

      "assign the contract again" in { _ =>
        // TODO(i31579): fill-in
      }
    }
  }

  /** Run a piece of code whilst injecting extra contracts into maliciousP1's phase 1 processing.
    * That allows us to run Phase 1 for the archival of a non-existent contract.
    */
  private def withExtraContracts[A](contracts: GenContractInstance*)(body: => A): A = {
    withClue("Nesting of withExtraContracts is not supported") {
      p1ExtraContracts shouldBe empty
    }
    contracts.foreach(c => p1ExtraContracts.put(c.contractId, c))
    try body
    finally p1ExtraContracts.clear()
  }
  private lazy val p1ExtraContracts: TrieMap[LfContractId, GenContractInstance] = TrieMap.empty

  private def createIouTransactionTree(payer: PartyId, owner: PartyId)(implicit
      env: TestConsoleEnvironment
  ): GenTransactionTree = {
    import env.*
    val createCmdRaw = testIou(payer, owner)
      .create()
      .commands()
      .asScala
      .toSeq
      .map(c => Command.fromJavaProto(c.toProtoCommand))
    val createCmd =
      CommandsWithMetadata(createCmdRaw, Seq(payer), ledgerTime = environment.now.toLf)

    // Run Phase 1, make it fail with an exception, use the exception to extract the transaction tree
    case class InterceptException(tree: GenTransactionTree) extends RuntimeException
    val InterceptException(tree) = maliciousP1
      .submitCommand(
        createCmd,
        transactionTreeInterceptor = tree => throw InterceptException(tree),
      )
      .value
      .failed
      .futureValueUS
      .asInstanceOf[InterceptException]

    tree
  }

  def createdContractInstance(tree: GenTransactionTree): ContractInstance = {
    val newContractInstance: NewContractInstance =
      tree.rootViews.toSeq.loneElement.tryUnwrap.viewParticipantData.tryUnwrap.createdCore.loneElement.contract
    newContractInstance.traverseCreatedAt {
      case t: CreationTime.CreatedAt => Right(t)
      case t: CreationTime => Left(s"Unsupported created at time: $t")
    }.value
  }

  private def replacingLocalVerdict[A](
      verdict: LocalVerdict,
      modifyConfirmingParties: Set[LfPartyId] => Set[LfPartyId] = identity,
  )(body: => A)(implicit env: TestConsoleEnvironment): A = {
    import env.*
    replacingConfirmationResponses(
      participant1,
      sequencer1,
      daId,
      withLocalVerdict(verdict, modifyConfirmingParties),
    )(body)
  }

  private def mkArchiveCmd(iouId: String, payer: PartyId)(implicit
      env: TestConsoleEnvironment
  ): CommandsWithMetadata = {
    import env.*

    val archiveCmds = new Iou.ContractId(iouId)
      .exerciseArchive()
      .commands()
      .asScala
      .toSeq
      .map(c => Command.fromJavaProto(c.toProtoCommand))

    CommandsWithMetadata(archiveCmds, Seq(payer), ledgerTime = environment.now.toLf)
  }

  def mkUnassignmentTree(iouInstance: ContractInstance, payer: PartyId)(implicit
      env: TestConsoleEnvironment
  ): FullUnassignmentTree = {
    import env.*

    val helpers = ReassignmentDataHelpers(
      contract = iouInstance,
      sourceSynchronizer = Source(daId),
      targetSynchronizer = Target(acmeId),
      pureCrypto = pureCrypto,
      targetTimestamp = Target(participant1.testing.fetch_synchronizer_time(acmeId)),
    )
    helpers
      .fullUnassignmentTree(
        payer.toLf,
        participant1,
        MediatorGroupRecipient(NonNegativeInt.zero),
      )()
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
