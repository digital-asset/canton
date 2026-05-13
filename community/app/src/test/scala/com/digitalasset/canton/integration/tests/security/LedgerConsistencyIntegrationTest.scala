// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.data.{EitherT, OptionT}
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi.data
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{CryptoPureApi, Salt, SaltSeed}
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.MediatorError.{DuplicateConfirmationRequest, InvalidMessage}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.MultiSynchronizerFeatureFlag
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceAlarm,
  SyncServiceSynchronizerDisconnect,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.{
  CreatesExistingContracts,
  ModelConformance,
}
import com.digitalasset.canton.sequencing.client.SendResult
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{
  LedgerCommandId,
  LedgerUserId,
  LfPartyId,
  ReassignmentCounter,
  config,
}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.CreationTime
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** Tests various flavors of a participant committing inconsistent transactions:
  *   - Transactions that are not consistent with the ledger state. (E.g. a contract is archived,
  *     the transaction archives it again.)
  *   - Inconsistent reassignments (E.g. archive a contract that has been unassigned or vice versa.)
  *   - Transaction that are internally inconsistent. (E.g. the same transaction archives a contract
  *     twice.)
  *   - Transactions that become inconsistent with the ledger state due to partial rollbacks.
  *     (Namely, the creation of a transient contract is rolled back whereas the archival is
  *     committed.)
  *   - TBD: inconsistent use of contract keys
  *
  * Currently, the test merely checks that the affected participant does not crash right away. In
  * the future, it will also check:
  *   - Correct states in the Canton ACS and ledger ACS.
  *   - Well-defined behavior of ledger API
  *   - Well-defined behavior of ACS commitment processor
  *   - The above holds even after restarts.
  *
  * TODO(i12904): Update description
  */
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

  private var maliciousP1: MaliciousParticipantNode = _

  private var defaultPayer: PartyId = _
  private var defaultOwner: PartyId = _

  private var dummyCmd: Seq[data.Command] = _

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

        // Increase the reconciliation interval to a large value to effectively disable the AcsCommitmentProcessor.
        // Otherwise, it could complain about mismatches, as some tests fork the ledger.
        // TODO(i31578): remove this
        sequencer1.topology.synchronizer_parameters
          .propose_update(
            daId,
            _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(100000)),
          )

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
          resolveContractOverride = cid => OptionT.fromOption(p1ExtraContracts.get(cid)),
        )

        defaultPayer = participant1.adminParty
        defaultOwner = participant1.adminParty

        {
          val emptyJavaList = new util.ArrayList[String]()
          val dummyContract = JavaDecodeUtil
            .decodeAllCreated(UniversalContract.COMPANION)(
              participant1.ledger_api.javaapi.commands.submit(
                Seq(defaultPayer),
                new UniversalContract(
                  emptyJavaList,
                  List(defaultPayer.toProtoPrimitive).asJava,
                  emptyJavaList,
                  List(defaultPayer.toProtoPrimitive).asJava,
                ).create().commands().asScala.toSeq,
              )
            )
            .loneElement

          dummyCmd = dummyContract.id.exerciseTouch(emptyJavaList).commands.asScala.toSeq
        }
      }

  private lazy val unexpectedMediatorApproval: (LogEntry => Assertion, String) = (
    _.shouldBeCantonError(
      SyncServiceAlarm,
      _ shouldBe "Mediator approved a request that has been locally rejected.",
    ),
    "Unexpected mediator approval",
  )
  private lazy val indexerWarnings: (LogEntry => Assertion, String) = (
    _.loggerName should include("ParallelIndexerSubscription"),
    "Indexer warnings",
  )
  private lazy val duplicateCreate: (LogEntry => Assertion, String) = (
    _.shouldBeCantonError(
      CreatesExistingContracts,
      _ shouldBe "Rejected transaction would create contract(s) that already exist ",
    ),
    "Creation of existing contract",
  )
  private lazy val viewReconstructionFailure: (LogEntry => Assertion, String) = (
    _.shouldBeCantonError(
      ModelConformance,
      _ should include("Reconstructed view differs from received view."),
    ),
    "view reconstruction error",
  )

  "A participant" when {
    "a contract is non-existent" can {
      "archive the contract" in { implicit env =>
        import env.*

        val (_, iouInstance) = mkCreateData()

        val (_, events) = withP1KnowingContracts(iouInstance) {
          runMaliciously()(
            archiveMaliciously(iouInstance.contractId),
            LogEntry.assertLogSeq(
              Seq(unexpectedMediatorApproval, indexerWarnings)
            ),
          )
        }

        events.assertStatusOk(participant1)
      }

      "unassign the contract" in { implicit env =>
        val (_, iouInstance) = mkCreateData()

        runMaliciously()(
          unassignMaliciously(iouInstance),
          LogEntry.assertLogSeq(
            Seq(unexpectedMediatorApproval, indexerWarnings)
          ),
        )
      }

      "not create a contract twice within the same transaction" in { implicit env =>
        import env.*

        // Series of failed attempts to commit a transaction creating the same contract twice.

        val (singleCreateTree, _) = mkCreateData()

        // Duplicate root view in the transaction tree. The mediator will reject this due to non-unique view hashes.
        val doubleCreateTree = GenTransactionTree.rootViewsUnsafe
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .modify(seq => seq ++ seq)(singleCreateTree)

        val (_, events2) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          trackingLedgerEvents(Seq(participant1), Seq.empty)(
            maliciousP1.submitTransactionTree(doubleCreateTree)
          ),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include regex raw"Unable to create transaction tree: " +
                  raw"A transaction tree must contain a hash at most once. Found the hash SHA-256:\S+ twice\.",
                "Non-unique root hash",
              )
            ),
            Seq(_.shouldBeCantonErrorCode(InvalidMessage)),
          ),
        )
        events2.assertNoTransactions()

        // Modify the salt of ViewParticipantData.
        // This would recover the uniqueness of view hashes, but it will fail the model conformance check.
        val createTreeModifiedSalt = GenTransactionTree.rootViewsUnsafe
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(GenLens[ViewParticipantData](_.salt))
          .replace(Salt.tryDeriveSalt(SaltSeed.generate()(pureCrypto), 0, pureCrypto))(
            singleCreateTree
          )

        val (_, events3) = runMaliciously()(
          maliciousP1.submitTransactionTree(createTreeModifiedSalt),
          LogEntry.assertLogSeq(
            Seq(viewReconstructionFailure, unexpectedMediatorApproval)
          ),
        )
        events3.assertNoTransactions()
      }

      "archive a transient contract before its creation" in { implicit env =>
        import env.*

        // Submit the command sequence dummy; create; archive
        // but use an interceptor to transform this into archive; create; dummy
        // The dummy command is needed so that the transformation does not change the position of create in the tree,
        // because that would change contract ids as well.

        val createAndArchive = IouSyntax
          .testIou(defaultPayer, defaultOwner)
          .createAnd()
          .exerciseArchive()
          .commands()
          .asScala

        val rawCmds = (dummyCmd ++ createAndArchive)
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val createCmd =
          CommandsWithMetadata(rawCmds, Seq(defaultPayer), ledgerTime = environment.now.toLf)

        runMaliciously()(
          maliciousP1
            .submitCommand(
              createCmd,
              transactionTreeInterceptor = GenTransactionTree.rootViewsUnsafe
                .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
                .modify(_.reverse)(_),
            ),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  ModelConformance,
                  _ should fullyMatch regex raw"Rejected transaction due to a failed model conformance check: Contract id \S+ created in node NodeId\(1\) is referenced before in NodeId\(0\)",
                ),
                "model conformance check",
              ),
              unexpectedMediatorApproval,
            )
          ),
        )
      }

      "rollback creation and commit archival of a transient contract" in { implicit env =>
        import env.*

        val createAndArchive = IouSyntax
          .testIou(defaultPayer, defaultOwner)
          .createAnd()
          .exerciseArchive()
          .commands()
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val createCmd =
          CommandsWithMetadata(
            createAndArchive,
            Seq(defaultPayer),
            ledgerTime = environment.now.toLf,
          )

        runMaliciously()(
          maliciousP1
            .submitCommand(
              createCmd,
              // Replace the ViewParticipantData salt of view 0, the view that creates the contract
              // in order to cause a model conformance error.
              transactionTreeInterceptor = GenTransactionTree.rootViewsUnsafe
                .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
                .index(0)
                .andThen(MerkleTree.Optics.unblinded[TransactionView])
                .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
                .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
                .andThen(GenLens[ViewParticipantData](_.salt))
                .replace(Salt.tryDeriveSalt(SaltSeed.generate()(pureCrypto), 0, pureCrypto)),
            ),
          LogEntry.assertLogSeq(
            Seq(viewReconstructionFailure, unexpectedMediatorApproval, indexerWarnings)
          ),
        )
      }
    }

    "a contract is archived" can {
      "create the contract" in { implicit env =>
        import env.*

        val (transactionTree, iouInstance) = assignNonExistentContract()

        archive(iouInstance.contractId)

        val (_, createEvents) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree),
          LogEntry.assertLogSeq(
            Seq(duplicateCreate, unexpectedMediatorApproval)
          ),
        )
        createEvents.assertStatusOk(participant1)
      }

      "archive the contract again" in { implicit env =>
        import env.*

        // Participant1 submits the duplicate archival and must fail gracefully, because commitAfterFailedActivenessCheck == true.
        val payer = participant1.adminParty

        // Participant2 must crash, because commitAfterFailedActivenessCheck == false.
        val owner = participant2.adminParty

        val iouInstance = create(payer, owner)
        archive(iouInstance.contractId, payer)

        // The second archival succeeds only because
        // - we are replacing the local verdict of participant1 by "approve"
        //   (participant2 does not need to approve, as it merely hosts an observer.)
        // - we have configured commitAfterFailedActivenessCheck
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            replacingConfirmationResponses(
              participant1,
              sequencer1,
              daId,
              withLocalVerdict(localApprove),
            ) {
              val (_, events2) = trackingLedgerEvents(Seq(participant1), Seq.empty) {
                archiveMaliciously(iouInstance.contractId, payer).futureValueUS
                  .valueOrFail("Submission failed")
              }
              events2.assertStatusOk(participant1)
            }

            // The ping serves several purposes:
            // - verify that participant2 is broken
            // - force disconnect from da
            participant2.health.maybe_ping(participant2, 2.seconds) shouldBe empty
            participant2.synchronizers.list_connected() shouldBe empty
          },
          LogEntry.assertLogSeq(
            Seq(
              unexpectedMediatorApproval,
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
                    "Synchronizer 'synchronizer1' fatally disconnected"
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
                _.loggerName should include("participant=participant2"),
                _.shouldBeCantonErrorCode(AcsCommitmentProcessor.Errors.InternalError),
              ),
          ),
        )
      }

      "assign the contract" in { implicit env =>
        import env.*

        val iouInstance = create()
        archive(iouInstance.contractId)

        // Assign the contract
        val (_, assignEvents) = runMaliciously()(
          assignMaliciously(iouInstance),
          LogEntry.assertLogSeq(
            mustContainWithClue = Seq.empty,
            mayContain = Seq(
              _.shouldBeCantonErrorCode(AcsCommitmentProcessor.Errors.InternalError)
            ),
          ),
        )

        assignEvents.assertStatusOk(participant1)
      }

      "unassign the contract" in { implicit env =>
        import env.*

        val iouInstance = create()
        archive(iouInstance.contractId)

        runMaliciously()(
          unassignMaliciously(iouInstance),
          LogEntry.assertLogSeq(
            Seq(unexpectedMediatorApproval)
          ),
        )

        assertPingSucceeds(participant1, participant1, synchronizerId = Some(daId))
      }
    }

    "a contract has been unassigned" can {
      "create the contract" in { implicit env =>
        import env.*

        val (transactionTree, _, _) = unassignNonExistentContract()

        // Create contract
        val (_, createEvents) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree),
          LogEntry.assertLogSeq(
            Seq(duplicateCreate, unexpectedMediatorApproval)
          ),
        )
        createEvents.assertStatusOk(participant1)
      }

      "archive the contract" in { implicit env =>
        import env.*

        val (_, iou, _) = unassignNonExistentContract()

        val (_, events) = runMaliciously()(
          archiveMaliciously(iou.contractId),
          LogEntry.assertLogSeq(
            Seq(unexpectedMediatorApproval)
          ),
        )

        events.assertStatusOk(participant1)
      }

      "unassign the contract again" in { implicit env =>
        val (_, iou, counter) = unassignNonExistentContract()

        // Unassign the contract again
        runMaliciously()(
          unassignMaliciously(
            iou,
            reassignmentCounter = ReassignmentCounter(counter + 1),
          ),
          LogEntry.assertLogSeq(
            Seq(unexpectedMediatorApproval)
          ),
        )
      }
    }

    "a contract has been created" can {
      "not create the contract again" in { implicit env =>
        import env.*

        val (transactionTree, _) = mkCreateData()

        // Create the iou for the first time
        val (_, events1) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree)
        )
        events1.assertStatusOk(participant1)

        // Create the iou for the second time
        runMaliciously(suppressionRule = LevelAndAbove(Level.INFO))(
          maliciousP1.submitTransactionTree(transactionTree),
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

      "assign the contract" in { implicit env =>
        import env.*

        val iouInstance = create()

        val (_, assignEvents) = runMaliciously()(
          assignMaliciously(iouInstance)
        )
        assignEvents.assertStatusOk(participant1)
      }

      "archive the contract twice within the same transaction" in { implicit env =>
        import env.*

        val iouInstance = create()

        val (_, events) = runMaliciously()(
          maliciousP1.submitCommand(
            archiveCmdsWithMetadata(iouInstance.contractId),
            transactionInterceptor = (tx, metadata) => {
              val (nodeId, node) = tx.nodes.loneElement
              val nextNodeId = LfNodeId(nodeId.index + 1)
              val newTx = LfSubmittedTransaction(
                LfVersionedTransaction(
                  tx.version,
                  Map(nodeId -> node, nextNodeId -> node),
                  ImmArray(nodeId, nextNodeId),
                )
              )
              val newMetadata = metadata
                .focus(_.nodeSeeds)
                .modify(_.slowAppend(ImmArray(nextNodeId -> Hash.fromString("deadbeef" * 8).value)))
              newTx -> newMetadata
            },
          ),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  ModelConformance,
                  _ should fullyMatch regex raw"Rejected transaction due to a failed model conformance check: ErrorWithInternalConsistencyCheck\(UsedAfterArchive\(.*\)\)",
                ),
                "internal consistency failure",
              ),
              unexpectedMediatorApproval,
            )
          ),
        )
        events.assertStatusOk(participant1)
      }
    }

    "a contract has been assigned" can {
      "create the contract" in { implicit env =>
        import env.*

        val (transactionTree, _) = assignNonExistentContract()

        // Now run the real creation
        val (_, createEvents) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree),
          LogEntry.assertLogSeq(
            Seq(duplicateCreate, unexpectedMediatorApproval)
          ),
        )
        createEvents.assertStatusOk(participant1)
      }

      "assign the contract again" in { implicit env =>
        import env.*

        val (_, iou) = assignNonExistentContract()

        // Assign the contract again
        val (_, assignEvents) = runMaliciously()(
          assignMaliciously(iou, reassignmentCounter = ReassignmentCounter(2))
        )
        assignEvents.assertStatusOk(participant1)
      }
    }
  }

  private def create(payer: PartyId = defaultPayer, owner: PartyId = defaultOwner)(implicit
      env: TestConsoleEnvironment
  ): ContractInstance = {
    import env.*
    val iou = IouSyntax.createIou(participant1)(payer, owner)
    participant1.testing
      .acs_search(daName, exactId = iou.id.contractId, limit = PositiveInt.one)
      .loneElement
  }

  private def mkCreateData(
      payer: PartyId = defaultPayer,
      owner: PartyId = defaultOwner,
  )(implicit
      env: TestConsoleEnvironment
  ): (GenTransactionTree, ContractInstance) = {
    import env.*

    val createCmdRaw = IouSyntax
      .testIou(payer, owner)
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

    val newContractInstance: NewContractInstance =
      tree.rootViews.toSeq.loneElement.tryUnwrap.viewParticipantData.tryUnwrap.createdCore.loneElement.contract
    val contractInstance = newContractInstance.traverseCreatedAt {
      case t: CreationTime.CreatedAt => Right(t)
      case t: CreationTime => Left(s"Unsupported created at time: $t")
    }.value

    (tree, contractInstance)
  }

  private def archive(
      cid: LfContractId,
      payer: PartyId = defaultPayer,
  )(implicit env: TestConsoleEnvironment): data.Transaction = {
    import env.*
    participant1.ledger_api.javaapi.commands.submit(Seq(payer), archiveCommands(cid))
  }

  private def archiveMaliciously(cid: LfContractId, payer: PartyId = defaultPayer)(implicit
      env: TestConsoleEnvironment
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] =
    maliciousP1.submitCommand(archiveCmdsWithMetadata(cid, payer))

  private def archiveCmdsWithMetadata(cid: LfContractId, payer: PartyId = defaultPayer)(implicit
      env: TestConsoleEnvironment
  ): CommandsWithMetadata = {
    import env.*
    CommandsWithMetadata(
      archiveCommands(cid).map(c => Command.fromJavaProto(c.toProtoCommand)),
      Seq(payer),
      ledgerTime = environment.now.toLf,
    )
  }

  private def archiveCommands(cid: LfContractId): Seq[data.Command] =
    new Iou.ContractId(cid.coid)
      .exerciseArchive()
      .commands()
      .asScala
      .toSeq

  private def assignNonExistentContract(
      payer: PartyId = defaultPayer,
      owner: PartyId = defaultOwner,
  )(implicit
      env: TestConsoleEnvironment
  ): (GenTransactionTree, ContractInstance) = {
    import env.*

    // Create an iou
    val (transactionTree, iouInstance) = mkCreateData(payer, owner)
    participant1.testing.acs_search(daName, iouInstance.contractId.coid) shouldBe empty

    // Assign
    val (_, assignEvents) = runMaliciously()(
      assignMaliciously(iouInstance, payer)
    )
    assignEvents.assertStatusOk(participant1)

    participant1.testing.acs_search(daName, iouInstance.contractId.coid) should have size 1

    (transactionTree, iouInstance)
  }

  private def assignMaliciously(
      iou: ContractInstance,
      payer: PartyId = defaultPayer,
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter(1),
  )(implicit
      env: TestConsoleEnvironment
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    import env.*

    val data = UnassignmentData(
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
        reassignmentCounter,
      ),
      Set(participant1.id),
      Source(acmeId),
      Target(daId),
      Target(participant1.testing.fetch_synchronizer_time(daId)),
      participant1.testing.fetch_synchronizer_time(acmeId),
    )

    maliciousP1.submitAssignmentRequest(payer.toLf, data)
  }

  private def unassignNonExistentContract(
      payer: PartyId = defaultPayer,
      owner: PartyId = defaultOwner,
  )(implicit
      env: TestConsoleEnvironment
  ): (GenTransactionTree, ContractInstance, Long) = {
    import env.*
    val (transactionTree, iou) = assignNonExistentContract(payer, owner)

    val event = participant1.ledger_api.commands.submit_unassign(
      defaultPayer,
      Seq(iou.contractId),
      daId,
      acmeId,
    )

    val counter = event.events.loneElement.reassignmentCounter

    (transactionTree, iou, counter)
  }

  private def unassignMaliciously(
      iouInstance: ContractInstance,
      payer: PartyId = defaultPayer,
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter(1),
  )(implicit
      env: TestConsoleEnvironment
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    import env.*

    val helpers = ReassignmentDataHelpers(
      contract = iouInstance,
      sourceSynchronizer = Source(daId),
      targetSynchronizer = Target(acmeId),
      pureCrypto = pureCrypto,
      targetTimestamp = Target(participant1.testing.fetch_synchronizer_time(acmeId)),
    )
    val tree = helpers
      .fullUnassignmentTree(
        payer.toLf,
        participant1,
        MediatorGroupRecipient(NonNegativeInt.zero),
      )(reassignmentCounter = reassignmentCounter)

    maliciousP1.submitUnassignmentRequest(tree)
  }

  private def runMaliciously[A](
      confirmingParties: Set[LfPartyId] = Set(defaultPayer.toLf),
      suppressionRule: SuppressionRule = LevelAndAbove(Level.WARN),
  )(
      body: => EitherT[FutureUnlessShutdown, ?, A],
      logAssertion: Seq[LogEntry] => Assertion = LogEntry.assertLogSeq(Seq.empty),
  )(implicit
      env: TestConsoleEnvironment
  ): (A, TrackingResult) = {
    import env.*

    loggerFactory
      .assertLogsSeq(suppressionRule)(
        replacingConfirmationResponses(
          participant1,
          sequencer1,
          daId,
          withLocalVerdict(localApprove, _ => confirmingParties),
        ) {
          trackingLedgerEvents(Seq(participant1), Seq.empty) {
            body.futureValueUS.valueOrFail("Submission failed")
          }
        },
        logAssertion,
      )
  }

  /** Run a piece of code whilst injecting extra contracts into maliciousP1's phase 1 processing.
    * That allows us to run Phase 1 for the archival of a non-existent contract.
    */
  private def withP1KnowingContracts[A](contracts: GenContractInstance*)(body: => A): A = {
    withClue("Nesting of withExtraContracts is not supported") {
      p1ExtraContracts shouldBe empty
    }
    contracts.foreach(c => p1ExtraContracts.put(c.contractId, c))
    try body
    finally p1ExtraContracts.clear()
  }
  private lazy val p1ExtraContracts: TrieMap[LfContractId, GenContractInstance] = TrieMap.empty
}

// Need to test all storage backends to cover all relevant code paths.
final class LedgerConsistencyIntegrationTestPostgres extends LedgerConsistencyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

// Need to test all storage backends to cover all relevant code paths.
final class LedgerConsistencyIntegrationTestH2 extends LedgerConsistencyIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

// Need to test all storage backends to cover all relevant code paths.
final class LedgerConsistencyIntegrationTestInMemory extends LedgerConsistencyIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
