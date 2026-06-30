// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.data.EitherT
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.javaapi.data
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{CryptoPureApi, Salt, SaltSeed}
import com.digitalasset.canton.damltestslf23.java.da.types
import com.digitalasset.canton.damltestslf23.java.universal.UniversalContract
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.MediatorError.{DuplicateConfirmationRequest, InvalidMessage}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.MultiSynchronizerFeatureFlag
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
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
import com.digitalasset.daml.lf.transaction.{CreationTime, Transaction}
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

/** Tests various flavors of a participant committing inconsistent transactions:
  *   - Transactions that are not consistent with the ledger state. (E.g. a contract is archived,
  *     the transaction archives it again.)
  *   - Inconsistent reassignments (E.g. archive a contract that has been unassigned or vice versa.)
  *   - Transaction that are internally inconsistent. (E.g. the same transaction archives a contract
  *     twice.)
  *   - Transactions that become inconsistent with the ledger state due to partial rollbacks.
  *     (Namely, the creation of a transient contract is rolled back whereas the archival is
  *     committed.)
  *   - inconsistent use of contract keys
  *
  * Currently, the test checks the following:
  *   - The affected participant does not crash right away.
  *   - Well-defined behavior of ledger API
  *
  * In the future, the test will also check:
  *   - Correct states in the Canton ACS and ledger ACS.
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

  private var defaultMaintainer: PartyId = _
  private var defaultObserver: PartyId = _

  private var mgmtContract: UniversalContract.Contract = _

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

        val darPaths = Seq(CantonExamplesPath, CantonTestsLF23Path)

        // Connect all participants to da
        participants.local.synchronizers.connect_local(sequencer1, alias = daName)
        darPaths.foreach(participants.local.dars.upload(_))

        // Connect participant1 to acme
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        darPaths.foreach(participant1.dars.upload(_, synchronizerId = Some(acmeId)))

        // Also enable multi-sync
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
        )

        defaultMaintainer = participant1.adminParty
        defaultObserver = participant1.adminParty

        // Create the mgmt contract for calling contract key operations just once during setup
        mgmtContract = JavaDecodeUtil
          .decodeAllCreated(UniversalContract.COMPANION)(
            participant1.ledger_api.javaapi.commands.submit(
              Seq(defaultMaintainer),
              createCommands(keyText = "mgmt"),
            )
          )
          .loneElement
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
  private lazy val internalConsistencyFailure: (LogEntry => Assertion, String) = (
    _.shouldBeCantonError(
      ModelConformance,
      _ should fullyMatch regex raw"Rejected transaction due to a failed model conformance check: ErrorWithInternalConsistencyCheck\(.*\)",
    ),
    "internal consistency failure",
  )
  private lazy val failedActivenessCheckApproved: (LogEntry => Assertion, String) = (
    _.shouldBeCantonError(
      SyncServiceAlarm,
      _ should fullyMatch regex raw"Request RequestId\(\S+\) with failed activeness check is approved\.",
    ),
    "Failed activeness check",
  )

  "A participant" when {
    "a contract is non-existent" can {
      "archive the contract" in { implicit env =>
        import env.*

        val (_, instance) = mkCreateData()

        val (_, events) = runMaliciously()(
          archiveMaliciously(
            instance.contractId,
            disclosedContracts = Map(instance.contractId -> instance),
          ),
          LogEntry.assertLogSeq(
            Seq(failedActivenessCheckApproved, unexpectedMediatorApproval, indexerWarnings)
          ),
        )

        events.assertStatusOk(participant1)
        exercisedIdOf(events) shouldBe instance.contractId
        assertInactive(instance.contractId)
      }

      "unassign the contract" in { implicit env =>
        import env.*

        val (_, instance) = mkCreateData()

        val (_, events) = runMaliciously()(
          unassignMaliciously(instance),
          LogEntry.assertLogSeq(
            Seq(unexpectedMediatorApproval, indexerWarnings)
          ),
        )

        events.assertStatusOk(participant1)
        assertUnassigned(events, instance.contractId)
        assertInactive(instance.contractId)
      }

      "not create a contract twice within the same transaction" in { implicit env =>
        import env.*

        // Series of failed attempts to commit a transaction creating the same contract twice.

        val (singleCreateTree, instance) = mkCreateData()

        // Duplicate root view in the transaction tree. The mediator will reject this due to non-unique view hashes.
        val doubleCreateTree = GenTransactionTree.Optics.rootViewsUnsafe
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .modify(seq => seq ++ seq)(singleCreateTree)

        val (_, events1) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
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
        events1.assertNoTransactions()
        assertInactive(instance.contractId)

        // Modify the salt of ViewParticipantData.
        // This would recover the uniqueness of view hashes, but it will fail the model conformance check.
        val createTreeModifiedSalt = GenTransactionTree.Optics.rootViewsUnsafe
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(GenLens[ViewParticipantData](_.salt))
          .replace(Salt.tryDeriveSalt(SaltSeed.generate()(pureCrypto), 0, pureCrypto))(
            singleCreateTree
          )

        val (_, events2) = runMaliciously()(
          maliciousP1.submitTransactionTree(createTreeModifiedSalt),
          LogEntry.assertLogSeq(
            Seq(viewReconstructionFailure, unexpectedMediatorApproval)
          ),
        )
        events2.assertNoTransactions()
        assertInactive(instance.contractId)
      }

      "archive a transient contract before its creation" in { implicit env =>
        import env.*

        // Overall strategy: execute the command sequence archive; create; dummy by
        // submitting the command sequence dummy; create; archive and
        // using an interceptor to reverse the order of commands.
        // The dummy command is needed so that the transformation does not change the position of create in the tree,
        // because that would change contract ids as well.

        val dummyCmd =
          mgmtContract.id.exerciseTouch(new util.ArrayList[String]()).commands.asScala.toSeq
        val createAndArchive = mkUniversalContract()
          .createAnd()
          .exerciseArchive()
          .commands()
          .asScala

        val rawCmds = (dummyCmd ++ createAndArchive)
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val cmd =
          CommandsWithMetadata(rawCmds, Seq(defaultMaintainer), ledgerTime = environment.now.toLf)

        val (_, events) = runMaliciously()(
          maliciousP1
            .submitCommand(
              cmd,
              transactionTreeInterceptor = GenTransactionTree.Optics.rootViewsUnsafe
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

        events.assertStatusOk(participant1)
        val cid = inside(events.awaitTransactions(participant1).loneElement.events) {
          case Seq(ev1, ev2, ev3) =>
            val exercisedId = exercisedIdOfEvent(ev1)

            val createdCid = createdIdOfEvent(ev2)
            exercisedId shouldBe createdCid

            exercisedIdOfEvent(ev3, consuming = false).coid shouldBe mgmtContract.id.contractId

            exercisedId
        }
        assertInactive(cid) // FIXME(i29855): should be active, because the create comes last
      }

      "rollback creation and commit archival of a transient contract" in { implicit env =>
        import env.*

        val createAndArchive = mkUniversalContract()
          .createAnd()
          .exerciseArchive()
          .commands()
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand))

        val createAndArchiveCmd =
          CommandsWithMetadata(
            createAndArchive,
            Seq(defaultMaintainer),
            ledgerTime = environment.now.toLf,
          )

        val (_, events) = runMaliciously()(
          maliciousP1
            .submitCommand(
              createAndArchiveCmd,
              // Replace the ViewParticipantData salt of view 0, the view that creates the contract
              // in order to cause a model conformance error.
              transactionTreeInterceptor = GenTransactionTree.Optics.rootViewsUnsafe
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

        events.assertStatusOk(participant1)
        val cid = exercisedIdOf(events)
        assertInactive(cid)
      }

      "query the contract by key" in { implicit env =>
        import env.*

        // Create the data of a non-existent contract to be looked up
        val (_, nonExistent) = mkCreateData(keyText = "myKey")

        // Query the non-existent contract by key
        val (_, events) = runMaliciously()(
          maliciousP1
            .submitCommand(
              queryByKeyCmdsWithMetadata(
                "myKey",
                disclosedContracts = Map(nonExistent.contractId -> nonExistent),
              ),
              // Use interceptor to change the result of QueryByKey to yield the id of nonExistent
              transactionInterceptor = modifyQueryByKeyResult(_ => Vector(nonExistent.contractId)),
            ),
          LogEntry.assertLogSeq(Seq(failedActivenessCheckApproved, unexpectedMediatorApproval)),
        )

        events.assertStatusOk(participant1)
        exercisedIdOf(events, consuming = false).coid shouldBe mgmtContract.id.contractId
        assertInactive(nonExistent.contractId)
      }
    }

    "a contract is archived" can {
      "create the contract" in { implicit env =>
        import env.*

        val (transactionTree, instance) = assignNonExistentContract()

        archive(instance.contractId)

        val (_, events) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree),
          LogEntry.assertLogSeq(
            Seq(failedActivenessCheckApproved, duplicateCreate, unexpectedMediatorApproval)
          ),
        )

        events.assertStatusOk(participant1)
        val cid = createdIdOf(events)
        assertActive(cid)
      }

      "archive the contract again" in { implicit env =>
        import env.*

        // Participant1 submits the duplicate archival and must fail gracefully, because commitAfterFailedActivenessCheck == true.
        val maintainer = participant1.adminParty

        // Participant2 must crash, because commitAfterFailedActivenessCheck == false.
        val observer = participant2.adminParty

        val instance = create(maintainer, observer)
        archive(instance.contractId, maintainer)

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
                archiveMaliciously(instance.contractId, maintainer).futureValueUS
                  .valueOrFail("Submission failed")
              }

              events2.assertStatusOk(participant1)
              exercisedIdOf(events2) shouldBe instance.contractId
              assertInactive(instance.contractId)
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
              failedActivenessCheckApproved,
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

        val instance = create()
        archive(instance.contractId)

        // Assign the contract
        val (_, events) = runMaliciously()(
          assignMaliciously(instance),
          LogEntry.assertLogSeq(Seq(unexpectedMediatorApproval)),
        )

        events.assertStatusOk(participant1)
        assertAssigned(events, instance.contractId)
        assertActive(instance.contractId)
      }

      "unassign the contract" in { implicit env =>
        import env.*

        val instance = create()
        archive(instance.contractId)

        val (_, events) = runMaliciously()(
          unassignMaliciously(instance),
          LogEntry.assertLogSeq(
            Seq(unexpectedMediatorApproval)
          ),
        )

        events.assertStatusOk(participant1)
        assertUnassigned(events, instance.contractId)
        assertInactive(instance.contractId)
      }

      "query the contract by key" in { implicit env =>
        import env.*

        val instance = create(keyText = "myKey")
        archive(instance.contractId)

        val (_, events) =
          runMaliciously()(
            maliciousP1
              .submitCommand(
                queryByKeyCmdsWithMetadata("myKey"),
                // Use interceptor to change the result of QueryByKey to yield the id of instance
                transactionInterceptor = modifyQueryByKeyResult(_ => Vector(instance.contractId)),
              ),
            LogEntry.assertLogSeq(Seq(failedActivenessCheckApproved, unexpectedMediatorApproval)),
          )

        events.assertStatusOk(participant1)
        exercisedIdOf(events, consuming = false).coid shouldBe mgmtContract.id.contractId
        assertInactive(instance.contractId)
      }
    }

    "a contract has been unassigned" can {
      "create the contract" in { implicit env =>
        import env.*

        val (transactionTree, instance, _) = unassignNonExistentContract()

        // Create contract
        val (_, events) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree),
          LogEntry.assertLogSeq(
            Seq(failedActivenessCheckApproved, duplicateCreate, unexpectedMediatorApproval)
          ),
        )

        events.assertStatusOk(participant1)
        createdIdOf(events) shouldBe instance.contractId
        assertActive(instance.contractId)
      }

      "archive the contract" in { implicit env =>
        import env.*

        val (_, instance, _) = unassignNonExistentContract()

        val (_, events) = runMaliciously()(
          archiveMaliciously(instance.contractId),
          LogEntry.assertLogSeq(
            Seq(failedActivenessCheckApproved, unexpectedMediatorApproval)
          ),
        )

        events.assertStatusOk(participant1)
        exercisedIdOf(events) shouldBe instance.contractId
        assertInactive(instance.contractId)
      }

      "unassign the contract again" in { implicit env =>
        import env.*

        val (_, instance, counter) = unassignNonExistentContract()

        // Unassign the contract again
        val (_, events) = runMaliciously()(
          unassignMaliciously(
            instance,
            reassignmentCounter = ReassignmentCounter(counter + 1),
          ),
          LogEntry.assertLogSeq(
            Seq(unexpectedMediatorApproval)
          ),
        )

        events.assertStatusOk(participant1)
        assertUnassigned(events, instance.contractId)
        assertInactive(instance.contractId)
      }

      "query the contract by key" in { implicit env =>
        import env.*

        val (_, instance, _) = unassignNonExistentContract(keyText = "myKey")

        val (_, events) =
          runMaliciously()(
            maliciousP1
              .submitCommand(
                queryByKeyCmdsWithMetadata("myKey"),
                // Use interceptor to change the result of QueryByKey to yield the id of instance
                transactionInterceptor = modifyQueryByKeyResult(_ => Vector(instance.contractId)),
              ),
            LogEntry.assertLogSeq(Seq(failedActivenessCheckApproved, unexpectedMediatorApproval)),
          )

        events.assertStatusOk(participant1)
        exercisedIdOf(events, consuming = false).coid shouldBe mgmtContract.id.contractId
        assertInactive(instance.contractId)
      }
    }

    "a contract has been created" can {
      "not create the contract again" in { implicit env =>
        import env.*

        val (transactionTree, instance) = mkCreateData()

        // Create the contract for the first time
        val (_, events1) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree)
        )
        events1.assertStatusOk(participant1)

        // Create the contract for the second time
        val (_, events2) = runMaliciously(suppressionRule = LevelAndAbove(Level.INFO))(
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

        events2.assertNoTransactions()
        assertActive(instance.contractId)
      }

      "assign the contract" in { implicit env =>
        import env.*

        val instance = create()

        val (_, events) = runMaliciously()(
          assignMaliciously(instance),
          LogEntry.assertLogSeq(Seq(unexpectedMediatorApproval)),
        )

        events.assertStatusOk(participant1)
        assertAssigned(events, instance.contractId)
        assertActive(instance.contractId)
      }

      "archive the contract twice within the same transaction" in { implicit env =>
        import env.*

        val instance = create()

        val (_, events) = runMaliciously()(
          maliciousP1.submitCommand(
            archiveCmdsWithMetadata(instance.contractId),
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
          LogEntry.assertLogSeq(Seq(internalConsistencyFailure, unexpectedMediatorApproval)),
        )

        events.assertStatusOk(participant1)

        val txEvents = events.awaitTransactions(participant1).loneElement.events
        forEvery(txEvents)(exercisedIdOfEvent(_) shouldBe instance.contractId)
        txEvents should have size 2

        assertInactive(instance.contractId)
      }

      "archive and then query the contract by key" in { implicit env =>
        import env.*

        val instance = create(keyText = "myKey")

        val cmds = CommandsWithMetadata(
          (archiveCommands(instance.contractId) ++ queryByKeyCommands("myKey"))
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(defaultMaintainer),
          ledgerTime = environment.now.toLf,
        )

        val (_, events) =
          runMaliciously()(
            maliciousP1
              .submitCommand(
                cmds,
                // Use interceptor to change the result of QueryByKey to yield the id of instance
                transactionInterceptor = modifyQueryByKeyResult(_ => Vector(instance.contractId)),
              ),
            LogEntry.assertLogSeq(Seq(internalConsistencyFailure, unexpectedMediatorApproval)),
          )

        events.assertStatusOk(participant1)
        inside(events.awaitTransactions(participant1).loneElement.events) { case Seq(ev1, ev2) =>
          exercisedIdOfEvent(ev1) shouldBe instance.contractId
          exercisedIdOfEvent(ev2, consuming = false).coid shouldBe mgmtContract.id.contractId
        }
        assertInactive(instance.contractId)
      }

      "query by key even when it yields no contract" in { implicit env =>
        import env.*

        val instance = create(keyText = "myKey")

        val (_, events) =
          // Maintainers do not validate negative key queries. Therefore, the request gets approved despite the incorrect lookup.
          trackingLedgerEvents(Seq(participant1), Seq.empty)(
            maliciousP1
              .submitCommand(
                queryByKeyCmdsWithMetadata("myKey"),
                // Use interceptor to change the result of QueryByKey to yield empty resolution
                transactionInterceptor = modifyQueryByKeyResult(_ => Vector.empty),
              )
              .futureValueUS
              .valueOrFail("Submission failed")
          )

        events.assertStatusOk(participant1)
        exercisedIdOf(events, consuming = false).coid shouldBe mgmtContract.id.contractId
        assertActive(instance.contractId)
      }

      "rollback the request when query by key yields a contract with the wrong key" in {
        implicit env =>
          val instance = create(keyText = "myKey")

          val (_, events) =
            runMaliciously()(
              maliciousP1
                .submitCommand(
                  queryByKeyCmdsWithMetadata("myKey2"),
                  // Use interceptor to change the result of QueryByKey to yield instance
                  transactionInterceptor = modifyQueryByKeyResult(_ => Vector(instance.contractId)),
                ),
              LogEntry.assertLogSeq(
                Seq(
                  (
                    _.shouldBeCantonError(
                      ModelConformance,
                      _ should (
                        startWith regex raw"Rejected transaction due to a failed model conformance check:" and
                          include regex raw"SPEEDY CRASH \(\S+\): Contract key mismatch: the ledger returned a contract whose key does not match the requested key\."
                      ),
                    ),
                    "model conformance error",
                  ),
                  unexpectedMediatorApproval,
                )
              ),
            )

          events.assertNoTransactions()
          assertActive(instance.contractId)
      }

      "query contracts by key yielding contracts in the wrong order" in { implicit env =>
        import env.*

        val instance = create(keyText = "myKey")

        // First create a contract and then query it in the same transaction.
        val cmds = CommandsWithMetadata(
          (createCommands(keyText = "myKey") ++ queryByKeyCommands("myKey"))
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(defaultMaintainer),
          ledgerTime = environment.now.toLf,
        )

        val (_, events) = runMaliciously()(
          maliciousP1.submitCommand(
            cmds,
            // Reverse the key resolution, so that the local contract comes last.
            // This triggers an internal consistency failure, as local contracts must come first.
            transactionInterceptor = modifyQueryByKeyResult(_.reverse),
          ),
          LogEntry.assertLogSeq(Seq(internalConsistencyFailure, unexpectedMediatorApproval)),
        )

        events.assertStatusOk(participant1)
        val createdCid = inside(events.awaitTransactions(participant1).loneElement.events) {
          case Seq(ev1, ev2) =>
            val createdCid = createdIdOfEvent(ev1)
            // Recall that created refers to the contract created by cmds, whereas
            // instance.contractId refers to a contract created previously.
            createdCid should not be instance.contractId
            exercisedIdOfEvent(ev2, consuming = false).coid shouldBe mgmtContract.id.contractId
            createdCid
        }
        assertActive(createdCid)
      }

      "query contracts by key yielding inconsistent results" in { implicit env =>
        import env.*

        val instance1 = create(keyText = "myKey")
        val instance2 = create(keyText = "myKey")

        // Query a key twice
        val cmds = CommandsWithMetadata(
          (queryByKeyCommands("myKey") ++ queryByKeyCommands("myKey"))
            .map(c => Command.fromJavaProto(c.toProtoCommand)),
          Seq(defaultMaintainer),
          ledgerTime = environment.now.toLf,
        )

        val reverse = new AtomicBoolean(false)

        val (_, events) = runMaliciously()(
          maliciousP1.submitCommand(
            cmds,
            // Reverse the key resolution in some but not all QueryByKey nodes.
            // This will fail the internal consistency check, as all queries should yield the same result.
            transactionInterceptor =
              modifyQueryByKeyResult(cids => if (reverse.getAndSet(true)) cids.reverse else cids),
          ),
          LogEntry.assertLogSeq(Seq(internalConsistencyFailure, unexpectedMediatorApproval)),
        )

        events.assertStatusOk(participant1)

        val txEvents = events.awaitTransactions(participant1).loneElement.events
        forEvery(txEvents)(
          exercisedIdOfEvent(_, consuming = false).coid shouldBe mgmtContract.id.contractId
        )
        txEvents should have size 2

        assertActive(instance1.contractId)
        assertActive(instance2.contractId)
      }
    }

    "a contract has been assigned" can {
      "create the contract" in { implicit env =>
        import env.*

        val (transactionTree, instance) = assignNonExistentContract()

        // Now run the real creation
        val (_, events) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree),
          LogEntry.assertLogSeq(
            Seq(failedActivenessCheckApproved, duplicateCreate, unexpectedMediatorApproval)
          ),
        )

        events.assertStatusOk(participant1)
        createdIdOf(events) shouldBe instance.contractId
        assertActive(instance.contractId)
      }

      "assign the contract again" in { implicit env =>
        import env.*

        val (_, instance) = assignNonExistentContract()

        // Assign the contract again
        val (_, events) = runMaliciously()(
          assignMaliciously(instance, reassignmentCounter = ReassignmentCounter(2)),
          LogEntry.assertLogSeq(Seq(unexpectedMediatorApproval)),
        )

        events.assertStatusOk(participant1)
        assertAssigned(events, instance.contractId)
        assertActive(instance.contractId)
      }

      "archive the contract and create it again" in { implicit env =>
        import env.*

        // Overall strategy:
        // Firstly, assign the contract.
        // Then, execute the command sequence archive; create; dummy by
        // submitting the command sequence dummy; create; archive and
        // using an interceptor to reverse the order of commands.
        // The dummy command is needed so that the transformation does not change the position of create in the tree,
        // because that would change contract ids as well.

        val dummyCmd =
          mgmtContract.id.exerciseTouch(new util.ArrayList[String]()).commands.asScala.toSeq
        val createAndArchive = mkUniversalContract()
          .createAnd()
          .exerciseArchive()
          .commands()
          .asScala

        val (transactionTree, instance) =
          assignNonExistentContract(mkCreateCmds = _ => dummyCmd ++ createAndArchive)

        val revertedTransactionTree = GenTransactionTree.Optics.rootViewsUnsafe
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .modify(_.reverse)(transactionTree)

        val (_, events) = runMaliciously()(
          maliciousP1
            .submitTransactionTree(revertedTransactionTree),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonError(
                  ModelConformance,
                  _ should fullyMatch regex raw"Rejected transaction due to a failed model conformance check: Contract id \S+ created in node NodeId\(1\) is referenced before in NodeId\(0\)",
                ),
                "model conformance check",
              ),
              duplicateCreate,
              failedActivenessCheckApproved,
              unexpectedMediatorApproval,
            )
          ),
        )

        events.assertStatusOk(participant1)
        inside(events.awaitTransactions(participant1).loneElement.events) {
          case Seq(ev1, ev2, ev3) =>
            exercisedIdOfEvent(ev1) shouldBe instance.contractId
            createdIdOfEvent(ev2) shouldBe instance.contractId
            exercisedIdOfEvent(ev3, consuming = false).coid shouldBe mgmtContract.id.contractId
        }

        participant1.ledger_api.state.acs
          .active_contracts_of_party(defaultMaintainer)
          .map(_.createdEvent.value.contractId) should contain(instance.contractId.coid)
        // FIXME(i29855): the Canton ACS reorders archive; create to create; archive
        //  Instead, assertActive(instance.contractId) should succeed
        lookupActiveContractInstances(instance.contractId.coid) shouldBe empty
      }

      "create the contract again and archive it" in { implicit env =>
        import env.*

        // Overall strategy:
        // Firstly, assign the contract.
        // Then, submit the command sequence create; archive.

        val createAndArchive = mkUniversalContract()
          .createAnd()
          .exerciseArchive()
          .commands()
          .asScala
          .toSeq

        val (transactionTree, instance) =
          assignNonExistentContract(mkCreateCmds = _ => createAndArchive)

        val (_, events) = runMaliciously()(
          maliciousP1.submitTransactionTree(transactionTree),
          LogEntry.assertLogSeq(
            Seq(
              duplicateCreate,
              failedActivenessCheckApproved,
              unexpectedMediatorApproval,
            )
          ),
        )

        events.assertStatusOk(participant1)
        inside(events.awaitTransactions(participant1).loneElement.events) { case Seq(ev1, ev2) =>
          createdIdOfEvent(ev1) shouldBe instance.contractId
          exercisedIdOfEvent(ev2) shouldBe instance.contractId
        }
        // FIXME(i29855): The ledger acs currently ignores transient archivals
        //  Instead, assertInactive(instance.contractId) should succeed.
        participant1.ledger_api.state.acs
          .active_contracts_of_party(defaultMaintainer)
          .map(_.createdEvent.value.contractId) should contain(instance.contractId.coid)
        lookupActiveContractInstances(instance.contractId.coid) shouldBe empty
      }
    }
  }

  private def create(
      maintainer: PartyId = defaultMaintainer,
      observer: PartyId = defaultObserver,
      keyText: String = "dummyKey",
  )(implicit
      env: TestConsoleEnvironment
  ): ContractInstance = {
    import env.*
    val contract = JavaDecodeUtil
      .decodeAllCreated(UniversalContract.COMPANION)(
        participant1.ledger_api.javaapi.commands.submit(
          Seq(maintainer),
          createCommands(maintainer, observer, keyText),
        )
      )
      .loneElement

    lookupActiveContractInstances(contract.id.contractId).loneElement
  }

  private def createCommands(
      maintainer: PartyId = defaultMaintainer,
      observer: PartyId = defaultObserver,
      keyText: String,
  ): Seq[data.Command] =
    mkUniversalContract(maintainer, observer, keyText).create().commands().asScala.toSeq

  private def mkUniversalContract(
      maintainer: PartyId = defaultMaintainer,
      observer: PartyId = defaultObserver,
      keyText: String = "dummyKey",
  ): UniversalContract = new UniversalContract(
    List(maintainer.toProtoPrimitive).asJava,
    new util.ArrayList[String](),
    List(observer.toProtoPrimitive).asJava,
    List(maintainer.toProtoPrimitive).asJava,
    keyText,
  )

  private def mkCreateData(
      maintainer: PartyId = defaultMaintainer,
      keyText: String = "dummyKey",
      mkCreateCmds: String => Seq[data.Command] =
        createCommands(defaultMaintainer, defaultObserver, _),
  )(implicit
      env: TestConsoleEnvironment
  ): (GenTransactionTree, ContractInstance) = {
    import env.*

    val createCmd =
      CommandsWithMetadata(
        mkCreateCmds(keyText).map(c => Command.fromJavaProto(c.toProtoCommand)),
        Seq(maintainer),
        ledgerTime = environment.now.toLf,
      )

    // Run Phase 1, make it fail with an exception, use the exception to extract the transaction tree
    case class InterceptException(tree: GenTransactionTree) extends RuntimeException
    val InterceptException(tree) = maliciousP1
      .submitCommand(
        createCmd,
        transactionTreeInterceptor = tree => throw InterceptException(tree),
      )
      .value
      .transform {
        case Failure(ex: InterceptException) => Success(Outcome(ex))
        case other => fail(s"Unexpected submission outcome: $other")
      }
      .futureValueUS

    val newContractInstance: NewContractInstance =
      tree.rootViews.toSeq
        .flatMap(_.tryUnwrap.viewParticipantData.tryUnwrap.createdCore.map(_.contract))
        .loneElement
    val contractInstance = newContractInstance.traverseCreatedAt {
      case t: CreationTime.CreatedAt => Right(t)
      case t: CreationTime => Left(s"Unsupported created at time: $t")
    }.value

    (tree, contractInstance)
  }

  private def archive(
      cid: LfContractId,
      maintainer: PartyId = defaultMaintainer,
  )(implicit env: TestConsoleEnvironment): data.Transaction = {
    import env.*
    participant1.ledger_api.javaapi.commands.submit(Seq(maintainer), archiveCommands(cid))
  }

  private def archiveMaliciously(
      cid: LfContractId,
      maintainer: PartyId = defaultMaintainer,
      disclosedContracts: Map[LfContractId, GenContractInstance] = Map.empty,
  )(implicit
      env: TestConsoleEnvironment
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] =
    maliciousP1.submitCommand(archiveCmdsWithMetadata(cid, maintainer, disclosedContracts))

  private def archiveCmdsWithMetadata(
      cid: LfContractId,
      maintainer: PartyId = defaultMaintainer,
      disclosedContracts: Map[LfContractId, GenContractInstance] = Map.empty,
  )(implicit
      env: TestConsoleEnvironment
  ): CommandsWithMetadata = {
    import env.*
    CommandsWithMetadata(
      archiveCommands(cid).map(c => Command.fromJavaProto(c.toProtoCommand)),
      Seq(maintainer),
      disclosedContracts = disclosedContracts,
      ledgerTime = environment.now.toLf,
    )
  }

  private def archiveCommands(cid: LfContractId): Seq[data.Command] =
    new UniversalContract.ContractId(cid.coid)
      .exerciseArchive()
      .commands()
      .asScala
      .toSeq

  private def queryByKeyCmdsWithMetadata(
      keyText: String,
      disclosedContracts: Map[LfContractId, GenContractInstance] = Map.empty,
  )(implicit
      env: TestConsoleEnvironment
  ): CommandsWithMetadata = {
    import env.*

    val cmdsRaw = queryByKeyCommands(keyText)
      .map(c => Command.fromJavaProto(c.toProtoCommand))

    CommandsWithMetadata(
      cmdsRaw,
      Seq(defaultMaintainer),
      disclosedContracts = disclosedContracts,
      ledgerTime = environment.now.toLf,
    )
  }

  private def queryByKeyCommands(keyText: String): Seq[data.Command] =
    mgmtContract.id
      .exerciseDelegateLookup(
        new types.Tuple2(
          List(defaultMaintainer.toProtoPrimitive).asJava,
          keyText,
        ),
        Long.MaxValue,
        new util.ArrayList[String](),
      )
      .commands()
      .asScala
      .toSeq

  def modifyQueryByKeyResult(f: Vector[LfContractId] => Vector[LfContractId])(
      tx: LfSubmittedTransaction,
      metadata: Transaction.Metadata,
  ): (LfSubmittedTransaction, Transaction.Metadata) = {

    val modifiedNodes = tx.nodes.map {
      case (nid, qbk: LfNodeQueryByKey) =>
        nid -> qbk.focus(_.result).modify(f).focus(_.exhaustive).replace(true)
      case keyValue => keyValue
    }

    LfSubmittedTransaction(
      LfVersionedTransaction(tx.version, modifiedNodes, tx.roots)
    ) -> metadata
  }

  private def assignNonExistentContract(
      maintainer: PartyId = defaultMaintainer,
      keyText: String = "dummyKey",
      mkCreateCmds: String => Seq[data.Command] =
        createCommands(defaultMaintainer, defaultObserver, _),
  )(implicit
      env: TestConsoleEnvironment
  ): (GenTransactionTree, ContractInstance) = {
    import env.*

    // Create a contract
    val (transactionTree, instance) = mkCreateData(maintainer, keyText, mkCreateCmds)
    assertInactive(instance.contractId)

    // Assign
    val (_, assignEvents) = runMaliciously()(
      assignMaliciously(instance, maintainer)
    )
    assignEvents.assertStatusOk(participant1)

    assertActive(instance.contractId)

    (transactionTree, instance)
  }

  private def assignMaliciously(
      instance: ContractInstance,
      maintainer: PartyId = defaultMaintainer,
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter(1),
  )(implicit
      env: TestConsoleEnvironment
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    import env.*

    val data = UnassignmentData(
      ReassignmentSubmitterMetadata(
        maintainer.toLf,
        participant1.id,
        LedgerCommandId.assertFromString("kartoffelsuppe"),
        None,
        LedgerUserId.assertFromString(LedgerApiCommands.defaultUserId),
        None,
      ),
      ContractsReassignmentBatch(
        instance,
        Source(UniversalContract.PACKAGE_ID),
        Target(UniversalContract.PACKAGE_ID),
        reassignmentCounter,
      ),
      Set(participant1.id),
      Source(acmeId),
      Target(daId),
      Target(participant1.testing.fetch_synchronizer_time(daId)),
      participant1.testing.fetch_synchronizer_time(acmeId),
    )

    maliciousP1.submitAssignmentRequest(maintainer.toLf, data)
  }

  private def unassignNonExistentContract(
      maintainer: PartyId = defaultMaintainer,
      keyText: String = "dummyKey",
      mkCreateCmds: String => Seq[data.Command] =
        createCommands(defaultMaintainer, defaultObserver, _),
  )(implicit
      env: TestConsoleEnvironment
  ): (GenTransactionTree, ContractInstance, Long) = {
    import env.*
    val (transactionTree, instance) = assignNonExistentContract(maintainer, keyText, mkCreateCmds)

    val event = participant1.ledger_api.commands.submit_unassign(
      defaultMaintainer,
      Seq(instance.contractId),
      daId,
      acmeId,
    )

    val counter = event.events.loneElement.reassignmentCounter

    (transactionTree, instance, counter)
  }

  private def unassignMaliciously(
      instance: ContractInstance,
      maintainer: PartyId = defaultMaintainer,
      reassignmentCounter: ReassignmentCounter = ReassignmentCounter(1),
  )(implicit
      env: TestConsoleEnvironment
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    import env.*

    val helpers = ReassignmentDataHelpers(
      contract = instance,
      sourceSynchronizer = Source(daId),
      targetSynchronizer = Target(acmeId),
      pureCrypto = pureCrypto,
      targetTimestamp = Target(participant1.testing.fetch_synchronizer_time(acmeId)),
    )
    val tree = helpers
      .fullUnassignmentTree(
        maintainer.toLf,
        participant1,
        MediatorGroupRecipient(NonNegativeInt.zero),
      )(reassignmentCounter = reassignmentCounter)

    maliciousP1.submitUnassignmentRequest(tree)
  }

  private def runMaliciously[A](
      confirmingParties: Set[LfPartyId] = Set(defaultMaintainer.toLf),
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

  private def createdIdOf(events: TrackingResult)(implicit
      env: TestConsoleEnvironment
  ): LfContractId = {
    import env.*

    events.assertStatusOk(participant1)
    val tx = events.awaitTransactions(participant1).loneElement
    val event = tx.events.loneElement
    createdIdOfEvent(event)
  }

  private def createdIdOfEvent(event: Event): LfContractId = {
    val cidStr = event.event.created.value.contractId
    LfContractId.assertFromString(cidStr)
  }

  private def exercisedIdOf(events: TrackingResult, consuming: Boolean = true)(implicit
      env: TestConsoleEnvironment
  ): LfContractId = {
    import env.*

    val tx = events.awaitTransactions(participant1).loneElement
    val event = tx.events.loneElement
    exercisedIdOfEvent(event, consuming)
  }

  private def exercisedIdOfEvent(event: Event, consuming: Boolean = true): LfContractId = {
    val exercise = event.event.exercised.value
    exercise.consuming shouldBe consuming
    val cidStr = exercise.contractId
    LfContractId.assertFromString(cidStr)
  }

  def assertUnassigned(events: TrackingResult, contractId: LfContractId)(implicit
      env: TestConsoleEnvironment
  ): Assertion = {
    import env.*

    events
      .allUnassigned(participant1)
      .loneElement shouldBe contractId.coid
  }

  def assertAssigned(events: TrackingResult, contractId: LfContractId)(implicit
      env: TestConsoleEnvironment
  ): Assertion = {
    import env.*

    events
      .allAssigned(participant1)
      .loneElement shouldBe contractId.coid
  }

  def assertActive(contractId: LfContractId)(implicit
      env: TestConsoleEnvironment
  ): Assertion = {
    import env.*

    participant1.ledger_api.state.acs
      .active_contracts_of_party(defaultMaintainer)
      .map(_.createdEvent.value.contractId) should contain(contractId.coid)
    lookupActiveContractInstances(contractId.coid).loneElement.contractId shouldBe contractId
  }

  /** Looks-up contract instances. Yields only instances that are active in the Canton ACS.
    */
  private def lookupActiveContractInstances(
      contractIdStr: String
  )(implicit env: TestConsoleEnvironment): Seq[ContractInstance] = {
    import env.*
    participant1.testing.acs_search(daName, exactId = contractIdStr, limit = PositiveInt.one)
  }

  def assertInactive(contractId: LfContractId)(implicit
      env: TestConsoleEnvironment
  ): Assertion = {
    import env.*

    participant1.ledger_api.state.acs
      .active_contracts_of_party(defaultMaintainer)
      .map(_.createdEvent.value.contractId) should not contain contractId.coid
    lookupActiveContractInstances(contractId.coid) shouldBe empty
  }
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
