// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.implicits.*
import com.daml.lf.engine.Error
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.ViewType.TransferInViewType
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferInTree, TransferSubmitterMetadata}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.participant.admin.{PackageInspectionOpsForTesting, PackageService}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.mkActivenessSet
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  InFlightSubmissionTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferInProcessingSteps.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  NoSubmissionPermission,
  ReceivedMultipleRequests,
  ReceivedNoRequests,
  StakeholderMismatch,
  SubmittingPartyMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.{
  GlobalCausalOrderer,
  ProcessingStartingPoints,
  SingleDomainCausalTracker,
}
import com.digitalasset.canton.participant.store.TransferStoreTest.{
  coidAbs1,
  contract,
  transactionId1,
}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{
  MultiDomainEventLog,
  SyncDomainEphemeralState,
  SyncDomainPersistentState,
  TransferStoreTest,
}
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{submitter, submitterParticipant}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.time.{DomainTimeTracker, TimeProofTestUtil}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

class TransferInProcessingStepsTest extends AsyncWordSpec with BaseTest {
  private val sourceDomain = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  private val sourceMediator = MediatorId(
    UniqueIdentifier.tryFromProtoPrimitive("mediator::source")
  )
  private val targetDomain = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  private val targetMediator = MediatorId(
    UniqueIdentifier.tryFromProtoPrimitive("mediator::target")
  )
  private val anotherDomain = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::another"))
  private val anotherMediator = MediatorId(
    UniqueIdentifier.tryFromProtoPrimitive("mediator::another")
  )

  private val party1: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party1::party")
  ).toLf
  private val party2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("party2::party")
  ).toLf

  private val participant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::participant")
  )
  private def submitterInfo(submitter: LfPartyId): TransferSubmitterMetadata = {
    TransferSubmitterMetadata(
      submitter,
      LedgerApplicationId.assertFromString("tests"),
      participant.toLf,
      None,
    )
  }

  private val workflowId: Option[LfWorkflowId] = None

  private val identityFactory = TestingTopology()
    .withDomains(sourceDomain)
    .withReversedTopology(
      Map(submitterParticipant -> Map(party1 -> ParticipantPermission.Submission))
    )
    .withParticipants(participant) // required such that `participant` gets a signing key
    .build(loggerFactory)

  private val cryptoSnapshot =
    identityFactory
      .forOwnerAndDomain(submitterParticipant, sourceDomain)
      .currentSnapshotApproximation

  private val pureCrypto = TestingIdentityFactory.pureCrypto()

  val hash = TestHash.digest("123")
  private val seedGenerator = new SeedGenerator(pureCrypto)
  val globalTracker = new GlobalCausalOrderer(
    participant,
    _ => true,
    DefaultProcessingTimeouts.testing,
    new InMemoryMultiDomainCausalityStore(loggerFactory),
    loggerFactory,
  )

  private val transferInProcessingSteps =
    testInstance(targetDomain, Set(party1), Set(party1), cryptoSnapshot, None)

  private def statefulDependencies
      : Future[(SyncDomainPersistentState, SyncDomainEphemeralState)] = {
    val multiDomainEventLog = mock[MultiDomainEventLog]
    val persistentState =
      new InMemorySyncDomainPersistentState(
        IndexedDomain.tryCreate(targetDomain, 1),
        pureCrypto,
        enableAdditionalConsistencyChecks = true,
        loggerFactory,
      )
    for {
      _ <- persistentState.parameterStore.setParameters(defaultStaticDomainParameters)
    } yield {
      val state = new SyncDomainEphemeralState(
        persistentState,
        multiDomainEventLog,
        new SingleDomainCausalTracker(
          globalTracker,
          new InMemorySingleDomainCausalDependencyStore(targetDomain, loggerFactory),
          loggerFactory,
        ),
        mock[InFlightSubmissionTracker],
        ProcessingStartingPoints.default,
        _ => mock[DomainTimeTracker],
        ParticipantTestMetrics.domain,
        DefaultProcessingTimeouts.testing,
        useCausalityTracking = true,
        loggerFactory = loggerFactory,
      )
      (persistentState, state)
    }
  }

  "prepare submission" should {
    def setUpOrFail(
        transferData: TransferData,
        transferOutResult: DeliveredTransferOutResult,
        persistentState: SyncDomainPersistentState,
    ): Future[Unit] = {
      for {
        _ <- valueOrFail(persistentState.transferStore.addTransfer(transferData))(
          "add transfer data failed"
        )
        _ <- valueOrFail(persistentState.transferStore.addTransferOutResult(transferOutResult))(
          "add transfer out result failed"
        )
      } yield ()
    }

    val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
    val transferDataF =
      TransferStoreTest.mkTransferDataForDomain(transferId, sourceMediator, party1, targetDomain)
    val submissionParam =
      SubmissionParam(
        submitterInfo(party1),
        transferId,
        workflowId,
        SourceProtocolVersion(testedProtocolVersion),
      )
    val transferOutResult =
      TransferInProcessingStepsTest.transferOutResult(
        sourceDomain,
        cryptoSnapshot,
        participant,
      )

    "succeed without errors" in {
      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(transferData, transferOutResult, persistentState)
        _preparedSubmission <-
          transferInProcessingSteps
            .prepareSubmission(
              submissionParam,
              targetMediator,
              state,
              cryptoSnapshot,
            )
            .valueOrFailShutdown("transfer in submission")
      } yield succeed
    }

    "fail when a receiving party has no participant on the domain" in {
      val transferOutRequest = TransferOutRequest(
        submitterInfo(party1),
        Set(party1, party2), // Party 2 is a stakeholder and therefore a receiving party
        Set.empty,
        workflowId,
        coidAbs1,
        transferId.sourceDomain,
        SourceProtocolVersion(testedProtocolVersion),
        sourceMediator,
        targetDomain,
        TargetProtocolVersion(testedProtocolVersion),
        TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, domainId = targetDomain),
      )
      val uuid = new UUID(1L, 2L)
      val seed = seedGenerator.generateSaltSeed()
      val transferData2 = {
        val fullTransferOutTree =
          transferOutRequest.toFullTransferOutTree(
            pureCrypto,
            pureCrypto,
            seed,
            uuid,
          )
        TransferData(
          SourceProtocolVersion(testedProtocolVersion),
          transferId.requestTimestamp,
          RequestCounter(0),
          fullTransferOutTree,
          CantonTimestamp.ofEpochSecond(10),
          contract,
          transactionId1,
          None,
        )
      }
      for {
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(transferData2, transferOutResult, persistentState)
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.prepareSubmission(
            submissionParam,
            targetMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        inside(preparedSubmission) { case NoParticipantForReceivingParty(_, p) =>
          assert(p == party2)
        }
      }
    }

    "fail when transfer-out processing is not yet complete" in {
      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- valueOrFail(persistentState.transferStore.addTransfer(transferData))(
          "add transfer data failed"
        )
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.prepareSubmission(
            submissionParam,
            targetMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case TransferOutIncomplete(_, _) =>
        }
      }
    }

    "fail when submitting party is not a stakeholder" in {
      val submissionParam2 =
        SubmissionParam(
          submitterInfo(party2),
          transferId,
          workflowId,
          SourceProtocolVersion(testedProtocolVersion),
        )

      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(transferData, transferOutResult, persistentState)
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.prepareSubmission(
            submissionParam2,
            targetMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case SubmittingPartyMustBeStakeholder(_, _, _) =>
        }
      }
    }

    "fail when participant does not have submission permission for party" in {

      val failingTopology = TestingTopology(domains = Set(sourceDomain))
        .withReversedTopology(
          Map(submitterParticipant -> Map(party1 -> ParticipantPermission.Observation))
        )
        .build(loggerFactory)
      val cryptoSnapshot2 =
        failingTopology.forOwnerAndDomain(participant, sourceDomain).currentSnapshotApproximation

      for {
        transferData <- transferDataF
        deps <- statefulDependencies
        (persistentState, state) = deps
        _ <- setUpOrFail(transferData, transferOutResult, persistentState)
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.prepareSubmission(
            submissionParam,
            targetMediator,
            state,
            cryptoSnapshot2,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case NoSubmissionPermission(_, _, _) =>
        }
      }
    }

    "fail when submitting party not hosted on the participant" in {

      val submissionParam2 =
        SubmissionParam(
          submitterInfo(party2),
          transferId,
          workflowId,
          SourceProtocolVersion(testedProtocolVersion),
        )

      for {
        transferData2 <- TransferStoreTest.mkTransferDataForDomain(
          transferId,
          sourceMediator,
          party2,
          targetDomain,
        )
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps
        _ <- setUpOrFail(transferData2, transferOutResult, persistentState)
        preparedSubmission <- leftOrFailShutdown(
          transferInProcessingSteps.prepareSubmission(
            submissionParam2,
            targetMediator,
            ephemeralState,
            cryptoSnapshot,
          )
        )("prepare submission did not return a left")
      } yield {
        preparedSubmission should matchPattern { case PartyNotHosted(_, _, _) =>
        }
      }
    }
  }

  "receive request" should {
    val contractId = ExampleTransactionFactory.suffixedId(10, 0)
    val contract = ExampleTransactionFactory.asSerializable(
      contractId,
      contractInstance = ExampleTransactionFactory.contractInstance(),
    )

    val transferOutResult =
      TransferInProcessingStepsTest.transferOutResult(
        sourceDomain,
        cryptoSnapshot,
        submitterParticipant,
      )
    val inTree =
      makeFullTransferInTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        transferOutResult,
      )
    val inRequestF = for {
      inRequest <- encryptFullTransferInTree(inTree)
    } yield inRequest

    def checkSuccessful(
        result: transferInProcessingSteps.CheckActivenessAndWritePendingContracts
    ): Assertion =
      result match {
        case transferInProcessingSteps.CheckActivenessAndWritePendingContracts(
              activenessSet,
              pendingContracts,
              _,
            ) =>
          assert(activenessSet == mkActivenessSet(txIn = Set(contractId)))
          assert(pendingContracts == Seq(WithTransactionId(contract, transactionId1)))
        case _ => fail()
      }

    "succeed without errors" in {
      for {
        inRequest <- inRequestF
        envelopes = NonEmpty(
          Seq,
          OpenEnvelope(inRequest, RecipientsTest.testInstance, testedProtocolVersion),
        )
        decrypted <- valueOrFail(transferInProcessingSteps.decryptViews(envelopes, cryptoSnapshot))(
          "decrypt request failed"
        )
        result <- valueOrFail(
          transferInProcessingSteps.computeActivenessSetAndPendingContracts(
            CantonTimestamp.Epoch,
            RequestCounter(1),
            SequencerCounter(1),
            NonEmptyUtil.fromUnsafe(decrypted.views),
            Seq.empty,
            cryptoSnapshot,
          )
        )("compute activeness set failed")
      } yield {
        decrypted.decryptionErrors shouldBe Seq.empty
        checkSuccessful(result)
      }
    }

    "fail when target domain is not current domain" in {
      val inTree2 = makeFullTransferInTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        anotherDomain,
        anotherMediator,
        transferOutResult,
      )
      for {
        result <- leftOrFail(
          transferInProcessingSteps.computeActivenessSetAndPendingContracts(
            CantonTimestamp.Epoch,
            RequestCounter(1),
            SequencerCounter(1),
            NonEmpty(Seq, WithRecipients(inTree2, RecipientsTest.testInstance)),
            Seq.empty,
            cryptoSnapshot,
          )
        )("compute activeness set did not return a left")
      } yield {
        result match {
          case UnexpectedDomain(_, targetD, currentD) =>
            assert(targetD == anotherDomain)
            assert(currentD == targetDomain)
          case x => fail(x.toString)
        }
      }
    }

    "fail when multiple requests are present" in {
      // Send the same transfer-in request twice
      for {
        result <- leftOrFail(
          transferInProcessingSteps.computeActivenessSetAndPendingContracts(
            CantonTimestamp.Epoch,
            RequestCounter(1),
            SequencerCounter(1),
            NonEmpty(
              Seq,
              WithRecipients(inTree, RecipientsTest.testInstance),
              WithRecipients(inTree, RecipientsTest.testInstance),
            ),
            Seq.empty,
            cryptoSnapshot,
          )
        )("compute activenss set did not return a left")
      } yield {
        result should matchPattern { case ReceivedMultipleRequests(Seq(_, _)) =>
        }
      }
    }

    "fail if there are not transfer-in view trees with the right root hash" in {
      transferInProcessingSteps.pendingDataAndResponseArgsForMalformedPayloads(
        CantonTimestamp.Epoch,
        RequestCounter(1),
        SequencerCounter(1),
        Seq.empty,
        cryptoSnapshot,
      ) shouldBe Left(ReceivedNoRequests)
    }
  }

  "construct pending data and response" should {
    val contractId = ExampleTransactionFactory.suffixedId(10, 0)
    val contract =
      ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(),
        metadata = ContractMetadata.tryCreate(Set.empty, Set(party1), None),
      )
    val transferOutResult =
      TransferInProcessingStepsTest.transferOutResult(
        sourceDomain,
        cryptoSnapshot,
        submitterParticipant,
      )

    "fail when wrong stakeholders given" in {
      for {
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps

        // party2 is incorrectly registered as a stakeholder
        fullTransferInTree2 = makeFullTransferInTree(
          party1,
          stakeholders = Set(party1, party2),
          contract,
          transactionId1,
          targetDomain,
          targetMediator,
          transferOutResult,
        )

        pendingDataAndResponseArgs2 = TransferInProcessingSteps.PendingDataAndResponseArgs(
          fullTransferInTree2,
          CantonTimestamp.Epoch,
          RequestCounter(1),
          SequencerCounter(1),
          cryptoSnapshot,
          transferringParticipant = true,
        )

        transferLookup = ephemeralState.transferCache
        contractLookup = ephemeralState.contractLookup

        result <- leftOrFail(
          transferInProcessingSteps
            .constructPendingDataAndResponse(
              pendingDataAndResponseArgs2,
              transferLookup,
              contractLookup,
              ephemeralState.causalityLookup,
              Future.successful(ActivenessResult.success),
              Future.unit,
              targetMediator,
            )
        )("construction of pending data and response did not return a left")
      } yield {
        result should matchPattern { case StakeholderMismatch(_, _, _, _) =>
        }
      }
    }

    "succeed without errors" in {

      for {
        deps <- statefulDependencies
        (persistentState, ephemeralState) = deps

        transferLookup = ephemeralState.transferCache
        contractLookup = ephemeralState.contractLookup

        fullTransferInTree = makeFullTransferInTree(
          party1,
          Set(party1),
          contract,
          transactionId1,
          targetDomain,
          targetMediator,
          transferOutResult,
        )
        pendingDataAndResponseArgs = TransferInProcessingSteps.PendingDataAndResponseArgs(
          fullTransferInTree,
          CantonTimestamp.Epoch,
          RequestCounter(1),
          SequencerCounter(1),
          cryptoSnapshot,
          transferringParticipant = true,
        )

        _unit = ephemeralState.causalityLookup.globalCausalOrderer.domainCausalityStore
          .registerTransferOut(
            fullTransferInTree.transferOutResultEvent.transferId,
            Set(
              VectorClock(sourceDomain, CantonTimestamp.MinValue.plusSeconds(1L), party1, Map.empty)
            ),
          )

        result <- valueOrFail(
          transferInProcessingSteps
            .constructPendingDataAndResponse(
              pendingDataAndResponseArgs,
              transferLookup,
              contractLookup,
              ephemeralState.causalityLookup,
              Future.successful(ActivenessResult.success),
              Future.unit,
              targetMediator,
            )
        )("construction of pending data and response failed")
      } yield {
        succeed
      }
    }
  }

  "validateTransferInRequest" should {
    val contractId = ExampleTransactionFactory.suffixedId(10, 0)
    val contract = ExampleTransactionFactory.asSerializable(
      contractId,
      contractInstance = ExampleTransactionFactory.contractInstance(),
    )
    val transferOutResult =
      TransferInProcessingStepsTest.transferOutResult(
        sourceDomain,
        cryptoSnapshot,
        submitterParticipant,
      )
    val inRequest =
      makeFullTransferInTree(
        party1,
        Set(party1),
        contract,
        transactionId1,
        targetDomain,
        targetMediator,
        transferOutResult,
      )

    "succeed without errors in the basic case" in {
      for {
        result <- valueOrFail(
          transferInProcessingSteps
            .validateTransferInRequest(
              CantonTimestamp.Epoch,
              inRequest,
              None,
              cryptoSnapshot,
              transferringParticipant = false,
            )
        )("validation of transfer in request failed")
      } yield {
        result shouldBe None
      }
    }

    val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
    val transferOutRequest = TransferOutRequest(
      submitterInfo(party1),
      Set(party1, party2), // Party 2 is a stakeholder and therefore a receiving party
      Set.empty,
      workflowId,
      contractId,
      transferId.sourceDomain,
      SourceProtocolVersion(testedProtocolVersion),
      sourceMediator,
      targetDomain,
      TargetProtocolVersion(testedProtocolVersion),
      TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, domainId = targetDomain),
    )
    val uuid = new UUID(3L, 4L)
    val seed = seedGenerator.generateSaltSeed()
    val fullTransferOutTree =
      transferOutRequest.toFullTransferOutTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )
    val transferData =
      TransferData(
        SourceProtocolVersion(testedProtocolVersion),
        CantonTimestamp.Epoch,
        RequestCounter(1),
        fullTransferOutTree,
        CantonTimestamp.Epoch,
        contract,
        transactionId1,
        Some(transferOutResult),
      )

    "succeed without errors when transfer data is valid" in {
      for {
        result <- valueOrFail(
          transferInProcessingSteps
            .validateTransferInRequest(
              CantonTimestamp.Epoch,
              inRequest,
              Some(transferData),
              cryptoSnapshot,
              transferringParticipant = false,
            )
        )("validation of transfer in request failed")
      } yield {
        result match {
          case Some(TransferInValidationResult(confirmingParties)) =>
            assert(confirmingParties == Set(party1))
          case _ => fail()
        }
      }
    }

    "wait for the topology state to be available " in {
      val promise: Promise[Unit] = Promise()
      val transferInProcessingSteps2 =
        testInstance(
          targetDomain,
          Set(party1),
          Set(party1),
          cryptoSnapshot,
          Some(promise.future), // Topology state is not available
        )

      val inValidated = transferInProcessingSteps2
        .validateTransferInRequest(
          CantonTimestamp.Epoch,
          inRequest,
          Some(transferData),
          cryptoSnapshot,
          transferringParticipant = false,
        )
        .value

      always() {
        inValidated.isCompleted shouldBe false
      }

      promise.completeWith(Future.unit)
      for {
        _ <- inValidated
      } yield { succeed }
    }
  }

  "get commit set and contracts to be stored and event" should {
    "succeed without errors" in {

      val inRes = TransferInProcessingStepsTest.transferInResult(targetDomain)

      val contractId = ExampleTransactionFactory.suffixedId(10, 0)
      val contract =
        ExampleTransactionFactory.asSerializable(
          contractId,
          contractInstance = ExampleTransactionFactory.contractInstance(),
          metadata = ContractMetadata.tryCreate(Set(party1), Set(party1), None),
        )
      val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
      val rootHash = mock[RootHash]
      when(rootHash.asLedgerTransactionId).thenReturn(LedgerTransactionId.fromString("id1"))
      val pendingRequestData = TransferInProcessingSteps.PendingTransferIn(
        RequestId(CantonTimestamp.Epoch),
        RequestCounter(1),
        SequencerCounter(1),
        rootHash,
        contract,
        submitterInfo(submitter),
        workflowId,
        transactionId1,
        transferringParticipant = false,
        transferId,
        contract.metadata.stakeholders,
      )

      for {
        deps <- statefulDependencies
        (_persistentState, state) = deps

        _result <- valueOrFail(
          transferInProcessingSteps.getCommitSetAndContractsToBeStoredAndEvent(
            mock[SignedContent[Deliver[DefaultOpenEnvelope]]],
            Right(inRes),
            pendingRequestData,
            state.pendingTransferInSubmissions,
            state.causalityLookup,
            pureCrypto,
          )
        )("get commit set and contracts to be stored and event failed")
      } yield succeed
    }
  }

  def testInstance(
      domainId: DomainId,
      signatories: Set[LfPartyId],
      stakeholders: Set[LfPartyId],
      snapshotOverride: DomainSnapshotSyncCryptoApi,
      awaitTimestampOverride: Option[Future[Unit]],
  ): TransferInProcessingSteps = {

    val pureCrypto = new SymbolicPureCrypto
    val engine = DAMLe.newEngine(uniqueContractKeys = false, enableLfDev = false)
    val mockPackageService =
      new PackageService(
        engine,
        new InMemoryDamlPackageStore(loggerFactory),
        mock[ParticipantEventPublisher],
        pureCrypto,
        _ => EitherT.rightT(()),
        new PackageInspectionOpsForTesting(participant, loggerFactory),
        ProcessingTimeout(),
        loggerFactory,
      )
    val packageResolver = DAMLe.packageResolver(mockPackageService)
    val damle: DAMLe = new DAMLe(
      packageResolver,
      engine,
      loggerFactory,
    ) {
      override def contractMetadata(
          contractInstance: LfContractInst,
          supersetOfSignatories: Set[LfPartyId],
      )(implicit traceContext: TraceContext): EitherT[Future, Error, ContractMetadata] = {
        EitherT.pure[Future, Error](ContractMetadata.tryCreate(signatories, stakeholders, None))
      }
    }
    val seedGenerator = new SeedGenerator(pureCrypto)

    new TransferInProcessingSteps(
      domainId,
      submitterParticipant,
      damle,
      TestTransferCoordination.apply(
        Set(),
        CantonTimestamp.Epoch,
        Some(snapshotOverride),
        Some(awaitTimestampOverride),
        loggerFactory,
      ),
      seedGenerator,
      causalityTracking = true,
      TargetProtocolVersion(testedProtocolVersion),
      loggerFactory = loggerFactory,
    )
  }

  def makeFullTransferInTree(
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: DomainId,
      targetMediator: MediatorId,
      transferOutResult: DeliveredTransferOutResult,
      uuid: UUID = new UUID(4L, 5L),
  ): FullTransferInTree = {
    val seed = seedGenerator.generateSaltSeed()
    TransferInProcessingSteps.makeFullTransferInTree(
      pureCrypto,
      seed,
      submitterInfo(submitter),
      workflowId,
      stakeholders,
      contract,
      creatingTransactionId,
      targetDomain,
      targetMediator,
      transferOutResult,
      uuid,
      SourceProtocolVersion(testedProtocolVersion),
      TargetProtocolVersion(testedProtocolVersion),
    )
  }

  def encryptFullTransferInTree(
      tree: FullTransferInTree
  ): Future[EncryptedViewMessage[TransferInViewType]] =
    EncryptedViewMessageFactory
      .create(TransferInViewType)(tree, cryptoSnapshot, testedProtocolVersion)
      .fold(
        error => throw new IllegalArgumentException(s"Cannot encrypt transfer-in request: $error"),
        Predef.identity,
      )

  def makeTransferInRequest(
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: DomainId,
      targetMediator: MediatorId,
      transferOutResult: DeliveredTransferOutResult,
  ): Future[EncryptedViewMessage[TransferInViewType]] = {
    val tree = makeFullTransferInTree(
      submitter,
      stakeholders,
      contract,
      creatingTransactionId,
      targetDomain,
      targetMediator,
      transferOutResult,
    )

    for {
      request <- encryptFullTransferInTree(tree)
    } yield request
  }

  def makeRootHashMessage(
      tree: FullTransferInTree
  ): RootHashMessage[SerializedRootHashMessagePayload] =
    RootHashMessage(
      tree.rootHash,
      tree.domainId,
      testedProtocolVersion,
      TransferInViewType,
      SerializedRootHashMessagePayload.empty,
    )
}

object TransferInProcessingStepsTest {

  def transferOutResult(
      sourceDomain: DomainId,
      cryptoSnapshot: SyncCryptoApi,
      participantId: ParticipantId,
  )(implicit traceContext: TraceContext): DeliveredTransferOutResult = {
    val protocolVersion = BaseTest.testedProtocolVersion

    implicit val ec: ExecutionContext = DirectExecutionContext(
      TracedLogger(
        NamedLoggerFactory("test-area", "transfer").getLogger(
          TransferInProcessingStepsTest.getClass
        )
      )
    )

    val result =
      TransferResult.create(
        RequestId(CantonTimestamp.Epoch),
        Set(),
        TransferOutDomainId(sourceDomain),
        Verdict.Approve(protocolVersion),
        protocolVersion,
      )
    val signedResult: SignedProtocolMessage[TransferOutResult] =
      Await.result(
        SignedProtocolMessage.tryCreate(result, cryptoSnapshot, protocolVersion),
        10.seconds,
      )
    val batch: Batch[OpenEnvelope[SignedProtocolMessage[TransferOutResult]]] =
      Batch.of(protocolVersion, (signedResult, Recipients.cc(participantId)))
    val deliver: Deliver[OpenEnvelope[SignedProtocolMessage[TransferOutResult]]] =
      Deliver.create(
        SequencerCounter(0),
        CantonTimestamp.Epoch,
        sourceDomain,
        Some(MessageId.tryCreate("msg-0")),
        batch,
        protocolVersion,
      )
    val signature =
      Await
        .result(cryptoSnapshot.sign(TestHash.digest("dummySignature")).value, 10.seconds)
        .valueOr(err => throw new RuntimeException(err.toString))
    val signedContent = SignedContent(
      deliver,
      signature,
      None,
    )

    val transferOutResult = DeliveredTransferOutResult(signedContent)
    transferOutResult
  }

  def transferInResult(targetDomain: DomainId): TransferInResult = TransferResult.create(
    RequestId(CantonTimestamp.Epoch),
    Set(),
    TransferInDomainId(targetDomain),
    Verdict.Approve(BaseTest.testedProtocolVersion),
    BaseTest.testedProtocolVersion,
  )
}
