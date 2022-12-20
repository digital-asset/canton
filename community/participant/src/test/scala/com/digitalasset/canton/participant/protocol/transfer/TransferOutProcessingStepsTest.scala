// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.implicits.*
import com.daml.lf.CantonOnly
import com.daml.lf.engine.Error
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.ViewType.TransferOutViewType
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferOutTree}
import com.digitalasset.canton.participant.admin.{PackageInspectionOpsForTesting, PackageService}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.mkActivenessSet
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  InFlightSubmissionTracker,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps.{
  PendingTransferOut,
  PermissionErrors,
  TargetDomainIsSourceDomain,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  NoSubmissionPermission,
  ReceivedNoRequests,
  SubmittingPartyMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.{
  GlobalCausalOrderer,
  ProcessingStartingPoints,
  SingleDomainCausalTracker,
}
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.store.{MultiDomainEventLog, SyncDomainEphemeralState}
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.time.{DomainTimeTracker, TimeProofTestUtil}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorService,
  LedgerTransactionId,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.annotation.nowarn
import scala.collection.immutable.Set
import scala.concurrent.{ExecutionContext, Future}

@nowarn("msg=match may not be exhaustive")
class TransferOutProcessingStepsTest extends AsyncWordSpec with BaseTest with HasExecutorService {

  private implicit val ec: ExecutionContext = executorService

  val sourceDomain = DomainId(UniqueIdentifier.tryFromProtoPrimitive("source::domain"))
  val sourceMediator = MediatorId(UniqueIdentifier.tryFromProtoPrimitive("source::mediator"))
  val targetDomain = DomainId(UniqueIdentifier.tryFromProtoPrimitive("target::domain"))

  val submitter: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("submitter::party")
  ).toLf
  val party1: LfPartyId = PartyId(UniqueIdentifier.tryFromProtoPrimitive("party1::party")).toLf
  val party2: LfPartyId = PartyId(UniqueIdentifier.tryFromProtoPrimitive("party2::party")).toLf

  val submittingParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("submitting::participant")
  )

  val adminSubmitter: LfPartyId = submittingParticipant.adminParty.toLf

  val pureCrypto = TestingIdentityFactory.pureCrypto()

  val multiDomainEventLog = mock[MultiDomainEventLog]
  val persistentState =
    new InMemorySyncDomainPersistentState(
      IndexedDomain.tryCreate(sourceDomain, 1),
      pureCrypto,
      enableAdditionalConsistencyChecks = true,
      loggerFactory,
    )
  val globalTracker = new GlobalCausalOrderer(
    submittingParticipant,
    _ => true,
    DefaultProcessingTimeouts.testing,
    new InMemoryMultiDomainCausalityStore(loggerFactory),
    loggerFactory,
  )

  def mkState: SyncDomainEphemeralState =
    new SyncDomainEphemeralState(
      persistentState,
      multiDomainEventLog,
      new SingleDomainCausalTracker(
        globalTracker,
        new InMemorySingleDomainCausalDependencyStore(sourceDomain, loggerFactory),
        loggerFactory,
      ),
      mock[InFlightSubmissionTracker],
      ProcessingStartingPoints.default,
      _ => mock[DomainTimeTracker],
      ParticipantTestMetrics.domain,
      DefaultProcessingTimeouts.testing,
      useCausalityTracking = true,
      loggerFactory,
    )(executorService)

  val engine = CantonOnly.newDamlEngine(uniqueContractKeys = false, enableLfDev = false)
  val mockPackageService =
    new PackageService(
      engine,
      new InMemoryDamlPackageStore(loggerFactory),
      mock[ParticipantEventPublisher],
      pureCrypto,
      _ => EitherT.rightT(()),
      new PackageInspectionOpsForTesting(submittingParticipant, loggerFactory),
      ProcessingTimeout(),
      loggerFactory,
    )
  private val packageResolver = DAMLe.packageResolver(mockPackageService)

  val damle: DAMLe = new DAMLe(
    packageResolver,
    engine,
    loggerFactory,
  ) {
    override def contractMetadata(
        contractInstance: LfContractInst,
        supersetOfSignatories: Set[LfPartyId],
    )(implicit traceContext: TraceContext) = {
      EitherT.pure[Future, Error](ContractMetadata.tryCreate(Set(party1), Set(party1), None))
    }
  }

  def generateIps(
      topology: Map[ParticipantId, Map[LfPartyId, ParticipantPermission]]
  ): TopologySnapshot =
    TestingTopology()
      .withReversedTopology(topology)
      .build(loggerFactory)
      .topologySnapshot()

  private val cryptoFactory = TestingTopology(domains = Set(sourceDomain, targetDomain))
    .withReversedTopology(
      Map(
        submittingParticipant -> Map(
          party1 -> ParticipantPermission.Submission,
          submittingParticipant.adminParty.toLf -> ParticipantPermission.Submission,
        )
      )
    )
    .build(loggerFactory)

  val cryptoSnapshot =
    cryptoFactory
      .forOwnerAndDomain(submittingParticipant, sourceDomain)
      .currentSnapshotApproximation

  val seedGenerator = new SeedGenerator(pureCrypto)

  private val cantonContractIdVersion =
    CantonContractIdVersion.fromProtocolVersion(testedProtocolVersion)

  private val coordination: TransferCoordination =
    TestTransferCoordination(
      Set(sourceDomain, targetDomain),
      CantonTimestamp.Epoch,
      Some(cryptoSnapshot),
      Some(None),
      loggerFactory,
    )(directExecutionContext)
  val outProcessingSteps =
    new TransferOutProcessingSteps(
      sourceDomain,
      submittingParticipant,
      damle,
      coordination,
      seedGenerator,
      SourceProtocolVersion(testedProtocolVersion),
      loggerFactory,
    )(executorService)

  val participants @ Seq(
    (participant1, admin1),
    (participant2, admin2),
    (participant3, admin3),
    (participant4, admin4),
  ) =
    (1 to 4).map { i =>
      val participant =
        ParticipantId(UniqueIdentifier.tryFromProtoPrimitive(s"participant$i::participant"))
      val admin = participant.adminParty.toLf
      (participant -> admin)
    }

  private val timeEvent =
    TimeProofTestUtil.mkTimeProof(timestamp = CantonTimestamp.Epoch, domainId = targetDomain)

  "createTransferOutRequest" should {
    val ips1 = generateIps(
      Map(
        submittingParticipant -> Map(submitter -> Submission),
        participant1 -> Map(party1 -> Submission),
        participant2 -> Map(party2 -> Submission),
      )
    )

    val contractId = cantonContractIdVersion.fromDiscriminator(
      ExampleTransactionFactory.lfHash(10),
      Unicum(pureCrypto.digest(HashPurpose.MerkleTreeInnerNode, ByteString.copyFromUtf8("unicum"))),
    )

    def mkTxOutRes(
        stakeholders: Set[LfPartyId],
        sourceIps: TopologySnapshot,
        targetIps: TopologySnapshot,
    ) = {
      TransferOutProcessingSteps
        .createTransferOutRequest(
          submittingParticipant,
          timeEvent,
          contractId,
          submitter,
          stakeholders,
          sourceDomain,
          SourceProtocolVersion(testedProtocolVersion),
          sourceMediator,
          targetDomain,
          TargetProtocolVersion(testedProtocolVersion),
          sourceIps,
          targetIps,
          logger,
        )
        .value
        .futureValue
    }

    "fail if submitter is not a stakeholder" in {
      val stakeholders = Set(party1, party2)
      val result = mkTxOutRes(stakeholders, ips1, ips1)
      result should matchPattern { case Left(SubmittingPartyMustBeStakeholder(_, _, _)) =>
      }
    }

    "fail if submitting participant does not have submission permission" in {
      val ipsNoSubmissionPermission =
        generateIps(Map(submittingParticipant -> Map(submitter -> Confirmation)))

      val result = mkTxOutRes(Set(submitter), ipsNoSubmissionPermission, ips1)
      result should matchPattern { case Left(NoSubmissionPermission(_, _, _)) =>
      }
    }

    "fail if a stakeholder cannot submit on target domain" in {
      val ipsNoSubmissionOnTarget = generateIps(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
        )
      )

      val stakeholders = Set(submitter, party1)
      val result = mkTxOutRes(stakeholders, ips1, ipsNoSubmissionOnTarget)
      result should matchPattern { case Left(PermissionErrors(_)) =>
      }
    }

    "fail if a stakeholder cannot confirm on target domain" in {
      val ipsConfirmationOnSource = generateIps(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
        )
      )

      val ipsNoConfirmationOnTarget = generateIps(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Observation),
        )
      )

      val stakeholders = Set(submitter, party1)
      val result = mkTxOutRes(stakeholders, ipsConfirmationOnSource, ipsNoConfirmationOnTarget)
      result should matchPattern { case Left(PermissionErrors(_)) =>
      }
    }

    "fail if a stakeholder is not hosted on the same participant on both domains" in {
      val ipsDifferentParticipant = generateIps(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
          participant2 -> Map(party1 -> Submission),
        )
      )

      val stakeholders = Set(submitter, party1)
      val result = mkTxOutRes(stakeholders, ips1, ipsDifferentParticipant)
      result should matchPattern { case Left(PermissionErrors(_)) =>
      }
    }

    "fail if participant cannot confirm for admin party" in {
      val ipsAdminNoConfirmation = generateIps(
        Map(
          submittingParticipant -> Map(adminSubmitter -> Submission, submitter -> Submission),
          participant1 -> Map(party1 -> Observation),
        )
      )
      val result =
        loggerFactory.suppressWarningsAndErrors(
          mkTxOutRes(Set(submitter, party1), ipsAdminNoConfirmation, ips1)
        )
      result should matchPattern { case Left(PermissionErrors(_)) =>
      }
    }

    "pick the active confirming admin party" in {
      val ipsAdminNoConfirmation = generateIps(
        Map(
          submittingParticipant -> Map(adminSubmitter -> Submission, submitter -> Submission),
          participant1 -> Map(party1 -> Confirmation),
          participant2 -> Map(party1 -> Observation),
        )
      )
      val result =
        loggerFactory.suppressWarningsAndErrors(
          mkTxOutRes(Set(submitter, party1), ipsAdminNoConfirmation, ips1)
        )
      result should matchPattern { case Right(x) =>
      }
    }

    "work if topology constraints are satisfied" in {
      val ipsSource = generateIps(
        Map(
          submittingParticipant -> Map(adminSubmitter -> Submission, submitter -> Submission),
          participant1 -> Map(adminSubmitter -> Observation, submitter -> Confirmation),
          participant2 -> Map(party1 -> Submission),
          participant3 -> Map(party1 -> Submission),
          participant4 -> Map(party1 -> Confirmation),
        )
      )
      val ipsTarget = generateIps(
        Map(
          submittingParticipant -> Map(submitter -> Submission),
          participant1 -> Map(submitter -> Observation),
          participant3 -> Map(party1 -> Submission),
          participant4 -> Map(party1 -> Confirmation),
        )
      )
      val stakeholders = Set(submitter, party1)
      val result = mkTxOutRes(stakeholders, ipsSource, ipsTarget)
      assert(
        result == Right(
          (
            TransferOutRequest(
              submitter = submitter,
              stakeholders = stakeholders,
              adminParties = Set(adminSubmitter, admin3, admin4),
              contractId = contractId,
              sourceDomain = sourceDomain,
              sourceProtocolVersion = SourceProtocolVersion(testedProtocolVersion),
              sourceMediator = sourceMediator,
              targetDomain = targetDomain,
              targetProtocolVersion = TargetProtocolVersion(testedProtocolVersion),
              targetTimeProof = timeEvent,
            ),
            Set(submittingParticipant, participant1, participant2, participant3, participant4),
          )
        )
      )
    }

    "allow admin parties as stakeholders" in {
      val stakeholders = Set(submitter, adminSubmitter, admin1)
      val result = mkTxOutRes(stakeholders, ips1, ips1)
      assert(
        result == Right(
          (
            TransferOutRequest(
              submitter = submitter,
              stakeholders = stakeholders,
              adminParties = Set(adminSubmitter, admin1),
              contractId = contractId,
              sourceDomain = sourceDomain,
              sourceProtocolVersion = SourceProtocolVersion(testedProtocolVersion),
              sourceMediator = sourceMediator,
              targetDomain = targetDomain,
              targetProtocolVersion = TargetProtocolVersion(testedProtocolVersion),
              targetTimeProof = timeEvent,
            ),
            Set(submittingParticipant, participant1),
          )
        )
      )
    }
  }

  "prepare submission" should {
    "succeed without errors" in {
      val state = mkState
      val contractId = ExampleTransactionFactory.suffixedId(10, 0)
      val contract = ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(),
        metadata = ContractMetadata.tryCreate(
          signatories = Set(party1),
          stakeholders = Set(party1),
          maybeKeyWithMaintainers = None,
        ),
      )
      val transactionId = ExampleTransactionFactory.transactionId(1)
      val submissionParam =
        TransferOutProcessingSteps.SubmissionParam(
          party1,
          contractId,
          targetDomain,
          TargetProtocolVersion(testedProtocolVersion),
        )

      for {
        _ <- state.storedContractManager.addPendingContracts(
          RequestCounter(1),
          Seq(WithTransactionId(contract, transactionId)),
        )
        _submissionResult <-
          outProcessingSteps
            .prepareSubmission(
              submissionParam,
              sourceMediator,
              state,
              cryptoSnapshot,
            )
            .valueOrFailShutdown("prepare submission failed")
      } yield succeed
    }

    "check that the target domain is not equal to the source domain" in {
      val state = mkState
      val contractId = ExampleTransactionFactory.suffixedId(10, 0)
      val contract = ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(),
      )
      val transactionId = ExampleTransactionFactory.transactionId(1)
      val submissionParam = TransferOutProcessingSteps.SubmissionParam(
        party1,
        contractId,
        sourceDomain,
        TargetProtocolVersion(testedProtocolVersion),
      )

      for {
        _ <- state.storedContractManager.addPendingContracts(
          RequestCounter(1),
          Seq(WithTransactionId(contract, transactionId)),
        )
        submissionResult <- leftOrFailShutdown(
          outProcessingSteps.prepareSubmission(
            submissionParam,
            sourceMediator,
            state,
            cryptoSnapshot,
          )
        )("prepare submission failed")
      } yield {
        submissionResult should matchPattern { case TargetDomainIsSourceDomain(_, _) =>
        }
      }
    }
  }

  "receive request" should {
    val contractId = ExampleTransactionFactory.suffixedId(10, 0)
    val outRequest = TransferOutRequest(
      party1,
      Set(party1),
      Set(party1),
      contractId,
      sourceDomain,
      SourceProtocolVersion(testedProtocolVersion),
      sourceMediator,
      targetDomain,
      TargetProtocolVersion(testedProtocolVersion),
      timeEvent,
    )
    val outTree = makeFullTransferOutTree(outRequest)
    val encryptedOutRequestF = for {
      encrypted <- encryptTransferOutTree(outTree)
    } yield encrypted

    def checkSuccessful(
        result: outProcessingSteps.CheckActivenessAndWritePendingContracts
    ): Assertion =
      result match {
        case outProcessingSteps.CheckActivenessAndWritePendingContracts(
              activenessSet,
              pendingContracts,
              _pendingDataAndResponseArgs,
            ) =>
          activenessSet shouldBe mkActivenessSet(deact = Set(contractId))
          pendingContracts shouldBe Seq.empty
        case _ => fail()
      }

    "succeed without errors" in {
      for {
        encryptedOutRequest <- encryptedOutRequestF
        envelopes =
          NonEmpty(
            Seq,
            OpenEnvelope(encryptedOutRequest, RecipientsTest.testInstance, testedProtocolVersion),
          )
        decrypted <- valueOrFail(outProcessingSteps.decryptViews(envelopes, cryptoSnapshot))(
          "decrypt request failed"
        )
        result <- valueOrFail(
          outProcessingSteps.computeActivenessSetAndPendingContracts(
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

    "fail if there are not transfer-out requests with the right root hash" in {
      outProcessingSteps.pendingDataAndResponseArgsForMalformedPayloads(
        CantonTimestamp.Epoch,
        RequestCounter(1),
        SequencerCounter(1),
        Seq.empty,
        cryptoSnapshot,
      ) shouldBe Left(ReceivedNoRequests)
    }
  }

  "construct pending data and response" should {
    "succeed without errors" in {
      val state = mkState
      val contractId = ExampleTransactionFactory.suffixedId(10, 0)
      val metadata = ContractMetadata.tryCreate(Set.empty, Set(party1), None)
      val contract = ExampleTransactionFactory.asSerializable(
        contractId,
        contractInstance = ExampleTransactionFactory.contractInstance(),
        metadata = metadata,
      )
      val transactionId = ExampleTransactionFactory.transactionId(1)
      val outRequest = TransferOutRequest(
        party1,
        Set(party1),
        Set(submittingParticipant.adminParty.toLf),
        contractId,
        sourceDomain,
        SourceProtocolVersion(testedProtocolVersion),
        sourceMediator,
        targetDomain,
        TargetProtocolVersion(testedProtocolVersion),
        timeEvent,
      )

      val fullTransferOutTree = makeFullTransferOutTree(outRequest)
      val dataAndResponseArgs = TransferOutProcessingSteps.PendingDataAndResponseArgs(
        fullTransferOutTree,
        Recipients.cc(submittingParticipant),
        CantonTimestamp.Epoch,
        RequestCounter(1),
        SequencerCounter(1),
        cryptoSnapshot,
      )
      for {
        _ <- state.storedContractManager.addPendingContracts(
          RequestCounter(1),
          Seq(WithTransactionId(contract, transactionId)),
        )
        _result <- valueOrFail(
          outProcessingSteps
            .constructPendingDataAndResponse(
              dataAndResponseArgs,
              state.transferCache,
              state.storedContractManager,
              state.causalityLookup,
              Future.successful(ActivenessResult.success),
              Future.unit,
              sourceMediator,
            )
        )("construction of pending data and response failed")
      } yield succeed
    }
  }

  "get commit set and contracts to be stored and event" should {
    "succeed without errors" in {
      val state = mkState
      val contractId = ExampleTransactionFactory.suffixedId(10, 0)
      val contractHash = ExampleTransactionFactory.lfHash(0)
      val transferId = TransferId(sourceDomain, CantonTimestamp.Epoch)
      val rootHash = mock[RootHash]
      when(rootHash.asLedgerTransactionId).thenReturn(LedgerTransactionId.fromString("id1"))
      val transferResult =
        TransferResult.create(
          RequestId(CantonTimestamp.Epoch),
          Set(),
          TransferOutDomainId(sourceDomain),
          Verdict.Approve(testedProtocolVersion),
          testedProtocolVersion,
        )
      for {
        signedResult <- SignedProtocolMessage.tryCreate(
          transferResult,
          cryptoSnapshot,
          testedProtocolVersion,
        )
        deliver: Deliver[OpenEnvelope[SignedProtocolMessage[TransferOutResult]]] = {
          val batch: Batch[OpenEnvelope[SignedProtocolMessage[TransferOutResult]]] =
            Batch.of(testedProtocolVersion, (signedResult, Recipients.cc(submittingParticipant)))
          Deliver.create(
            SequencerCounter(0),
            CantonTimestamp.Epoch,
            sourceDomain,
            Some(MessageId.tryCreate("msg-0")),
            batch,
            testedProtocolVersion,
          )
        }
        signedContent = SignedContent(
          deliver,
          SymbolicCrypto.emptySignature,
          None,
        )
        transferInExclusivity = DynamicDomainParameters
          .defaultValues(testedProtocolVersion)
          .transferExclusivityLimitFor(timeEvent.timestamp)
        pendingOut = PendingTransferOut(
          RequestId(CantonTimestamp.Epoch),
          RequestCounter(1),
          SequencerCounter(1),
          rootHash,
          WithContractHash(contractId, contractHash),
          transferringParticipant = false,
          submitter,
          transferId,
          targetDomain,
          Set(party1),
          Set(party1),
          timeEvent,
          Some(transferInExclusivity),
        )
        _ <- valueOrFail(
          outProcessingSteps
            .getCommitSetAndContractsToBeStoredAndEvent(
              signedContent,
              Right(transferResult),
              pendingOut,
              state.pendingTransferOutSubmissions,
              state.causalityLookup,
              pureCrypto,
            )
        )("get commit set and contract to be stored and event")
      } yield {
        succeed
      }
    }
  }

  def makeFullTransferOutTree(
      request: TransferOutRequest,
      uuid: UUID = new UUID(6L, 7L),
  ): FullTransferOutTree = {
    val seed = seedGenerator.generateSaltSeed()
    request.toFullTransferOutTree(pureCrypto, pureCrypto, seed, uuid)
  }

  def encryptTransferOutTree(
      tree: FullTransferOutTree
  ): Future[EncryptedViewMessage[TransferOutViewType]] =
    EncryptedViewMessageFactory
      .create(TransferOutViewType)(tree, cryptoSnapshot, testedProtocolVersion)(
        implicitly[TraceContext],
        executorService,
      )
      .fold(error => fail(s"Failed to encrypt transfer-out request: $error"), Predef.identity)

  def makeRootHashMessage(
      request: FullTransferOutTree
  ): RootHashMessage[SerializedRootHashMessagePayload] =
    RootHashMessage(
      request.rootHash,
      sourceDomain,
      testedProtocolVersion,
      TransferOutViewType,
      SerializedRootHashMessagePayload.empty,
    )
}
