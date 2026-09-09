// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.data.{
  CantonTimestamp,
  DeduplicationPeriod,
  ReassignmentSubmitterMetadata,
  TransactionViewLimitConfig,
}
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentProcessor,
  UnassignmentProcessingSteps,
  UnassignmentProcessor,
  UnassignmentProcessorError,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory.TransactionConfirmationRequestCreationError
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.ContractInstanceOfId
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  InformeeMessage,
  TransactionConfirmationRequest,
}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  ExampleContractFactory,
  ExampleTransactionFactory,
  WellFormedTransaction,
}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MediatorGroupRecipient,
  SequencingSubmissionCost,
}
import com.digitalasset.canton.sequencing.traffic.TrafficStateController
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ParticipantId,
  PhysicalSynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPartyId,
  ReassignmentCounter,
  WorkflowId,
  config,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.IdString
import com.digitalasset.nonempty.NonEmpty
import org.mockito.captor.ArgCaptor
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.ExecutionContext

class TrafficCostEstimatorTest extends FixtureAnyWordSpec with BaseTest with HasExecutionContext {

  val alice: IdString.Party = LfPartyId.assertFromString("Alice")
  private val psid: PhysicalSynchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private val participantId: ParticipantId = DefaultTestIdentities.participant1
  private val snapshotTimestamp: CantonTimestamp = CantonTimestamp.ofEpochSecond(42)

  val contract = ExampleContractFactory.build()
  val contractId = contract.contractId

  final class Env(
      val estimator: TrafficCostEstimator,
      val snapshot: TopologySnapshot,
      val requestFactory: TransactionConfirmationRequestFactory,
      val trafficStateController: TrafficStateController,
      val cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi,
      val unassignmentProcessor: UnassignmentProcessor,
      val assignmentProcessor: AssignmentProcessor,
  ) {

    def trafficControlIs(params: Option[TrafficControlParameters]): Unit =
      when(
        snapshot.trafficControlParameters(eqTo(psid.protocolVersion), any[Boolean])(
          any[TraceContext]
        )
      ).thenReturn(FutureUnlessShutdown.pure(params))

    def stubEnabledPath(): Unit = {
      when(
        requestFactory.createConfirmationRequest(
          any[WellFormedTransaction[WithoutSuffixes]],
          any[SubmitterInfo],
          any[Option[WorkflowId]],
          any[MediatorGroupRecipient],
          any[SynchronizerSnapshotSyncCryptoApi],
          any[CantonTimestamp],
          any[SessionKeyStore],
          any[ContractInstanceOfId],
          any[CantonTimestamp],
          any[ProtocolVersion],
          any[TransactionViewLimitConfig],
        )(any[TraceContext])
      ).thenReturn(
        EitherT.rightT[FutureUnlessShutdown, TransactionConfirmationRequestCreationError](
          confirmationRequest
        )
      )

      when(
        unassignmentProcessor.buildSubmissionBatch(
          any[UnassignmentProcessingSteps.SubmissionParam],
          any[MediatorGroupRecipient],
          any[SynchronizerSnapshotSyncCryptoApi],
        )(any[TraceContext])
      ).thenReturn(
        EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
          Batch.empty[DefaultOpenEnvelope](testedProtocolVersion)
        )
      )

      when(
        assignmentProcessor.buildSubmissionBatchForCostEstimation(
          any[ReassignmentSubmitterMetadata],
          any[Seq[ContractInstance]],
          any[Source[PhysicalSynchronizerId]],
          any[Source[TopologySnapshot]],
          any[MediatorGroupRecipient],
          any[SynchronizerSnapshotSyncCryptoApi],
          any[CantonTimestamp],
          any[ReassignmentCounter],
        )(any[TraceContext])
      ).thenReturn(
        EitherT.rightT[FutureUnlessShutdown, ReassignmentProcessorError](
          Batch.empty[DefaultOpenEnvelope](testedProtocolVersion)
        )
      )

      when(snapshot.activeParticipantsOfAll(any[List[LfPartyId]])(any[TraceContext]))
        .thenReturn(EitherT.rightT[FutureUnlessShutdown, Set[LfPartyId]](Set(participantId)))

      when(snapshot.activeParticipantsOfParties(any[Seq[LfPartyId]])(any[TraceContext]))
        .thenReturn(FutureUnlessShutdown.pure(Map.empty[LfPartyId, Set[ParticipantId]]))

      when(
        trafficStateController.computeCost(
          any[Batch[DefaultOpenEnvelope]],
          any[TopologySnapshot],
          any[Boolean],
        )(any[ExecutionContext], any[TraceContext])
      ).thenReturn(
        FutureUnlessShutdown.pure(
          Some(SequencingSubmissionCost(stubbedRequestCost, testedProtocolVersion))
        ),
        FutureUnlessShutdown.pure(
          Some(SequencingSubmissionCost(stubbedResponseCost, testedProtocolVersion))
        ),
      )

      when(cryptoSnapshot.pureCrypto).thenReturn(
        new SynchronizerCryptoPureApi(defaultStaticSynchronizerParameters, new SymbolicPureCrypto())
      )

      when(
        cryptoSnapshot.sign(
          any[Hash],
          any[NonEmpty[Set[SigningKeyUsage]]],
          any[Option[SigningTimestampOverrides]],
        )(any[TraceContext])
      ).thenReturn(
        EitherT.rightT[FutureUnlessShutdown, SyncCryptoError](SymbolicCrypto.emptySignature)
      )
    }

    def stubBatchBuildFailure(error: ReassignmentProcessorError): Unit = {
      when(
        unassignmentProcessor.buildSubmissionBatch(
          any[UnassignmentProcessingSteps.SubmissionParam],
          any[MediatorGroupRecipient],
          any[SynchronizerSnapshotSyncCryptoApi],
        )(any[TraceContext])
      ).thenReturn(
        EitherT.leftT[FutureUnlessShutdown, Batch[DefaultOpenEnvelope]](error)
      )

      when(
        assignmentProcessor.buildSubmissionBatchForCostEstimation(
          any[ReassignmentSubmitterMetadata],
          any[Seq[ContractInstance]],
          any[Source[PhysicalSynchronizerId]],
          any[Source[TopologySnapshot]],
          any[MediatorGroupRecipient],
          any[SynchronizerSnapshotSyncCryptoApi],
          any[CantonTimestamp],
          any[ReassignmentCounter],
        )(any[TraceContext])
      ).thenReturn(
        EitherT.leftT[FutureUnlessShutdown, Batch[DefaultOpenEnvelope]](error)
      )
    }

    def estimateUnassignment(): Either[String, (NonNegativeLong, NonNegativeLong)] =
      estimator
        .estimateUnassignmentCost(
          submitter = alice,
          contractIds = Seq(contractId),
          targetSynchronizer = Target(psid),
          submitterInfo = submitterInfo,
          signatories = Seq(alice),
        )(TraceContext.empty)
        .value
        .failOnShutdown
        .futureValue

    def estimateAssignment(): Either[String, (NonNegativeLong, NonNegativeLong)] =
      estimator
        .estimateAssignmentCost(
          submitter = alice,
          contracts = Seq(contract),
          sourceSynchronizer = Source(psid),
          sourceSnapshot = Source(snapshot),
          submitterInfo = submitterInfo,
          signatories = Seq(alice),
        )(TraceContext.empty)
        .value
        .failOnShutdown
        .futureValue
  }

  override type FixtureParam = Env

  private val stubbedRequestCost = NonNegativeLong.tryCreate(1234L)
  private val stubbedResponseCost = NonNegativeLong.tryCreate(4321L)

  private val confirmationRequest: TransactionConfirmationRequest = {
    val example = new ExampleTransactionFactory()().standardHappyCases.head
    TransactionConfirmationRequest(
      InformeeMessage(example.fullInformeeTree, SymbolicCrypto.emptySignature)(
        testedProtocolVersion
      ),
      viewEnvelopes = Seq.empty,
      protocolVersion = testedProtocolVersion,
    )
  }

  private val submitterInfo: SubmitterInfo = SubmitterInfo(
    actAs = List.empty,
    readAs = List.empty,
    userId = Ref.UserId.assertFromString("traffic-cost-estimator-spec"),
    commandId = Ref.CommandId.assertFromString("command-id"),
    deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ZERO),
    submissionId = None,
    externallySignedSubmission = None,
    transactionHash = None,
  )

  override def withFixture(test: OneArgTest): Outcome = {
    val snapshot = mock[TopologySnapshot]
    when(snapshot.timestamp).thenReturn(snapshotTimestamp)

    val topologyClient = mock[SynchronizerTopologyClientWithInit]
    when(topologyClient.headSnapshot(any[TraceContext])).thenReturn(snapshot)

    // The estimator builds this eagerly, before the traffic control check, so it has to be
    // stubbed even for the tests where traffic control is disabled.
    val cryptoSnapshot = mock[SynchronizerSnapshotSyncCryptoApi]
    val synchronizerCrypto = mock[SynchronizerCryptoClient]
    when(
      synchronizerCrypto.createWithCustomCryptoSigner(
        any[TopologySnapshot],
        any[SyncCryptoSigner => SyncCryptoSigner],
      )
    )
      .thenReturn(cryptoSnapshot)

    val requestFactory = mock[TransactionConfirmationRequestFactory]
    val trafficStateController = mock[TrafficStateController]
    val unassignmentProcessor = mock[UnassignmentProcessor]
    val assignmentProcessor = mock[AssignmentProcessor]

    val estimator = new TrafficCostEstimator(
      confirmationRequestFactory = requestFactory,
      topologyClient = topologyClient,
      synchronizerCrypto = synchronizerCrypto,
      contractStore = mock[ContractStore],
      sessionKeyStore = mock[SessionKeyStore],
      psid = psid,
      participantId = participantId,
      trafficStateController = trafficStateController,
      defaultMaxSequencingTimeOffset = config.NonNegativeFiniteDuration.ofSeconds(30),
      clock = new SimClock(loggerFactory = loggerFactory),
      unassignmentProcessor = unassignmentProcessor,
      assignmentProcessor = assignmentProcessor,
      loggerFactory = loggerFactory,
    )

    withFixture(
      test.toNoArgTest(
        new Env(
          estimator,
          snapshot,
          requestFactory,
          trafficStateController,
          cryptoSnapshot,
          unassignmentProcessor,
          assignmentProcessor,
        )
      )
    )
  }

  def basicBehavior(estimation: Env => Either[String, (NonNegativeLong, NonNegativeLong)]): Unit = {
    "report zero cost when traffic control is disabled" in { env =>
      import env.*
      trafficControlIs(None)

      estimation(env).value shouldBe (NonNegativeLong.zero, NonNegativeLong.zero)

      // Avoid expensive work
      verifyZeroInteractions(requestFactory)
    }

    "return the estimated cost when traffic control is enabled and confirmations free" in { env =>
      import env.*
      trafficControlIs(Some(TrafficControlParameters(freeConfirmationResponses = true)))
      stubEnabledPath()

      estimation(env).value shouldBe (stubbedRequestCost, NonNegativeLong.zero)
    }

    "charge for the confirmation response when responses are not free" in { env =>
      import env.*
      trafficControlIs(Some(TrafficControlParameters(freeConfirmationResponses = false)))
      stubEnabledPath()

      estimation(env).value shouldBe (stubbedRequestCost, stubbedResponseCost)
    }

    "fail when the request cannot be built" in { env =>
      import env.*
      trafficControlIs(Some(TrafficControlParameters()))
      stubEnabledPath()
      stubBatchBuildFailure(UnassignmentProcessorError.UnknownContract(contractId))

      estimation(env).left.value should include("UnknownContract")
    }

    "derive the confirming parties from the signatories" in { env =>
      import env.*
      trafficControlIs(Some(TrafficControlParameters(freeConfirmationResponses = false)))
      stubEnabledPath()

      estimation(env).value

      val parties = ArgCaptor[Seq[LfPartyId]]
      verify(snapshot).activeParticipantsOfParties(parties.capture)(any[TraceContext])
      parties.value shouldBe Seq(alice)
    }
  }

  "estimateUnassignmentCost" should {
    behave like basicBehavior(_.estimateUnassignment())

    "estimate the unassignment the caller asked for" in { env =>
      import env.*
      trafficControlIs(Some(TrafficControlParameters(freeConfirmationResponses = true)))
      stubEnabledPath()

      estimateUnassignment().value

      val param = ArgCaptor[UnassignmentProcessingSteps.SubmissionParam]
      verify(unassignmentProcessor).buildSubmissionBatch(
        param.capture,
        any[MediatorGroupRecipient],
        any[SynchronizerSnapshotSyncCryptoApi],
      )(any[TraceContext])

      param.value.contractIds shouldBe Seq(contractId)
      param.value.targetSynchronizer shouldBe Target(psid)
      param.value.submitterMetadata.submitter shouldBe alice
      param.value.submitterMetadata.submittingParticipant shouldBe participantId
    }

  }

  "estimateAssignmentCost" should {
    behave like basicBehavior(_.estimateAssignment())

    "estimate the assignment the caller asked for" in { env =>
      import env.*
      trafficControlIs(Some(TrafficControlParameters(freeConfirmationResponses = true)))
      stubEnabledPath()

      estimateAssignment().value

      val param = ArgCaptor[ReassignmentSubmitterMetadata]
      val contracts = ArgCaptor[Seq[ContractInstance]]
      val sourceSynchronizer = ArgCaptor[Source[PhysicalSynchronizerId]]
      val sourceSnapshot = ArgCaptor[Source[TopologySnapshot]]

      verify(assignmentProcessor).buildSubmissionBatchForCostEstimation(
        param.capture,
        contracts.capture,
        sourceSynchronizer.capture,
        sourceSnapshot.capture,
        any[MediatorGroupRecipient],
        any[SynchronizerSnapshotSyncCryptoApi],
        any[CantonTimestamp],
        any[ReassignmentCounter],
      )(any[TraceContext])

      param.value.submitter shouldBe alice
      param.value.submittingParticipant shouldBe participantId
      contracts.value shouldBe Seq(contract)
      sourceSynchronizer.value shouldBe Source(psid)
      sourceSnapshot.value shouldBe Source(snapshot)
    }
  }
}
