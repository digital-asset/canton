// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.DecryptionError.FailedToDecrypt
import com.digitalasset.canton.crypto.SyncCryptoError.SyncCryptoDecryptionError
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, Hash, HashOps}
import com.digitalasset.canton.data.{CantonTimestamp, Informee, ViewTree, ViewType}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  DomainId,
  MediatorId,
  Member,
  ParticipantId,
}
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  PendingRequestData,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  NoMediatorError,
  PendingRequestDataOrReplayData,
}
import com.digitalasset.canton.participant.protocol.TestProcessingSteps.{
  TestPendingRequestData,
  TestProcessingError,
  TestProcessorError,
  TestViewTree,
  TestViewType,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessResult,
  ActivenessSet,
}
import com.digitalasset.canton.participant.store.{
  ContractLookup,
  SyncDomainEphemeralState,
  SyncDomainEphemeralStateLookup,
  TransferLookup,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.SyncCryptoDecryptError
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{LfContractId, RequestId, RootHash, ViewHash, v0}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import com.google.protobuf.ByteString

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}

class TestProcessingSteps(
    pendingRequestMap: concurrent.Map[RequestId, PendingRequestDataOrReplayData[
      TestPendingRequestData
    ]],
    pendingSubmissionMap: concurrent.Map[Int, Unit],
    pendingRequestData: Option[TestPendingRequestData],
)(implicit val ec: ExecutionContext)
    extends ProcessingSteps[
      Int,
      Unit,
      TestViewType,
      TransactionResultMessage,
      TestProcessingError,
    ]
    with BaseTest {
  override type PendingRequestData = TestPendingRequestData
  override type SubmissionResultArgs = Unit
  override type PendingDataAndResponseArgs = Unit
  override type RejectionArgs = Unit
  override type PendingSubmissions = concurrent.Map[Int, Unit]
  override type PendingSubmissionId = Int
  override type PendingSubmissionData = Unit
  override type SubmissionSendError = TestProcessingError
  override type RequestError = TestProcessingError
  override type ResultError = TestProcessingError

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): TestProcessingError =
    TestProcessorError(err)

  override def embedResultError(err: ProtocolProcessor.ResultProcessingError): TestProcessingError =
    TestProcessorError(err)

  override def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions =
    pendingSubmissionMap

  override def submissionIdOfPendingRequest(pendingData: TestPendingRequestData): Int = 0

  override def removePendingSubmission(
      pendingSubmissions: concurrent.Map[Int, Unit],
      pendingSubmissionId: Int,
  ): Option[Unit] =
    pendingSubmissions.remove(pendingSubmissionId)

  override def pendingRequestMap
      : SyncDomainEphemeralState => concurrent.Map[RequestId, PendingRequestDataOrReplayData[
        PendingRequestData
      ]] =
    x => pendingRequestMap

  override def requestKind: String = "test"

  override def submissionDescription(param: Int): String = s"submission $param"

  override def embedNoMediatorError(error: NoMediatorError): TestProcessingError =
    TestProcessorError(error)

  override def prepareSubmission(
      param: Int,
      mediatorId: MediatorId,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TestProcessingError, Submission] = {
    val envelope: ProtocolMessage = mock[ProtocolMessage]
    val recipient: Member = ParticipantId("participant1")
    EitherT.rightT(new UntrackedSubmission {
      override def batch: Batch[DefaultOpenEnvelope] =
        Batch.of((envelope, Recipients.cc(recipient)))
      override def pendingSubmissionId: Int = param
      override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.none

      override def embedSubmissionError(
          err: ProtocolProcessor.SubmissionProcessingError
      ): TestProcessingError =
        TestProcessorError(err)
      override def toSubmissionError(err: TestProcessingError): TestProcessingError = err
    })
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: concurrent.Map[Int, Unit],
      submissionParam: Int,
      pendingSubmissionId: Int,
  ): EitherT[Future, TestProcessingError, SubmissionResultArgs] = {
    pendingSubmissionMap.put(submissionParam, ())
    EitherT.pure(())
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      submissionResultArgs: SubmissionResultArgs,
  ): Unit =
    ()

  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TestViewType]]]],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit traceContext: TraceContext): EitherT[Future, TestProcessingError, DecryptedViews] = {
    def treeFor(viewHash: ViewHash, hash: Hash): TestViewTree = {
      val rootHash = RootHash(hash)
      TestViewTree(viewHash, rootHash)
    }

    val decryptedViewTrees = batch.map { envelope =>
      Hash
        .fromByteString(envelope.protocolMessage.encryptedView.viewTree.ciphertext)
        .bimap(
          err =>
            SyncCryptoDecryptError(
              SyncCryptoDecryptionError(FailedToDecrypt(err.toString)),
              envelope.protocolMessage,
            ),
          hash =>
            WithRecipients(treeFor(envelope.protocolMessage.viewHash, hash), envelope.recipients),
        )
    }
    EitherT.rightT(DecryptedViews(decryptedViewTrees.toList))
  }

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmpty[Seq[WithRecipients[TestViewTree]]],
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TestProcessingError, CheckActivenessAndWritePendingContracts] = {
    val res = CheckActivenessAndWritePendingContracts(ActivenessSet.empty, Seq.empty, ())
    EitherT.rightT(res)
  }

  override def pendingDataAndResponseArgsForMalformedPayloads(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
  ): Either[TestProcessingError, Unit] = Right(())

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      contractLookup: ContractLookup,
      tracker: SingleDomainCausalTracker,
      activenessResultFuture: Future[ActivenessResult],
      pendingCursor: Future[Unit],
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TestProcessingError, StorePendingDataAndSendResponseAndCreateTimeout] = {
    val res = StorePendingDataAndSendResponseAndCreateTimeout(
      pendingRequestData.getOrElse(TestPendingRequestData(0L, 0L, Set.empty)),
      Seq.empty,
      List.empty,
      (),
    )
    EitherT.rightT(res)
  }

  override def eventAndSubmissionIdForInactiveMediator(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmpty[Seq[WithRecipients[DecryptedView]]],
  )(implicit traceContext: TraceContext): (Option[TimestampedEvent], Option[PendingSubmissionId]) =
    (None, None)

  override def createRejectionEvent(rejectionArgs: Unit)(implicit
      traceContext: TraceContext
  ): Either[TestProcessingError, Option[TimestampedEvent]] =
    Right(None)

  override def getCommitSetAndContractsToBeStoredAndEvent(
      event: SignedContent[Deliver[DefaultOpenEnvelope]],
      result: Either[MalformedMediatorRequestResult, TransactionResultMessage],
      pendingRequestData: PendingRequestData,
      pendingSubmissionMap: PendingSubmissions,
      tracker: SingleDomainCausalTracker,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TestProcessingError, CommitAndStoreContractsAndPublishEvent] = {
    val result = CommitAndStoreContractsAndPublishEvent(None, Set.empty, None, None)
    EitherT.pure[Future, TestProcessingError](result)
  }

  override def postProcessSubmissionForInactiveMediator(
      declaredMediator: MediatorId,
      timestamp: CantonTimestamp,
      pendingSubmission: Unit,
  )(implicit traceContext: TraceContext): Unit = ()

  override def postProcessResult(verdict: Verdict, pendingSubmissionO: Unit)(implicit
      traceContext: TraceContext
  ): Unit = ()
}

object TestProcessingSteps {

  case class TestViewTree(
      viewHash: ViewHash,
      rootHash: RootHash,
      informees: Set[Informee] = Set.empty,
      domainId: DomainId = DefaultTestIdentities.domainId,
      mediatorId: MediatorId = DefaultTestIdentities.mediator,
  ) extends ViewTree
      with HasVersionedToByteString {
    def toBeSigned: Option[RootHash] = None
    override def pretty: Pretty[TestViewTree] = adHocPrettyInstance
    override def toByteString(version: ProtocolVersion): ByteString =
      throw new UnsupportedOperationException("TestViewTree cannot be serialized")
  }

  case object TestViewType extends ViewType {
    override type View = TestViewTree

    override def toProtoEnum: v0.ViewType =
      throw new UnsupportedOperationException("TestViewType cannot be serialized")
  }
  type TestViewType = TestViewType.type

  case class TestPendingRequestData(
      requestCounter: RequestCounter,
      requestSequencerCounter: SequencerCounter,
      pendingContracts: Set[LfContractId],
  ) extends PendingRequestData

  sealed trait TestProcessingError extends WrapsProcessorError

  case class TestProcessorError(err: ProtocolProcessor.ProcessorError) extends TestProcessingError {
    override def underlyingProcessorError(): Option[ProtocolProcessor.ProcessorError] = Some(err)
  }

}
