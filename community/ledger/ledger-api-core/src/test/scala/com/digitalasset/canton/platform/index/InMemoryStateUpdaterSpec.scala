// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import cats.syntax.bifunctor.toBifunctorOps
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.lf.crypto
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.digitalasset.canton.ledger.participant.state.v2.{CompletionInfo, TransactionMeta, Update}
import com.digitalasset.canton.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.index.InMemoryStateUpdater.PrepareResult
import com.digitalasset.canton.platform.index.InMemoryStateUpdaterSpec.{
  Scope,
  anotherMetadataChangedUpdate,
  metadataChangedUpdate,
  offset,
  update1,
  update3,
  update4,
  update5,
  update6,
}
import com.digitalasset.canton.platform.indexer.ha.EndlessReadService.configuration
import com.digitalasset.canton.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadataView
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadataView.PackageMetadata
import com.digitalasset.canton.platform.{DispatcherState, InMemoryState}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, TestEssentials}
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.mockito.{InOrder, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.chaining.*

class InMemoryStateUpdaterSpec
    extends AnyFlatSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with MockitoSugar
    with BaseTest {

  "flow" should "correctly process updates in order" in new Scope {
    runFlow(
      Seq(
        Vector(update1, metadataChangedUpdate) -> 1L,
        Vector(update3, update4) -> 3L,
        Vector(update5) -> 4L,
      )
    )
    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(1L),
      result(3L),
      result(4L),
    )
  }

  "flow" should "not process empty input batches" in new Scope {
    runFlow(
      Seq(
        // Empty input batch should have not effect
        Vector.empty -> 1L,
        Vector(update3) -> 3L,
        Vector(anotherMetadataChangedUpdate) -> 3L,
        Vector(update5) -> 4L,
      )
    )

    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(3L),
      result(3L), // Results in empty batch after processing
      result(4L), // Should still have effect on ledger end updates
    )
  }

  "prepare" should "throw exception for an empty vector" in new Scope {
    an[NoSuchElementException] should be thrownBy {
      InMemoryStateUpdater.prepare(emptyArchiveToMetadata)(Vector.empty, 0L)
    }
  }

  "prepare" should "prepare a batch of a single update" in new Scope {
    InMemoryStateUpdater.prepare(emptyArchiveToMetadata)(
      Vector(update1),
      0L,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(1L),
      0L,
      update1._2.traceContext,
      PackageMetadata(),
    )
  }

  "prepare" should "set last offset and eventSequentialId to last element" in new Scope {
    InMemoryStateUpdater.prepare(emptyArchiveToMetadata)(
      Vector(update1, metadataChangedUpdate),
      6L,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(2L),
      6L,
      metadataChangedUpdate._2.traceContext,
      PackageMetadata(),
    )
  }

  "prepare" should "append package metadata" in new Scope {
    def metadata: DamlLf.Archive => PackageMetadata = {
      case archive if archive.getHash == "00001" => PackageMetadata(templates = Set(templateId))
      case archive if archive.getHash == "00002" => PackageMetadata(templates = Set(templateId2))
      case _ => fail("unexpected archive hash")
    }

    InMemoryStateUpdater.prepare(metadata)(
      Vector(update5, update6),
      0L,
    ) shouldBe PrepareResult(
      Vector(),
      offset(6L),
      0L,
      update6._2.traceContext,
      PackageMetadata(templates = Set(templateId, templateId2)),
    )
  }

  "update" should "update the in-memory state" in new Scope {
    InMemoryStateUpdater.update(inMemoryState, logger)(prepareResult)

    inOrder.verify(packageMetadataView).update(packageMetadata)
    // TODO(i12283) LLP: Unit test contract state event conversion and cache updating

    inOrder
      .verify(inMemoryFanoutBuffer)
      .push(
        tx_accepted_withCompletionDetails_offset,
        tx_accepted_withCompletionDetails,
      )
    inOrder
      .verify(inMemoryFanoutBuffer)
      .push(tx_accepted_withoutCompletionDetails_offset, tx_accepted_withoutCompletionDetails)
    inOrder.verify(inMemoryFanoutBuffer).push(tx_rejected_offset, tx_rejected)

    inOrder.verify(ledgerEndCache).set(lastOffset -> lastEventSeqId)
    inOrder.verify(dispatcher).signalNewHead(lastOffset)
    inOrder
      .verify(submissionTracker)
      .onCompletion(
        tx_accepted_completionDetails.completionStreamResponse -> tx_accepted_submitters
      )

    inOrder
      .verify(submissionTracker)
      .onCompletion(
        tx_rejected_completionDetails.completionStreamResponse -> tx_rejected_submitters
      )

    inOrder.verifyNoMoreInteractions()
  }
}

object InMemoryStateUpdaterSpec {

  import TraceContext.Implicits.Empty.*
  trait Scope extends Matchers with ScalaFutures with MockitoSugar with TestEssentials {
    val templateId = Identifier.assertFromString("noPkgId:Mod:I")
    val templateId2 = Identifier.assertFromString("noPkgId:Mod:I2")

    val emptyArchiveToMetadata: DamlLf.Archive => PackageMetadata = _ => PackageMetadata()
    val cacheUpdates = ArrayBuffer.empty[PrepareResult]
    val cachesUpdateCaptor =
      (v: PrepareResult) => cacheUpdates.addOne(v).pipe(_ => ())

    // TODO(#13019) Avoid the global execution context
    @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
    val inMemoryStateUpdater = InMemoryStateUpdaterFlow(
      2,
      scala.concurrent.ExecutionContext.global,
      scala.concurrent.ExecutionContext.global,
      FiniteDuration(10, "seconds"),
      Metrics.ForTesting,
      logger,
    )(
      prepare = (_, lastEventSequentialId) => result(lastEventSequentialId),
      update = cachesUpdateCaptor,
    )(emptyTraceContext)

    val txLogUpdate1 = Traced(
      TransactionLogUpdate.TransactionAccepted(
        transactionId = "tx1",
        commandId = "",
        workflowId = workflowId,
        effectiveAt = Timestamp.Epoch,
        offset = offset(1L),
        events = Vector(),
        completionDetails = None,
        domainId = None,
      )
    )(emptyTraceContext)

    val ledgerEndCache: MutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches: ContractStateCaches = mock[ContractStateCaches]
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView: StringInterningView = mock[StringInterningView]
    val dispatcherState: DispatcherState = mock[DispatcherState]
    val packageMetadataView: PackageMetadataView = mock[PackageMetadataView]
    val submissionTracker: SubmissionTracker = mock[SubmissionTracker]
    val dispatcher: Dispatcher[Offset] = mock[Dispatcher[Offset]]

    val inOrder: InOrder = inOrder(
      ledgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      packageMetadataView,
      submissionTracker,
      dispatcher,
    )

    when(dispatcherState.getDispatcher).thenReturn(dispatcher)

    // TODO(#13019) Avoid the global execution context
    @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
    val inMemoryState = new InMemoryState(
      ledgerEndCache = ledgerEndCache,
      contractStateCaches = contractStateCaches,
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
      packageMetadataView = packageMetadataView,
      submissionTracker = submissionTracker,
      loggerFactory = loggerFactory,
    )(ExecutionContext.global)

    val loggingContext: LoggingContext = LoggingContext.ForTesting
    val tx_accepted_commandId = "cAccepted"
    val tx_accepted_transactionId = "tAccepted"
    val tx_accepted_submitters: Set[String] = Set("p1", "p2")

    val tx_rejected_transactionId = "tRejected"
    val tx_rejected_submitters: Set[String] = Set("p3", "p4")

    val tx_accepted_completion: Completion = Completion(
      commandId = tx_accepted_commandId,
      applicationId = "appId",
      updateId = tx_accepted_transactionId,
      submissionId = "submissionId",
      actAs = Seq.empty,
    )
    val tx_rejected_completion: Completion =
      tx_accepted_completion.copy(updateId = tx_rejected_transactionId)
    val tx_accepted_completionDetails: TransactionLogUpdate.CompletionDetails =
      TransactionLogUpdate.CompletionDetails(
        completionStreamResponse =
          CompletionStreamResponse(completion = Some(tx_accepted_completion)),
        submitters = tx_accepted_submitters,
      )

    val tx_rejected_completionDetails: TransactionLogUpdate.CompletionDetails =
      TransactionLogUpdate.CompletionDetails(
        completionStreamResponse =
          CompletionStreamResponse(completion = Some(tx_rejected_completion)),
        submitters = tx_rejected_submitters,
      )

    val tx_accepted_withCompletionDetails_offset: Offset =
      Offset.fromHexString(Ref.HexString.assertFromString("aaaa"))

    val tx_accepted_withoutCompletionDetails_offset: Offset =
      Offset.fromHexString(Ref.HexString.assertFromString("bbbb"))

    val tx_rejected_offset: Offset =
      Offset.fromHexString(Ref.HexString.assertFromString("cccc"))

    val tx_accepted_withCompletionDetails: TransactionLogUpdate.TransactionAccepted =
      TransactionLogUpdate.TransactionAccepted(
        transactionId = tx_accepted_transactionId,
        commandId = tx_accepted_commandId,
        workflowId = "wAccepted",
        effectiveAt = Timestamp.assertFromLong(1L),
        offset = tx_accepted_withCompletionDetails_offset,
        events = (1 to 3).map(_ => mock[TransactionLogUpdate.Event]).toVector,
        completionDetails = Some(tx_accepted_completionDetails),
        domainId = None,
      )

    val tx_accepted_withoutCompletionDetails: TransactionLogUpdate.TransactionAccepted =
      tx_accepted_withCompletionDetails.copy(
        completionDetails = None,
        offset = tx_accepted_withoutCompletionDetails_offset,
      )

    val tx_rejected: TransactionLogUpdate.TransactionRejected =
      TransactionLogUpdate.TransactionRejected(
        offset = tx_rejected_offset,
        completionDetails = tx_rejected_completionDetails,
      )
    val packageMetadata: PackageMetadata = PackageMetadata(templates = Set(templateId))

    val lastOffset: Offset = tx_rejected_offset
    val lastEventSeqId = 123L
    val updates: Vector[Traced[TransactionLogUpdate]] =
      Vector(
        Traced(tx_accepted_withCompletionDetails)(emptyTraceContext),
        Traced(tx_accepted_withoutCompletionDetails)(emptyTraceContext),
        Traced(tx_rejected)(emptyTraceContext),
      )
    val prepareResult: PrepareResult = PrepareResult(
      updates = updates,
      lastOffset = lastOffset,
      lastEventSequentialId = lastEventSeqId,
      emptyTraceContext,
      packageMetadata = packageMetadata,
    )

    def result(lastEventSequentialId: Long): PrepareResult =
      PrepareResult(
        Vector.empty,
        offset(1L),
        lastEventSequentialId,
        emptyTraceContext,
        PackageMetadata(),
      )

    def runFlow(
        input: Seq[(Vector[(Offset, Traced[Update])], Long)]
    )(implicit mat: Materializer): Done =
      Source(input)
        .via(inMemoryStateUpdater)
        .runWith(Sink.ignore)
        .futureValue
  }

  private val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("EndlessReadServiceParticipant")

  private val txId1 = Ref.TransactionId.assertFromString("tx1")
  private val txId2 = Ref.TransactionId.assertFromString("tx2")

  private val someSubmissionId: Ref.SubmissionId =
    Ref.SubmissionId.assertFromString("some submission id")
  private val workflowId: Ref.WorkflowId = Ref.WorkflowId.assertFromString("Workflow")
  private val someTransactionMeta: TransactionMeta = TransactionMeta(
    ledgerEffectiveTime = Timestamp.Epoch,
    workflowId = Some(workflowId),
    submissionTime = Timestamp.Epoch,
    submissionSeed = crypto.Hash.hashPrivateKey("SomeTxMeta"),
    optUsedPackages = None,
    optNodeSeeds = None,
    optByKeyNodes = None,
  )

  private val update1 = offset(1L) -> Traced(
    Update.TransactionAccepted(
      optCompletionInfo = None,
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(TransactionBuilder.Empty),
      transactionId = txId1,
      recordTime = Timestamp.Epoch,
      divulgedContracts = List.empty,
      blindingInfo = None,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
    )
  )
  private val rawMetadataChangedUpdate = offset(2L) -> Update.ConfigurationChanged(
    Timestamp.Epoch,
    someSubmissionId,
    participantId,
    configuration,
  )
  private val metadataChangedUpdate = rawMetadataChangedUpdate.bimap(identity, Traced[Update])
  private val update3 = offset(3L) -> Traced[Update](
    Update.TransactionAccepted(
      optCompletionInfo = None,
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(TransactionBuilder.Empty),
      transactionId = txId2,
      recordTime = Timestamp.Epoch,
      divulgedContracts = List.empty,
      blindingInfo = None,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
    )
  )
  private val update4 = offset(4L) -> Traced[Update](
    Update.CommandRejected(
      recordTime = Time.Timestamp.assertFromLong(1337L),
      completionInfo = CompletionInfo(
        actAs = List.empty,
        applicationId = Ref.ApplicationId.assertFromString("some-app-id"),
        commandId = Ref.CommandId.assertFromString("cmdId"),
        optDeduplicationPeriod = None,
        submissionId = None,
        statistics = None,
      ),
      reasonTemplate = FinalReason(new Status()),
    )
  )
  private val archive = DamlLf.Archive.newBuilder
    .setHash("00001")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 1"))
    .build

  private val archive2 = DamlLf.Archive.newBuilder
    .setHash("00002")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 2"))
    .build

  private val update5 = offset(5L) -> Traced[Update](
    Update.PublicPackageUpload(
      archives = List(archive),
      sourceDescription = None,
      recordTime = Timestamp.Epoch,
      submissionId = None,
    )
  )

  private val update6 = offset(6L) -> Traced[Update](
    Update.PublicPackageUpload(
      archives = List(archive2),
      sourceDescription = None,
      recordTime = Timestamp.Epoch,
      submissionId = None,
    )
  )

  private val anotherMetadataChangedUpdate =
    rawMetadataChangedUpdate
      .bimap(
        Function.const(offset(5L)),
        _.copy(recordTime = Time.Timestamp.assertFromLong(1337L)),
      )
      .bimap(identity, Traced[Update](_))

  private def offset(idx: Long): Offset = {
    val base = BigInt(1) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}
