// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, RequireTypes}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  NegativeStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.TopologyStore.{
  EffectiveStateChange,
  StateKeyFetch,
  TopologyStoreDeactivations,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.ExecutionContext

class TopologyStoreDedupTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  "copyFromPredecessorSynchronizerStore" should {
    "dedup concurrent copies so the underlying copy runs only once" in {
      val source = new DummyCopyTopologyStore(predecessor = None, loggerFactory)
      val predecessor = SynchronizerPredecessor(
        source.storeId.psid,
        CantonTimestamp.Epoch,
        isLateUpgrade = false,
      )
      val target = new DummyCopyTopologyStore(predecessor = Some(predecessor), loggerFactory)

      val concurrent =
        Seq.fill(5)(target.copyFromPredecessorSynchronizerStore(source))
      target.release()

      for {
        _ <- FutureUnlessShutdown.sequence(concurrent)
        _ = target.invocations.get() shouldBe 1L
        // once completed, a subsequent call triggers a fresh copy
        next = target.copyFromPredecessorSynchronizerStore(source)
        _ = target.release()
        _ <- next
        _ = target.invocations.get() shouldBe 2L
      } yield succeed
    }
  }
}

/** Minimal [[TopologyStore]] used to exercise the dedup logic in
  * [[TopologyStore.copyFromPredecessorSynchronizerStore]]. Only
  * `doCopyFromPredecessorSynchronizerStore` is implemented — it records invocations and returns the
  * future of a controllable promise so the test can release in-flight copies deterministically.
  */
private final class DummyCopyTopologyStore(
    override val predecessor: Option[SynchronizerPredecessor],
    val loggerFactory: com.digitalasset.canton.logging.NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStore[SynchronizerStore]
    with NamedLogging {

  private val inFlight: AtomicBoolean = new AtomicBoolean(false)
  private val pending =
    new java.util.concurrent.atomic.AtomicReference(PromiseUnlessShutdown.unsupervised[Unit]())
  val invocations: AtomicLong = new AtomicLong(0L)

  /** Complete the currently pending copy and arm a fresh promise for the next one. */
  def release(): Unit = {
    val p = pending.getAndSet(PromiseUnlessShutdown.unsupervised[Unit]())
    p.outcome_(())
  }

  override val storeId: SynchronizerStore = SynchronizerStore(
    PhysicalSynchronizerId(
      SynchronizerId(
        UniqueIdentifier.tryCreate(
          "dummy",
          Namespace(com.digitalasset.canton.crypto.Fingerprint.tryFromString("dummy")),
        )
      ),
      RequireTypes.NonNegativeInt.zero,
      BaseTest.testedProtocolVersion,
    )
  )
  override val protocolVersion: ProtocolVersion = BaseTest.testedProtocolVersion
  override val timeouts: com.digitalasset.canton.config.ProcessingTimeout =
    DefaultProcessingTimeouts.testing
  override protected def onClosed(): Unit = ()

  override protected def doCopyFromPredecessorSynchronizerStore(
      sourceStore: TopologyStore[SynchronizerStore]
  )(implicit
      ev: SynchronizerStore <:< SynchronizerStore,
      errorLoggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[Unit] = {
    ErrorUtil.requireState(inFlight.compareAndSet(false, true), "concurrent execution detected")(
      errorLoggingContext
    )
    invocations.incrementAndGet().discard
    pending.get().futureUS.map { _ =>
      inFlight.set(false)
    }
  }

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyStore.Change]] = ???
  override def maxTimestamp(sequencedTime: SequencedTime, includeRejected: Boolean)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] = ???
  override def latestTopologyChangeTimestamp()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] = ???
  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = ???
  override def updateDispatchingWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = ???
  override def findLatestTransactionsAndProposalsByTxHash(hashes: Set[TxHash])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = ???
  override def findTransactionsForMapping(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[MappingHash]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = ???
  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
      pagination: Option[(Option[UniqueIdentifier], Int)],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[PositiveStoredTopologyTransactions] = ???
  override def findNegativeTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
      pagination: Option[(Option[UniqueIdentifier], Int)],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[NegativeStoredTopologyTransactions] = ???
  override def findAllTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
      pagination: Option[(Option[UniqueIdentifier], Int)],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] = ???
  override def fetchAllDescending(items: Seq[StateKeyFetch])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = ???
  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removals: TopologyStoreDeactivations,
      additions: Seq[GenericValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = ???
  override def bulkInsert(initialSnapshot: GenericStoredTopologyTransactions)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = ???
  override protected[topology] def dumpStoreContent()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = ???
  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQuery,
      asOfExclusiveO: Option[CantonTimestamp],
      op: Option[TopologyChangeOp],
      types: Seq[TopologyMapping.Code],
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] = ???
  override def inspectKnownParties(
      asOfExclusive: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] = ???
  override def findFirstSequencerStateForSequencer(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, SequencerSynchronizerState]]
  ] = ???
  override def findFirstMediatorStateForMediator(mediatorId: MediatorId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState]]
  ] = ???
  override def findFirstTrustCertificateForParticipant(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, SynchronizerTrustCertificate]]
  ] = ???
  override def findEssentialStateAtSequencedTime(
      asOfInclusive: SequencedTime,
      includeRejected: Boolean,
  )(implicit traceContext: TraceContext): Source[GenericStoredTopologyTransaction, NotUsed] = ???
  override def findEssentialStateHashAtSequencedTime(asOfInclusive: SequencedTime)(implicit
      materializer: Materializer,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Hash] = ???
  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = ???
  override def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = ???
  override def findStoredForVersion(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransaction,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] = ???
  override def findStored(
      asOfExclusive: CantonTimestamp,
      transaction: GenericSignedTopologyTransaction,
      includeRejected: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] = ???
  override def findEffectiveStateChanges(
      fromEffectiveInclusive: CantonTimestamp,
      filterTypes: Option[Seq[TopologyMapping.Code]],
      onlyAtEffective: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[EffectiveStateChange]] = ???
  override def deleteAllData()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = ???

}
