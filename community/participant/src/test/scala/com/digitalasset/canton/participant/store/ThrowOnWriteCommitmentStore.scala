// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.{DiscardOps, LfPartyId}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.protocol.messages.AcsCommitment.CommitmentType
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  SignedProtocolMessage,
}
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}

class ThrowOnWriteCommitmentStore()(override implicit val ec: ExecutionContext)
    extends AcsCommitmentStore {
  // Counts the number of write method invocations
  val writeCounter = new AtomicInteger(0)

  override def storeComputed(
      period: CommitmentPeriod,
      counterParticipant: ParticipantId,
      commitment: CommitmentType,
  )(implicit traceContext: TraceContext): Future[Unit] =
    incrementCounterAndErrF()

  override def markOutstanding(period: CommitmentPeriod, counterParticipants: Set[ParticipantId])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    incrementCounterAndErrF()

  override def markComputedAndSent(period: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    incrementCounterAndErrF()

  override def storeReceived(commitment: SignedProtocolMessage[AcsCommitment])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    incrementCounterAndErrF()

  override def markSafe(counterParticipant: ParticipantId, period: CommitmentPeriod)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    incrementCounterAndErrF()

  private val err: AcsCommitmentStore.AcsCommitmentStoreError =
    AcsCommitmentStore.AcsCommitmentDbError("bla")

  def incrementCounterAndErrE[A]()
      : EitherT[Future, AcsCommitmentStore.AcsCommitmentStoreError, A] = {
    writeCounter.incrementAndGet().discard
    EitherT.leftT[Future, A](err)
  }

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsCommitmentStore.AcsCommitmentStoreError, Option[PruningStatus]] =
    EitherT.rightT[Future, AcsCommitmentStore.AcsCommitmentStoreError](None)

  override protected[canton] def advancePruningTimestamp(
      phase: PruningPhase,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsCommitmentStore.AcsCommitmentStoreError, Unit] =
    incrementCounterAndErrE()

  override protected[canton] def doPrune(limit: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsCommitmentStore.AcsCommitmentStoreError, Unit] =
    incrementCounterAndErrE()

  override def getComputed(period: CommitmentPeriod, counterParticipant: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Iterable[(CommitmentPeriod, CommitmentType)]] =
    Future.successful(Iterable.empty)

  override def lastComputedAndSent(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestampSecond]] =
    Future(None)

  override def noOutstandingCommitments(beforeOrAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(None)

  override def outstanding(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Future[Iterable[(CommitmentPeriod, ParticipantId)]] =
    Future.successful(Iterable.empty)

  override def searchComputedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit
      traceContext: TraceContext
  ): Future[Iterable[(CommitmentPeriod, ParticipantId, CommitmentType)]] =
    Future.successful(Iterable.empty)

  override def searchReceivedBetween(
      start: CantonTimestamp,
      end: CantonTimestamp,
      counterParticipant: Option[ParticipantId],
  )(implicit traceContext: TraceContext): Future[Iterable[SignedProtocolMessage[AcsCommitment]]] =
    Future.successful(Iterable.empty)

  override val runningCommitments: IncrementalCommitmentStore =
    new ThrowOnWriteIncrementalCommitmentStore

  override val queue: CommitmentQueue = new ThrowOnWriteCommitmentQueue

  private def incrementCounterAndErrF[V](): Future[V] = {
    writeCounter.incrementAndGet().discard
    Future.failed(new RuntimeException("error"))
  }

  class ThrowOnWriteIncrementalCommitmentStore extends IncrementalCommitmentStore {
    override def get()(implicit
        traceContext: TraceContext
    ): Future[(RecordTime, Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType])] =
      Future.successful((RecordTime.MinValue, Map.empty))

    override def watermark(implicit traceContext: TraceContext): Future[RecordTime] =
      Future.successful(RecordTime.MinValue)

    override def update(
        rt: RecordTime,
        updates: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
        deletes: Set[SortedSet[LfPartyId]],
    )(implicit traceContext: TraceContext): Future[Unit] =
      incrementCounterAndErrF()
  }

  class ThrowOnWriteCommitmentQueue extends CommitmentQueue {
    override def enqueue(commitment: AcsCommitment)(implicit
        traceContext: TraceContext
    ): Future[Unit] = incrementCounterAndErrF()

    override def peekThrough(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): Future[List[AcsCommitment]] = Future.successful(List.empty)

    override def deleteThrough(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): Future[Unit] = incrementCounterAndErrF()
  }
}
