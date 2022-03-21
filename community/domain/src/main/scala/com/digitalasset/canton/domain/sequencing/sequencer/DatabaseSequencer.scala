// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.Materializer
import cats.data.EitherT
import cats.syntax.functor._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors._
import com.digitalasset.canton.domain.sequencing.sequencer.store._
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.topology.{DomainId, DomainTopologyManagerId, Member}
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.protocol.{SendAsyncError, SubmissionRequest}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.FutureUtil.doNotAwait
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.Thereafter.syntax._
import com.digitalasset.canton.SequencerCounter
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

class DatabaseSequencer(
    writerStorageFactory: SequencerWriterStoreFactory,
    config: DatabaseSequencerConfig,
    totalNodeCount: Int,
    keepAliveInterval: Option[NonNegativeFiniteDuration],
    onlineSequencerCheckConfig: OnlineSequencerCheckConfig,
    override protected val timeouts: ProcessingTimeout,
    storage: Storage,
    clock: Clock,
    domainId: DomainId,
    cryptoApi: DomainSyncCryptoClient,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, tracer: Tracer, materializer: Materializer)
    extends BaseSequencer(DomainTopologyManagerId(domainId), loggerFactory)
    with FlagCloseable {
  private val store: SequencerStore =
    SequencerStore(
      storage,
      config.writer.maxSqlInListSize,
      timeouts,
      loggerFactory,
    )

  // if high availability is configured we will assume that more than one sequencer is being used
  // and we will switch to using the polling based event signalling as we won't have visibility
  // of all writes locally.
  private val eventSignaller =
    if (config.highAvailabilityEnabled)
      new PollingEventSignaller(config.reader.pollingInterval, loggerFactory)
    else
      new LocalSequencerStateEventSignaller(timeouts, loggerFactory)

  private val writer = SequencerWriter(
    config.writer,
    writerStorageFactory,
    totalNodeCount,
    keepAliveInterval,
    timeouts,
    storage,
    store,
    clock,
    cryptoApi,
    eventSignaller,
    loggerFactory,
  )
  withNewTraceContext { implicit traceContext =>
    timeouts.unbounded.await(s"Waiting for sequencer writer to fully start")(
      writer.startOrLogError()
    )
  }

  // periodically run the call to mark lagging sequencers as offline
  private def periodicallyMarkLaggingSequencersOffline(
      checkInterval: NonNegativeFiniteDuration,
      offlineCutoffDuration: NonNegativeFiniteDuration,
  ): Unit = {
    def schedule(): Unit = {
      val _ = clock.scheduleAfter(_ => markOffline(), checkInterval.unwrap)
    }

    def markOfflineF()(implicit traceContext: TraceContext): Future[Unit] = {
      val cutoffTime = clock.now.minus(offlineCutoffDuration.unwrap)
      logger.trace(s"Marking sequencers with watermarks earlier than [$cutoffTime] as offline")

      store.markLaggingSequencersOffline(cutoffTime)
    }

    def markOffline(): Unit = withNewTraceContext { implicit traceContext =>
      doNotAwait(
        performUnlessClosingF(markOfflineF().thereafter { _ =>
          // schedule next marking sequencers as offline regardless of outcome
          schedule()
        }).onShutdown {
          logger.debug("Skipping scheduling next offline sequencer check due to shutdown")
        },
        "Marking lagging sequencers as offline failed",
      )
    }

    schedule()
  }

  if (config.highAvailabilityEnabled)
    periodicallyMarkLaggingSequencersOffline(
      onlineSequencerCheckConfig.onlineCheckInterval,
      onlineSequencerCheckConfig.offlineDuration,
    )

  private val reader =
    new SequencerReader(
      config.reader,
      domainId,
      store,
      cryptoApi,
      eventSignaller,
      timeouts,
      loggerFactory,
    )

  override def isRegistered(member: Member)(implicit traceContext: TraceContext): Future[Boolean] =
    store.lookupMember(member).map(_.isDefined)

  override def registerMember(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit] = {
    // use a timestamp which is definitively lower than the subsequent sequencing timestamp
    // otherwise, if the timestamp we use to register the member is equal or higher than the
    // first message to the member, we'd ignore the first message by accident
    val nowMs = clock.monotonicTime().toMicros
    val uniqueMicros = nowMs - (nowMs % TotalNodeCountValues.MaxNodeCount) - 1
    EitherT.right(store.registerMember(member, CantonTimestamp.assertFromLong(uniqueMicros)).void)
  }

  override protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit] =
    writer.send(submission)

  override def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource] =
    reader.read(member, offset)

  override def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    // it is unlikely that the ack operation will be called without the member being registered
    // as the sequencer-client will need to be registered to send and subscribe.
    // rather than introduce an error to deal with this case in the database sequencer we'll just
    // fail the operation.
    withExpectedRegisteredMember(member, "Acknowledge") {
      store.acknowledge(_, timestamp)
    }

  def disableMember(member: Member)(implicit traceContext: TraceContext): Future[Unit] = {
    logger.info(show"Disabling member at the sequencer: $member")
    withExpectedRegisteredMember(member, "Disable member")(store.disableMember)
  }

  /** helper for performing operations that are expected to be called with a registered member so will just throw if we
    * find the member is unregistered.
    */
  private def withExpectedRegisteredMember[A](member: Member, operationName: String)(
      fn: SequencerMemberId => Future[A]
  )(implicit traceContext: TraceContext): Future[A] =
    for {
      memberIdO <- store.lookupMember(member).map(_.map(_.memberId))
      memberId = memberIdO.getOrElse {
        logger.warn(s"$operationName attempted to use member [$member] but they are not registered")
        sys.error(s"Operation requires the member to have been registered with the sequencer")
      }
      result <- fn(memberId)
    } yield result

  override def pruningStatus(implicit traceContext: TraceContext): Future[SequencerPruningStatus] =
    store.status(clock.now)

  override def health(implicit traceContext: TraceContext): Future[SequencerHealthStatus] =
    Future.successful(
      SequencerHealthStatus(
        isActive = writer.isActive
      )
    )

  override def prune(
      requestedTimestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, PruningError, String] =
    for {
      status <- EitherT.right[PruningError](this.pruningStatus)
      result <- store.prune(requestedTimestamp, status, config.writer.payloadToEventBound)
    } yield result

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, SequencerSnapshot] =
    EitherT.rightT(SequencerSnapshot.Unimplemented)

  override protected def onClosed(): Unit =
    Lifecycle.close(
      writer,
      reader,
      eventSignaller,
      store,
    )(logger)

  override def isLedgerIdentityRegistered(identity: LedgerIdentity)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    // unimplemented. We don't plan to implement ledger identity authorization for database sequencers, so this
    // function will never be implemented.
    Future.successful(false)

  override def authorizeLedgerIdentity(identity: LedgerIdentity)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    // see [[isLedgerIdentityRegistered]]
    EitherT.leftT("authorizeLedgerIdentity is not implemented for database sequencers")
}
