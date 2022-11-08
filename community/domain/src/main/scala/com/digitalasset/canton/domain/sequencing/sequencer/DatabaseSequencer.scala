// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.Materializer
import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.*
import com.digitalasset.canton.domain.sequencing.sequencer.store.*
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SendAsyncError,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.{
  AuthenticatedMember,
  DomainId,
  DomainTopologyManagerId,
  Member,
  UnauthenticatedMemberId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureUtil.doNotAwait
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import io.functionmeta.functionFullName
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

object DatabaseSequencer {

  /** Creates a single instance of a database sequencer. */
  def single(
      config: DatabaseSequencerConfig,
      timeouts: ProcessingTimeout,
      storage: Storage,
      clock: Clock,
      domainId: DomainId,
      topologyClientMember: Member,
      protocolVersion: ProtocolVersion,
      cryptoApi: DomainSyncCryptoClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): DatabaseSequencer = {

    val logger = TracedLogger(DatabaseSequencer.getClass, loggerFactory)

    ErrorUtil.requireArgument(
      !config.highAvailabilityEnabled,
      "Single database sequencer creation must not have HA enabled",
    )(ErrorLoggingContext.fromTracedLogger(logger)(TraceContext.empty))

    new DatabaseSequencer(
      SequencerWriterStoreFactory.singleInstance,
      config,
      TotalNodeCountValues.SingleSequencerTotalNodeCount,
      new LocalSequencerStateEventSignaller(
        timeouts,
        loggerFactory,
      ),
      None,
      // Dummy config which will be ignored anyway as `config.highAvailabilityEnabled` is false
      OnlineSequencerCheckConfig(),
      timeouts,
      storage,
      None,
      clock,
      domainId,
      topologyClientMember,
      protocolVersion,
      cryptoApi,
      loggerFactory,
    )
  }
}

class DatabaseSequencer(
    writerStorageFactory: SequencerWriterStoreFactory,
    config: DatabaseSequencerConfig,
    totalNodeCount: PositiveInt,
    eventSignaller: EventSignaller,
    keepAliveInterval: Option[NonNegativeFiniteDuration],
    onlineSequencerCheckConfig: OnlineSequencerCheckConfig,
    override protected val timeouts: ProcessingTimeout,
    storage: Storage,
    health: Option[SequencerHealthConfig],
    clock: Clock,
    domainId: DomainId,
    topologyClientMember: Member,
    protocolVersion: ProtocolVersion,
    cryptoApi: DomainSyncCryptoClient,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, tracer: Tracer, materializer: Materializer)
    extends BaseSequencer(
      DomainTopologyManagerId(domainId),
      loggerFactory,
      health,
      clock,
      BaseSequencer.checkSignature(cryptoApi),
    )
    with FlagCloseable {
  private val store: SequencerStore =
    SequencerStore(
      storage,
      protocolVersion,
      config.writer.maxSqlInListSize,
      timeouts,
      loggerFactory,
    )

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
    protocolVersion,
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
        performUnlessClosingF(functionFullName)(markOfflineF().thereafter { _ =>
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
      topologyClientMember,
      protocolVersion,
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

  override protected def sendAsyncSignedInternal(
      signedSubmission: SignedContent[SubmissionRequest]
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    sendAsyncInternal(signedSubmission.content)

  override def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource] =
    reader.read(member, offset)

  override def acknowledgeSigned(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val ack = signedAcknowledgeRequest.content
    acknowledge(ack.member, ack.timestamp)
  }

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
    withExpectedRegisteredMember(member, "Disable member") { memberId =>
      member match {
        // Unauthenticated members being disabled get automatically unregistered
        case unauthenticated: UnauthenticatedMemberId =>
          store.unregisterUnauthenticatedMember(unauthenticated)
        case _: AuthenticatedMember =>
          store.disableMember(memberId)
      }
    }
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

  override def healthInternal(implicit traceContext: TraceContext): Future[SequencerHealthStatus] =
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
      result <- store.prune(requestedTimestamp, status, config.writer.payloadToEventMargin)
    } yield result

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, SequencerSnapshot] =
    EitherT.rightT(SequencerSnapshot.unimplemented(protocolVersion))

  override private[sequencing] def firstSequencerCounterServeableForSequencer: SequencerCounter =
    // Database sequencers are never bootstrapped
    SequencerCounter.Genesis

  override def onClosed(): Unit = {
    super.onClosed()
    Lifecycle.close(
      writer,
      reader,
      eventSignaller,
      store,
    )(logger)
  }

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
