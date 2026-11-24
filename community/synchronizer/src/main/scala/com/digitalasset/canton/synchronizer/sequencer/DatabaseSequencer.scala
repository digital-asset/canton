// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, Storage}
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencer.admin.v30.TrafficSummary
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.TrafficControlError
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.RegisterError
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriter.ResetWatermark
import com.digitalasset.canton.synchronizer.sequencer.admin.data.{
  SequencerAdminStatus,
  SequencerHealthStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.BlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.SnapshotNotFound
import com.digitalasset.canton.synchronizer.sequencer.errors.{
  CreateSubscriptionError,
  SequencerError,
}
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerStore.SequencerPruningResult
import com.digitalasset.canton.synchronizer.sequencer.store.{
  MemberLifeCycleStatus,
  PayloadId,
  SequencerMemberId,
  SequencerMemberValidator,
  SequencerStore,
}
import com.digitalasset.canton.synchronizer.sequencer.time.{
  DisasterRecoverySequencingTimeUpperBound,
  LsuSequencingBounds,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  LsuTrafficState,
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.{
  LsuSequencerConnectionSuccessor,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.retry.Pause
import com.digitalasset.canton.util.signalling.EventSignaller
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class DatabaseSequencer(
    sequencerStore: SequencerStore,
    config: DatabaseSequencerConfig,
    eventSignaller: EventSignaller[SequencerMemberId, Unit],
    keepAliveInterval: Option[NonNegativeFiniteDuration],
    override protected val timeouts: ProcessingTimeout,
    storage: Storage,
    health: Option[SequencerHealthConfig],
    clock: Clock,
    topologyClientMember: Member,
    cryptoApi: SynchronizerCryptoClient,
    metrics: SequencerMetrics,
    loggerFactory: NamedLoggerFactory,
    lsuSequencingBounds: Option[LsuSequencingBounds],
    drSequencingTimeUpperBound: Option[DisasterRecoverySequencingTimeUpperBound],
    rateLimitManagerO: Option[SequencerRateLimitManager],
    disableSubmissionChecksForTesting: Boolean,
)(implicit ec: ExecutionContext, tracer: Tracer, materializer: Materializer)
    extends BaseSequencer(
      loggerFactory,
      health,
      clock,
      SignatureVerifier(cryptoApi),
      cryptoApi.psid.protocolVersion,
      lsuSequencingBounds,
      disableSubmissionChecksForTesting,
    )
    with FlagCloseable
    with HasCloseContext {

  private val psid = cryptoApi.psid
  private val protocolVersion: ProtocolVersion = psid.protocolVersion

  private val writer = SequencerWriter(
    config.writer,
    keepAliveInterval,
    timeouts,
    sequencerStore,
    rateLimitManagerO,
    clock,
    eventSignaller,
    protocolVersion,
    loggerFactory,
    lsuSequencingBounds,
    drSequencingTimeUpperBound,
    metrics,
  )

  override val pruningScheduler: Option[PruningScheduler] =
    pruningSchedulerBuilder.map(_(storage))

  override def adminStatus: SequencerAdminStatus = SequencerAdminStatus(storage.isActive)

  @VisibleForTesting
  private[canton] val store = writer.generalStore

  protected val memberValidator: SequencerMemberValidator = sequencerStore

  protected def resetWatermarkTo: ResetWatermark = SequencerWriter.ResetWatermarkToClockNow

  // Only start pruning scheduler after `store` variable above has been initialized to avoid racy NPE
  withNewTraceContext("db_sequencer_start") { implicit traceContext =>
    timeouts.unbounded.await(s"Waiting for sequencer writer to fully start")(
      writer
        .startOrLogError(resetWatermarkTo)
        .onShutdown(logger.info("Sequencer writer not started due to shutdown"))
    )

    pruningScheduler.foreach(ps =>
      // default wait should be enough since scheduler start only involves single database access
      timeouts.default.awaitUS(
        s"Waiting for pruning scheduler to start",
        logFailing = Level.INFO.some, // in case we're shutting down at start-up
      )(ps.start())
    )
  }

  protected val reader =
    new SequencerReader(
      config.reader,
      sequencerStore,
      cryptoApi,
      eventSignaller,
      topologyClientMember,
      lsuSequencingBounds,
      metrics,
      timeouts,
      loggerFactory,
    )

  override def isRegistered(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    sequencerStore.lookupMember(member).map(_.isDefined)

  override def isActive(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    sequencerStore.lookupMember(member).map {
      case Some(registeredMember) =>
        registeredMember.status == MemberLifeCycleStatus.Active
      case None =>
        logger.warn(
          s"Attempted to check if member $member is active but they are not registered"
        )
        false
    }

  override private[sequencer] final def registerMemberInternal(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RegisterError, Unit] =
    EitherT
      .right[RegisterError](sequencerStore.registerMember(member, timestamp))
      .map(_ => ())

  override protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    for {
      // TODO(#12405) Support aggregatable submissions in the DB sequencer
      _ <- EitherT.cond[FutureUnlessShutdown](
        submission.aggregationRule.isEmpty,
        (),
        SequencerErrors.UnsupportedFeature(
          "Aggregatable submissions are not yet supported by this database sequencer"
        ),
      )
      // TODO(#12363) Support group addresses in the DB Sequencer
      _ <- EitherT.cond[FutureUnlessShutdown](
        !submission.batch.allRecipients.exists {
          case _: MemberRecipient => false
          case _ => true
        },
        (),
        SequencerErrors.UnsupportedFeature(
          "Group addresses are not yet supported by this database sequencer"
        ),
      )
      _ <- writer.send(submission).leftWiden[CantonBaseError]
    } yield ()

  protected def blockSequencerWriteInternal(
      outcome: DeliverableSubmissionOutcome
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    writer.blockSequencerWrite(outcome)

  override protected def sendAsyncSignedInternal(
      signedSubmission: SignedContent[SubmissionRequest],
      skipLsuChecks: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    sendAsyncInternal(signedSubmission.content)

  override def readInternal(member: Member, timestamp: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.SequencedEventSource] =
    reader.read(member, timestamp)

  override protected def readPayloadsFromTimestampsInternal(timestamps: Seq[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PayloadId, Batch[ClosedEnvelope]]] =
    FutureUnlessShutdown.failed(
      new UnsupportedOperationException(
        "readPayloadsFromTimestampsInternal is not supported by the database sequencer"
      )
    )

  override def getTrafficSummaries(timestamps: Seq[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficControlError, Seq[TrafficSummary]] =
    EitherT.liftF(
      FutureUnlessShutdown.failed(
        io.grpc.Status.UNIMPLEMENTED
          .withDescription("Traffic summaries are not implemented on the database sequencer")
          .asRuntimeException()
      )
    )

  /** Internal method to be used in the sequencer integration.
    */
  // TODO(#18401): Refactor ChunkUpdate and merge/remove this method with `acknowledgeSignedInternal` below
  final protected def writeAcknowledgementInternal(
      member: Member,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    withExpectedRegisteredMember(member, "Acknowledge") {
      sequencerStore.acknowledge(_, timestamp)
    }

  override protected def acknowledgeSignedInternal(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // it is unlikely that the ack operation will be called without the member being registered
    // as the sequencer-client will need to be registered to send and subscribe.
    // rather than introduce an error to deal with this case in the database sequencer we'll just
    // fail the operation.
    val req = signedAcknowledgeRequest.content
    writeAcknowledgementInternal(req.member, req.timestamp)
  }

  protected def disableMemberInternal(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    sequencerStore.disableMember(member)

  // For the database sequencer, the SequencerId serves as the local sequencer identity/member
  // until the database and block sequencers are unified.
  override protected def localSequencerMember: Member = SequencerId(psid.uid)

  /** helper for performing operations that are expected to be called with a registered member so
    * will just throw if we find the member is unregistered.
    */
  final protected def withExpectedRegisteredMember[A](member: Member, operationName: String)(
      fn: SequencerMemberId => FutureUnlessShutdown[A]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[A] =
    for {
      memberIdO <- sequencerStore.lookupMember(member).map(_.map(_.memberId))
      memberId = memberIdO.getOrElse {
        logger.warn(s"$operationName attempted to use member [$member] but they are not registered")
        sys.error(s"Operation requires the member to have been registered with the sequencer")
      }
      result <- fn(memberId)
    } yield result

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerPruningStatus] =
    sequencerStore.status(clock.now)

  override protected def healthInternal(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerHealthStatus] =
    writer.healthStatus

  override def prune(
      requestedTimestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, PruningError, String] =
    for {
      status <- EitherT.right[PruningError](this.pruningStatus)
      // Update the max-event-age metric after pruning. Use the actually pruned timestamp as
      // the database sequencer tends to prune fewer events than asked for e.g. not wanting to
      // prune sequencer-counter checkpoints partially.
      report <- sequencerStore
        .prune(requestedTimestamp, status, config.writer.payloadToEventMargin.toInternal)
        .map { case SequencerPruningResult(tsActuallyPrunedUpTo, report) =>
          MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, tsActuallyPrunedUpTo)
          report
        }
    } yield report

  override def findPruningTimestamp(index: PositiveInt)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PruningSupportError, Option[CantonTimestamp]] =
    EitherT.right[PruningSupportError](
      sequencerStore.findPruningTimestamp(NonNegativeInt.tryCreate(index.value - 1))
    )

  override def reportMaxEventAgeMetric(
      oldestEventTimestamp: Option[CantonTimestamp]
  ): Either[PruningSupportError, Unit] =
    Either.right(
      MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, oldestEventTimestamp)
    )

  override def awaitContainingBlockLastTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, CantonTimestamp] =
    EitherT.right(
      FutureUnlessShutdown.pure(timestamp)
    ) // DatabaseSequencer doesn't have a concept of blocks

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] =
    for {
      safeWatermarkO <- EitherT.right(sequencerStore.safeWatermark)
      // we check if watermark is after the requested timestamp to avoid snapshotting the sequencer
      // at a timestamp that is not yet safe to read
      _ <- checkWatermarkIsAfterTimestamp(safeWatermarkO, timestamp)
      snapshot <- EitherT.right[SequencerError](sequencerStore.readStateAtTimestamp(timestamp))
    } yield snapshot

  private def checkWatermarkIsAfterTimestamp(
      safeWatermarkO: Option[CantonTimestamp],
      timestamp: CantonTimestamp,
  ): EitherT[FutureUnlessShutdown, SequencerError, Unit] =
    safeWatermarkO match {
      case Some(safeWatermark) =>
        EitherTUtil.condUnitET[FutureUnlessShutdown](
          timestamp <= safeWatermark,
          SnapshotNotFound.Error(timestamp, safeWatermark),
        )
      case None =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          SnapshotNotFound.MissingSafeWatermark(topologyClientMember)
        )
    }

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] = {
    val delay = 1.second
    val waitForWatermarkToPassTimestamp = EitherT(
      Pause(
        logger,
        this,
        maxRetries = (timeouts.default.duration / delay).toInt,
        delay,
        s"$functionFullName($timestamp)",
      )
        .unlessShutdown(
          sequencerStore.safeWatermark.flatMap(checkWatermarkIsAfterTimestamp(_, timestamp).value),
          DbExceptionRetryPolicy,
        )
    )
    waitForWatermarkToPassTimestamp.flatMap(_ => snapshot(timestamp))
  }

  override def onClosed(): Unit =
    LifeCycle.close(
      () => super.onClosed(),
      () => pruningScheduler foreach (LifeCycle.close(_)(logger)),
      writer,
      reader,
      eventSignaller,
      sequencerStore,
      cryptoApi,
    )(logger)

  override def trafficStatus(
      members: Seq[Member],
      selector: TimestampSelector,
      includeOnboardingMembers: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficControlError, SequencerTrafficStatus] =
    throw new UnsupportedOperationException(
      "Traffic control is not supported by the database sequencer"
    )

  override def setTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClientSend,
      synchronizerTimeTracker: SynchronizerTimeTracker,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TrafficControlErrors.TrafficControlError,
    Unit,
  ] =
    throw new UnsupportedOperationException(
      "Traffic control is not supported by the database sequencer"
    )

  override def getTrafficStateAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficState
  ]] =
    throw new UnsupportedOperationException(
      "Traffic control is not supported by the database sequencer"
    )

  override private[sequencer] def updateLsuSuccessor(
      successor: SynchronizerSuccessor,
      announcementEffectiveTime: EffectiveTime,
      isReplace: Boolean,
  )(implicit traceContext: TraceContext): Unit =
    reader.updateLsuSuccessor(Option.when(isReplace)(successor), announcementEffectiveTime)

  // TODO(#27919): provide a proper implementation
  override def sequencingTime(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure(None)

  override private[canton] def orderer: Option[BlockOrderer] = None

  override def getLsuTrafficControlState(trafficTsOverride: Option[CantonTimestamp] = None)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, LsuTrafficState] =
    throw new UnsupportedOperationException(
      "Traffic control is not supported by the database sequencer"
    )

  override def setLsuTrafficControlState(state: LsuTrafficState)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    throw new UnsupportedOperationException(
      "Traffic control is not supported by the database sequencer"
    )

  override def performLsuSequencingTest(mediatorGroupRecipient: MediatorGroupRecipient)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    throw new UnsupportedOperationException(
      "LSU sanity check is not supported by the database sequencer"
    )

  override def handleLsuSequencerConnectionSuccessor(
      successor: TopologyTransaction[TopologyChangeOp, LsuSequencerConnectionSuccessor]
  )(implicit
      traceContext: TraceContext
  ): Unit =
    throw new UnsupportedOperationException(
      "LSU operations are not supported by the DB sequencer"
    )
}
