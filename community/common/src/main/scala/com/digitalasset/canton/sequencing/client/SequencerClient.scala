// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import akka.stream.Materializer
import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.metrics.Timed
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  KeepAliveClientConfig,
  LoggingConfig,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.{
  Crypto,
  DomainSyncCryptoClient,
  HashPurpose,
  SyncCryptoApi,
  SyncCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.Lifecycle.toCloseableOption
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.CantonPrettyPrinter
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.ReplayAction.{SequencerEvents, SequencerSends}
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.*
import com.digitalasset.canton.sequencing.client.grpc.GrpcSequencerChannelBuilder
import com.digitalasset.canton.sequencing.client.http.HttpSequencerClient
import com.digitalasset.canton.sequencing.client.transports.*
import com.digitalasset.canton.sequencing.client.transports.replay.{
  ReplayingEventsSequencerClientTransport,
  ReplayingSendsSequencerClientTransport,
}
import com.digitalasset.canton.sequencing.handlers.{
  CleanSequencerCounterTracker,
  StoreSequencedEvent,
}
import com.digitalasset.canton.sequencing.handshake.SequencerHandshake
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore.{
  OrdinarySequencedEvent,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced, TracingConfig}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, SequencerCounter}
import com.google.common.annotations.VisibleForTesting
import io.functionmeta.functionFullName
import io.grpc.CallOptions
import io.opentelemetry.api.trace.Tracer

import java.nio.file.Path
import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, LinkedBlockingQueue}
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

/** Client configured options for how to connect to a sequencer
  *
  * @param eventInboxSize The size of the inbox queue used to store received events. Must be at least one.
  *                       Events in the inbox are processed in parallel.
  *                       A larger inbox may result in higher throughput at the price of higher memory consumption,
  *                       larger database queries, and longer crash recovery.
  * @param startupConnectionRetryDelay Initial delay before we attempt to establish an initial connection
  * @param initialConnectionRetryDelay Initial delay before a reconnect attempt
  * @param warnDisconnectDelay Consider sequencer to be degraded after delay
  * @param maxConnectionRetryDelay Maximum delay before a reconnect attempt
  * @param handshakeRetryAttempts How many attempts should we make to get a handshake response
  * @param handshakeRetryDelay How long to delay between attempts to fetch a handshake response
  * @param defaultMaxSequencingTimeOffset if no max-sequencing-time is supplied to send, our current time will be offset by this amount
  * @param acknowledgementInterval Controls how frequently the client acknowledges how far it has successfully processed
  *                                to the sequencer which allows the sequencer to remove this data when pruning.
  * @param keepAlive keep alive config used for GRPC sequencers
  * @param authToken configuration settings for the authentication token manager
  * @param optimisticSequencedEventValidation if true, sequenced event signatures will be validated first optimistically
  *                                           and only strict if the optimistic evaluation failed. this means that
  *                                           for a split second, we might still accept an event signed with a key that
  *                                           has just been revoked.
  * @param skipSequencedEventValidation if true, sequenced event validation will be skipped. the default setting is false.
  *                                     this option should only be enabled if a defective validation is blocking processing.
  *                                     therefore, unless you know what you are doing, you shouldn't touch this setting.
  */
case class SequencerClientConfig(
    eventInboxSize: PositiveInt = PositiveInt.tryCreate(100),
    startupConnectionRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(1),
    initialConnectionRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(10),
    warnDisconnectDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
    maxConnectionRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(30),
    handshakeRetryAttempts: NonNegativeInt = NonNegativeInt.tryCreate(50),
    handshakeRetryDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
    defaultMaxSequencingTimeOffset: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMinutes(5),
    acknowledgementInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(1),
    keepAliveClient: Option[KeepAliveClientConfig] = Some(KeepAliveClientConfig()),
    authToken: AuthenticationTokenManagerConfig = AuthenticationTokenManagerConfig(),
    // TODO(#10040) remove optimistic validation
    optimisticSequencedEventValidation: Boolean = true,
    skipSequencedEventValidation: Boolean = false,
)

/** What type of message is being sent.
  * Used by the domain and surrounding infrastructure for prioritizing send requests.
  */
sealed trait SendType {
  private[client] val isRequest: Boolean
}

object SendType {

  /** An initial confirmation request. This is subject to throttling at the domain if resource constrained. */
  case object ConfirmationRequest extends SendType {
    override private[client] val isRequest = true
  }

  /** There's currently no requirement to distinguish other types of request */
  case object Other extends SendType {
    override private[client] val isRequest = false
  }
}

/** The sequencer client facilitates access to the individual domain sequencer. A client centralizes the
  * message signing operations, as well as the handling and storage of message receipts and delivery proofs,
  * such that this functionality does not have to be duplicated throughout the participant node.
  */
class SequencerClient(
    val domainId: DomainId,
    val member: Member,
    sequencerClientTransport: SequencerClientTransport,
    val config: SequencerClientConfig,
    testingConfig: TestingConfigInternal,
    val staticDomainParameters: StaticDomainParameters,
    override val timeouts: ProcessingTimeout,
    eventValidatorFactory: SequencedEventValidatorFactory,
    clock: Clock,
    signSubmission: TraceContext => SubmissionRequest => EitherT[Future, String, SignedContent[
      SubmissionRequest
    ]],
    private val sequencedEventStore: SequencedEventStore,
    sendTracker: SendTracker,
    metrics: SequencerClientMetrics,
    recorderO: Option[SequencerClientRecorder],
    replayEnabled: Boolean,
    loggingConfig: LoggingConfig,
    val loggerFactory: NamedLoggerFactory,
    initialCounter: Option[SequencerCounter] = None,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends SequencerClientSend
    with FlagCloseableAsync
    with NamedLogging
    with HasFlushFuture
    with Spanning {
  private val currentTransport =
    new AtomicReference[SequencerClientTransport](sequencerClientTransport)
  private def transport: SequencerClientTransport = currentTransport.get

  private val closeReasonPromise = Promise[SequencerClient.CloseReason]()
  // Keeps track of the subscription.
  private val currentSubscription =
    new AtomicReference[Option[ResilientSequencerSubscription[SequencerClientSubscriptionError]]](
      None
    )
  // Optional reference as is only created when tracking is subscribed
  private val periodicAcknowledgementsRef =
    new AtomicReference[Option[PeriodicAcknowledgements]](None)

  /** Stash for storing the failure that comes out of an application handler, either synchronously or asynchronously.
    * If non-empty, no further events should be sent to the application handler.
    */
  private val applicationHandlerFailure: SingleUseCell[ApplicationHandlerFailure] =
    new SingleUseCell[ApplicationHandlerFailure]

  /** Completed iff the handler is idle. */
  private val handlerIdle: AtomicReference[Promise[Unit]] = new AtomicReference(
    Promise.successful(())
  )

  /** Queue containing received and not yet handled events.
    * Used for batched processing.
    */
  private val receivedEvents: BlockingQueue[OrdinarySerializedEvent] =
    new ArrayBlockingQueue[OrdinarySerializedEvent](config.eventInboxSize.unwrap)

  private lazy val printer =
    new CantonPrettyPrinter(loggingConfig.api.maxStringLength, loggingConfig.api.maxMessageLines)

  /** returns true if the sequencer subscription is healthy */
  def subscriptionIsHealthy: Boolean =
    currentSubscription.get().exists(!_.isDegraded)

  override def sendAsync(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType = SendType.Other,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
      maxSequencingTime: CantonTimestamp = generateMaxSequencingTime,
      messageId: MessageId = generateMessageId,
      callback: SendCallback = SendCallback.empty,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    for {
      _ <- EitherT.cond[Future](
        member.isAuthenticated,
        (),
        SendAsyncClientError.RequestInvalid(
          "Only authenticated members can use the authenticated send operation"
        ): SendAsyncClientError,
      )
      _ <- EitherT.cond[Future](
        timestampOfSigningKey.isEmpty || batch.envelopes.forall(
          _.recipients.allRecipients.forall(_.isAuthenticated)
        ),
        (),
        SendAsyncClientError.RequestInvalid(
          "Requests addressed to unauthenticated members must not specify a timestamp for the signing key"
        ): SendAsyncClientError,
      )
      result <- sendAsyncInternal(
        batch,
        requiresAuthentication = true,
        sendType,
        timestampOfSigningKey,
        maxSequencingTime,
        messageId,
        callback,
      )
    } yield result

  /** Does the same as [[sendAsync]], except that this method is supposed to be used
    * only by unauthenticated members for very specific operations that do not require authentication
    * such as requesting that a participant's topology data gets accepted by the topology manager
    */
  def sendAsyncUnauthenticated(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType = SendType.Other,
      maxSequencingTime: CantonTimestamp = generateMaxSequencingTime,
      messageId: MessageId = generateMessageId,
      callback: SendCallback = SendCallback.empty,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    if (member.isAuthenticated)
      EitherT.leftT(
        SendAsyncClientError.RequestInvalid(
          "Only unauthenticated members can use the unauthenticated send operation"
        )
      )
    else
      sendAsyncInternal(
        batch,
        requiresAuthentication = false,
        sendType,
        // Requests involving unauthenticated members must not specify a signing key
        None,
        maxSequencingTime,
        messageId,
        callback,
      )

  private def protocolVersion: ProtocolVersion = staticDomainParameters.protocolVersion

  private def sendAsyncInternal(
      batch: Batch[DefaultOpenEnvelope],
      requiresAuthentication: Boolean,
      sendType: SendType = SendType.Other,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
      maxSequencingTime: CantonTimestamp,
      messageId: MessageId = generateMessageId,
      callback: SendCallback = SendCallback.empty,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    withSpan("SequencerClient.sendAsync") { implicit traceContext => span =>
      val request = SubmissionRequest(
        member,
        messageId,
        sendType.isRequest,
        Batch.closeEnvelopes(batch),
        maxSequencingTime,
        timestampOfSigningKey,
        protocolVersion,
      )

      if (loggingConfig.eventDetails) {
        logger.debug(
          s"About to send async batch ${printer.printAdHoc(batch)} as request ${printer.printAdHoc(request)}"
        )
      }

      span.setAttribute("member", member.show)
      span.setAttribute("message_id", messageId.unwrap)

      require(
        Batch.supportedProtoVersions.protoVersionsCount == 1,
        "verifyBatchSize below assumes the batch is serialized with Proto version 0." +
          " Update it if more versions are supported",
      )

      val serializedRequestSize = request.toProtoV0.serializedSize
      val maxInboundMessageSize = staticDomainParameters.maxInboundMessageSize.unwrap
      val unitOrRequestSizeErr = Either.cond(
        serializedRequestSize <= maxInboundMessageSize,
        (),
        SendAsyncClientError.RequestInvalid(
          s"Batch size ($serializedRequestSize bytes) is exceeding maximum size (${maxInboundMessageSize} bytes) for domain $domainId"
        ),
      )

      def trackSend: EitherT[Future, SendAsyncClientError, Unit] =
        sendTracker
          .track(messageId, maxSequencingTime, callback)
          .leftMap[SendAsyncClientError] { case SavePendingSendError.MessageIdAlreadyTracked =>
            // we're already tracking this message id
            SendAsyncClientError.DuplicateMessageId
          }

      if (replayEnabled) {
        EitherT.fromEither(for {
          _ <- unitOrRequestSizeErr
        } yield {
          // Invoke the callback immediately, because it will not be triggered by replayed messages,
          // as they will very likely have mismatching message ids.
          val dummySendResult =
            SendResult.Success(
              Deliver.create(
                SequencerCounter.Genesis,
                CantonTimestamp.now(),
                domainId,
                None,
                Batch(List.empty, protocolVersion),
                protocolVersion,
              )
            )
          callback(dummySendResult)
        })
      } else {
        for {
          _ <- EitherT.fromEither[Future](unitOrRequestSizeErr)
          _ <- trackSend
          _ = recorderO.foreach(_.recordSubmission(request))
          _ <- performSend(messageId, request, requiresAuthentication)
        } yield ()
      }
    }

  /** Perform the send, without any check.
    */
  private def performSend(
      messageId: MessageId,
      request: SubmissionRequest,
      requiresAuthentication: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
    EitherTUtil
      .timed(metrics.submissions.sends) {
        val timeout = timeouts.network.duration
        if (requiresAuthentication) {
          val sigCheckSupportSince = ProtocolVersion.dev
          val sigCheckSupported = protocolVersion >= sigCheckSupportSince
          if (sigCheckSupported) for {
            signedContent <- signSubmission(traceContext)(request)
              .leftMap { err =>
                val message = s"Error signing submission request $err"
                logger.error(message)
                SendAsyncClientError.RequestRefused(SendAsyncError.RequestRefused(message))
              }
            _ <- transport.sendAsyncSigned(
              signedContent,
              timeout,
              protocolVersion,
            )
          } yield ()
          else
            transport.sendAsync(
              request,
              timeout,
              protocolVersion,
            )
        } else
          transport.sendAsyncUnauthenticated(
            request,
            timeout,
            protocolVersion,
          )
      }
      .leftSemiflatMap { err =>
        // increment appropriate error metrics
        err match {
          case SendAsyncClientError.RequestRefused(SendAsyncError.Overloaded(_)) =>
            metrics.submissions.overloaded.metric.inc()
          case _ =>
        }

        // cancel pending send now as we know the request will never cause a sequenced result
        logger.debug(s"Cancelling the pending send as the sequencer returned error: $err")
        sendTracker.cancelPendingSend(messageId).map(_ => err)
      }
  }

  override def generateMaxSequencingTime: CantonTimestamp =
    clock.now.add(config.defaultMaxSequencingTimeOffset.unwrap)

  override protected def generateMessageId: MessageId = MessageId.randomMessageId()

  /** Create a subscription for sequenced events for this member,
    * starting after the prehead in the `sequencerCounterTrackerStore`.
    *
    * The `eventHandler` is monitored by [[com.digitalasset.canton.sequencing.handlers.CleanSequencerCounterTracker]]
    * so that the `sequencerCounterTrackerStore` advances the prehead
    * when (a batch of) events has been successfully processed by the `eventHandler` (synchronously and asynchronously).
    *
    * @see subscribe for the description of the `eventHandler`, `timeTracker` and the `timeoutHandler`
    */
  def subscribeTracking(
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      timeoutHandler: SendTimeoutHandler = loggingTimeoutHandler,
      onCleanHandler: Traced[SequencerCounterCursorPrehead] => Future[Unit] = _ => Future.unit,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    sequencerCounterTrackerStore.preheadSequencerCounter.flatMap { cleanPrehead =>
      val priorTimestamp = cleanPrehead.fold(CantonTimestamp.MinValue)(
        _.timestamp
      ) // Sequencer client will feed events right after this ts to the handler.
      val cleanSequencerCounterTracker = new CleanSequencerCounterTracker(
        sequencerCounterTrackerStore,
        onCleanHandler,
        loggerFactory,
      )
      subscribeAfter(
        priorTimestamp,
        cleanPrehead.map(_.timestamp),
        cleanSequencerCounterTracker(eventHandler),
        timeTracker,
        PeriodicAcknowledgements.fetchCleanCounterFromStore(sequencerCounterTrackerStore),
        timeoutHandler,
      )
    }
  }

  /** Create a subscription for sequenced events for this member,
    * starting after the last event in the [[com.digitalasset.canton.store.SequencedEventStore]] up to `priorTimestamp`.
    * A sequencer client can only have a single subscription - additional subscription attempts will throw an exception.
    * When an event is received any send timeouts caused by the advancement of the sequencer clock observed from the
    * event will first be raised to the `timeoutHandler`, then the `eventHandler` will be invoked.
    *
    * If the [[com.digitalasset.canton.store.SequencedEventStore]] contains events after `priorTimestamp`,
    * the handler is first fed with these events before the subscription is established,
    * starting at the last event found in the [[com.digitalasset.canton.store.SequencedEventStore]].
    *
    * @param priorTimestamp The timestamp of the event prior to where the event processing starts.
    *                       If [[scala.None$]], the subscription starts at the [[com.digitalasset.canton.data.CounterCompanion.Genesis]].
    * @param cleanPreheadTsO The timestamp of the clean prehead sequencer counter, if known.
    * @param eventHandler A function handling the events.
    * @param timeTracker Tracker for operations requiring the current domain time. Only updated with received events and not previously stored events.
    * @param timeoutHandler A function receiving timeout notifications for previously made sends. Defaults to logging timeouts at warn level.
    * @param fetchCleanTimestamp A function for retrieving the latest clean timestamp to use for periodic acknowledgements
    * @return The future completes after the subscription has been established or when an error occurs before that.
    *         In particular, synchronous processing of events from the [[com.digitalasset.canton.store.SequencedEventStore]]
    *         runs before the future completes.
    */
  def subscribeAfter(
      priorTimestamp: CantonTimestamp,
      cleanPreheadTsO: Option[CantonTimestamp],
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      fetchCleanTimestamp: PeriodicAcknowledgements.FetchCleanTimestamp,
      timeoutHandler: SendTimeoutHandler = loggingTimeoutHandler,
  )(implicit traceContext: TraceContext): Future[Unit] =
    subscribeAfterInternal(
      priorTimestamp,
      cleanPreheadTsO,
      eventHandler,
      timeTracker,
      timeoutHandler,
      fetchCleanTimestamp,
      requiresAuthentication = true,
    )

  /** Does the same as [[subscribeAfter]], except that this method is supposed to be used
    * only by unauthenticated members
    */
  def subscribeAfterUnauthenticated(
      priorTimestamp: CantonTimestamp,
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      timeoutHandler: SendTimeoutHandler = loggingTimeoutHandler,
  )(implicit traceContext: TraceContext): Future[Unit] =
    subscribeAfterInternal(
      priorTimestamp,
      // We do not track cleanliness for unauthenticated subscriptions
      cleanPreheadTsO = None,
      eventHandler,
      timeTracker,
      timeoutHandler,
      PeriodicAcknowledgements.noAcknowledgements,
      requiresAuthentication = false,
    )

  private def subscribeAfterInternal(
      priorTimestamp: CantonTimestamp,
      cleanPreheadTsO: Option[CantonTimestamp],
      eventHandler: PossiblyIgnoredApplicationHandler[ClosedEnvelope],
      timeTracker: DomainTimeTracker,
      timeoutHandler: SendTimeoutHandler,
      fetchCleanTimestamp: PeriodicAcknowledgements.FetchCleanTimestamp,
      requiresAuthentication: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val subscriptionF = performUnlessClosingUSF(functionFullName) {
      for {
        initialPriorEventO <- FutureUnlessShutdown.outcomeF(
          sequencedEventStore
            .find(SequencedEventStore.LatestUpto(priorTimestamp))
            .toOption
            .value
        )
        _ = if (initialPriorEventO.isEmpty) {
          logger.info(s"No event found up to $priorTimestamp. Resubscribing from the beginning.")
        }
        _ = cleanPreheadTsO.zip(initialPriorEventO).fold(()) {
          case (cleanPreheadTs, initialPriorEvent) =>
            ErrorUtil.requireArgument(
              initialPriorEvent.timestamp <= cleanPreheadTs,
              s"The initial prior event's timestamp ${initialPriorEvent.timestamp} is after the clean prehead at $cleanPreheadTs.",
            )
        }

        // bulk-feed the event handler with everything that we already have in the SequencedEventStore
        replayStartTimeInclusive = initialPriorEventO
          .fold(CantonTimestamp.MinValue)(_.timestamp)
          .immediateSuccessor
        _ = logger.info(
          s"Processing events from the SequencedEventStore from ${replayStartTimeInclusive} on"
        )

        replayEvents <- FutureUnlessShutdown.outcomeF(
          sequencedEventStore
            .findRange(
              SequencedEventStore
                .ByTimestampRange(replayStartTimeInclusive, CantonTimestamp.MaxValue),
              limit = None,
            )
            .valueOr { overlap =>
              ErrorUtil.internalError(
                new IllegalStateException(
                  s"Sequenced event store's pruning at ${overlap.pruningStatus.timestamp} is at or after the resubscription at $replayStartTimeInclusive."
                )
              )
            }
        )
        subscriptionStartsAt = replayEvents.headOption.fold(
          cleanPreheadTsO.fold(SubscriptionStart.FreshSubscription: SubscriptionStart)(
            SubscriptionStart.CleanHeadResubscriptionStart
          )
        )(replayEv =>
          SubscriptionStart.ReplayResubscriptionStart(replayEv.timestamp, cleanPreheadTsO)
        )
        _ <- eventHandler.subscriptionStartsAt(subscriptionStartsAt)

        eventBatches = replayEvents.grouped(config.eventInboxSize.unwrap)
        _ <- FutureUnlessShutdown.outcomeF(
          MonadUtil
            .sequentialTraverse_(eventBatches)(processEventBatch(eventHandler, _))
            .valueOr(err => throw SequencerClientSubscriptionException(err))
        )
      } yield {
        val lastEvent = replayEvents.lastOption
        val preSubscriptionEvent = lastEvent.orElse(initialPriorEventO)

        val nextCounter = initialCounter
          .getOrElse(preSubscriptionEvent.fold(SequencerCounter.Genesis)(_.counter))

        val eventValidator = eventValidatorFactory.create(
          // We validate events before we persist them in the SequencedEventStore
          // so we do not need to revalidate the replayed events.
          preSubscriptionEvent,
          // we need to inform the validator if this connection is unauthenticated, as unauthenticated connections
          // do not have the topology data to verify signatures
          unauthenticated = !requiresAuthentication,
        )

        logger.info(
          s"Starting subscription at timestamp ${preSubscriptionEvent.map(_.timestamp)}; next counter $nextCounter"
        )

        val eventDelay: DelaySequencedEvent = {
          val first = testingConfig.testSequencerClientFor.find(elem =>
            elem.memberName == member.uid.id.unwrap
              &&
              elem.domainName == domainId.unwrap.id.unwrap
          )

          first match {
            case Some(value) =>
              DelayedSequencerClient.registerAndCreate(
                value.environmentId,
                domainId,
                member.uid.toString,
              )
            case None => NoDelay
          }
        }

        val subscriptionHandler = new SubscriptionHandler(
          StoreSequencedEvent(sequencedEventStore, domainId, loggerFactory).apply(
            timeTracker.wrapHandler(eventHandler)
          ),
          timeoutHandler,
          eventValidator,
          eventDelay,
          preSubscriptionEvent.map(_.counter),
        )

        val subscription = ResilientSequencerSubscription[SequencerClientSubscriptionError](
          domainId.toString,
          member,
          transport,
          subscriptionHandler.handleEvent,
          nextCounter,
          config.initialConnectionRetryDelay.toScala,
          config.warnDisconnectDelay.toScala,
          config.maxConnectionRetryDelay.toScala,
          timeouts,
          requiresAuthentication,
          loggerFactory,
        )

        val replaced = currentSubscription.compareAndSet(None, Some(subscription))
        if (!replaced) {
          // there's an existing subscription!
          logger.warn(
            "Cannot create additional subscriptions to the sequencer from the same client"
          )
          sys.error("The sequencer client already has a running subscription")
        }

        // periodically acknowledge that we've successfully processed up to the clean counter
        // We only need to it setup once; the sequencer client will direct the acknowledgements to the
        // right transport.
        periodicAcknowledgementsRef.compareAndSet(
          None,
          PeriodicAcknowledgements
            .create(
              config.acknowledgementInterval.toScala,
              subscriptionIsHealthy,
              SequencerClient.this,
              fetchCleanTimestamp,
              clock,
              timeouts,
              loggerFactory,
            )
            .some,
        )

        // pipe the eventual close reason of the subscription to the client itself
        subscription.closeReason onComplete closeWithSubscriptionReason

        // now start the subscription
        subscription.start

        subscription
      }
    }

    // we may have actually not created a subscription if we have been closed
    val loggedAbortF = subscriptionF.unwrap.map {
      case UnlessShutdown.AbortedDueToShutdown =>
        logger.info("Ignoring the sequencer subscription request as the client is being closed")
      case UnlessShutdown.Outcome(_subscription) =>
        // Everything is fine, so no need to log anything.
        ()
    }
    FutureUtil.logOnFailure(loggedAbortF, "Sequencer subscription failed")
  }

  private val loggingTimeoutHandler: SendTimeoutHandler = msgId => {
    // logged at debug as this is likely logged at the caller using a send callback at a higher level
    logger.debug(s"Send with message id [$msgId] has timed out")(TraceContext.empty)
    EitherT.rightT(())
  }

  private class SubscriptionHandler(
      applicationHandler: OrdinaryApplicationHandler[ClosedEnvelope],
      timeoutHandler: SendTimeoutHandler,
      eventValidator: SequencedEventValidator,
      processingDelay: DelaySequencedEvent,
      initialPriorEventCounter: Option[SequencerCounter],
  ) {

    // keep track of the last event that we processed. In the event the SequencerClient is recreated or that our [[ResilientSequencerSubscription]] reconnects
    // we'll restart from the last successfully processed event counter and we'll validate it is still the last event we processed and that we're not seeing
    // a sequencer fork.
    private val priorEventCounter =
      new AtomicReference[Option[SequencerCounter]](initialPriorEventCounter)

    def handleEvent(
        serializedEvent: OrdinarySerializedEvent
    ): Future[Either[SequencerClientSubscriptionError, Unit]] = {
      implicit val traceContext: TraceContext = serializedEvent.traceContext
      // Process the event only if no failure has been detected
      applicationHandlerFailure.get.fold {
        recorderO.foreach(_.recordEvent(serializedEvent))

        def notifySendTracker(
            event: OrdinarySequencedEvent[_]
        ): EitherT[Future, SequencerClientSubscriptionError, Unit] =
          sendTracker.update(timeoutHandler)(event).leftWiden[SequencerClientSubscriptionError]

        def batchAndCallHandler(): Unit = {
          logger.debug(
            show"Storing event in the event inbox.\n${serializedEvent.signedEvent.content}"
          )
          if (!receivedEvents.offer(serializedEvent)) {
            logger.debug(
              s"Event inbox is full. Blocking sequenced event with timestamp ${serializedEvent.timestamp}."
            )
            blocking { receivedEvents.put(serializedEvent) }
            logger.debug(s"Unblocked sequenced event with timestamp ${serializedEvent.timestamp}.")
          }
          signalHandler(applicationHandler)
        }

        // to ensure that we haven't forked since we last connected, we actually subscribe from the event we last
        // successfully processed and do another round of validations on it to ensure it's the same event we really
        // did last process. However if successful, there's no need to give it to the application handler or to store
        // it as we're really sure we've already processed it.
        // we'll also see the last event replayed if the resilient sequencer subscription reconnects.
        val isReplayOfPriorEvent = priorEventCounter.get().contains(serializedEvent.counter)

        if (isReplayOfPriorEvent) {
          // just validate
          logger.debug(
            s"Do not handle event with sequencerCounter ${serializedEvent.counter}, as it is replayed and has already been handled."
          )
          eventValidator
            .validateOnReconnect(serializedEvent)
            .leftMap[SequencerClientSubscriptionError](EventValidationError)
            .value
        } else {
          logger.debug(
            s"Validating sequenced event with counter ${serializedEvent.counter} and timestamp ${serializedEvent.timestamp}"
          )
          (for {
            _unit <- EitherT.liftF(processingDelay.delay(serializedEvent))
            _ <- eventValidator
              .validate(serializedEvent)
              .leftMap[SequencerClientSubscriptionError](EventValidationError)
            _ <- notifySendTracker(serializedEvent)
          } yield {
            batchAndCallHandler()
            priorEventCounter.set(Some(serializedEvent.counter))
          }).value
        }
      }(err => Future.successful(Left(err)))
    }

    // Here is how shutdown works:
    //   1. we stop injecting new events even if the handler is idle using the performUnlessClosing,
    //   2. the synchronous processing will mark handlerIdle as not completed, and once started, will be added to the flush
    //      the performUnlessClosing will guard us from entering the close method (and starting to flush) before we've successfully
    //      registered with the flush future
    //   3. once the synchronous processing finishes, it will mark the `handlerIdle` as completed and complete the flush future
    //   4. before the synchronous processing terminates and before it marks the handler to be idle again,
    //      it will add the async processing to the flush future.
    //   Consequently, on shutdown, we first have to wait on the flush future.
    //     a. No synchronous event will be added to the flush future anymore by the signalHandler
    //        due to the performUnlessClosing. Therefore, we can be sure that once the flush future terminates
    //        during shutdown, that the synchronous processing has completed and nothing new has been added.
    //     b. However, the synchronous event processing will be adding async processing to the flush future in the
    //        meantime. This means that the flush future we are waiting on might be outdated.
    //        Therefore, we have to wait on the flush future again. We can then be sure that all asynchronous
    //        futures have been added in the meantime as the synchronous flush future finished.
    //     c. I (rv) think that waiting on the `handlerIdle` is a unnecessary for shutdown as it does the
    //        same as the flush future. We only need it to ensure we don't start the sequential processing in parallel.
    private def signalHandler(
        eventHandler: OrdinaryApplicationHandler[ClosedEnvelope]
    )(implicit traceContext: TraceContext): Unit = performUnlessClosing(functionFullName) {
      val isIdle = blocking {
        synchronized {
          val oldPromise = handlerIdle.getAndUpdate(p => if (p.isCompleted) Promise() else p)
          oldPromise.isCompleted
        }
      }
      if (isIdle) {
        val handlingF = handleReceivedEventsUntilEmpty(eventHandler)
        addToFlushAndLogError("invoking the application handler")(handlingF)
      }
    }.discard

    private def handleReceivedEventsUntilEmpty(
        eventHandler: OrdinaryApplicationHandler[ClosedEnvelope]
    ): Future[Unit] = {
      val inboxSize = config.eventInboxSize.unwrap
      val javaEventList = new java.util.ArrayList[OrdinarySerializedEvent](inboxSize)
      if (receivedEvents.drainTo(javaEventList, inboxSize) > 0) {
        import scala.jdk.CollectionConverters.*
        val handlerEvents = javaEventList.asScala

        def stopHandler(): Unit = blocking {
          this.synchronized { val _ = handlerIdle.get().success(()) }
        }

        processEventBatch(eventHandler, handlerEvents.toSeq).value
          .transformWith {
            case Success(Right(())) => handleReceivedEventsUntilEmpty(eventHandler)
            case Success(Left(_)) | Failure(_) =>
              // `processEventBatch` has already set `applicationHandlerFailure` so we don't need to propagate the error.
              stopHandler()
              Future.unit
          }
      } else {
        val stillBusy = blocking {
          this.synchronized {
            val idlePromise = handlerIdle.get()
            if (receivedEvents.isEmpty) {
              // signalHandler must not be executed here, because that would lead to lost signals.
              idlePromise.success(())
            }
            // signalHandler must not be executed here, because that would lead to duplicate invocations.
            !idlePromise.isCompleted
          }
        }

        if (stillBusy) {
          handleReceivedEventsUntilEmpty(eventHandler)
        } else {
          Future.unit
        }
      }
    }
  }

  /** If the returned future fails, contains a [[scala.Left$]]
    * or [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]
    * then [[applicationHandlerFailure]] contains an error.
    */
  private def processEventBatch[
      Box[+X <: Envelope[_]] <: PossiblyIgnoredSequencedEvent[X],
      Env <: Envelope[_],
  ](
      eventHandler: ApplicationHandler[Lambda[`+X <: Envelope[_]` => Traced[Seq[Box[X]]]], Env],
      eventBatch: Seq[Box[Env]],
  ): EitherT[Future, ApplicationHandlerFailure, Unit] =
    NonEmpty.from(eventBatch).fold(EitherT.pure[Future, ApplicationHandlerFailure](())) {
      eventBatchNE =>
        applicationHandlerFailure.get.fold {
          implicit val batchTraceContext: TraceContext = TraceContext.ofBatch(eventBatch)(logger)
          val lastSc = eventBatchNE.last1.counter
          val firstEvent = eventBatchNE.head1
          val firstSc = firstEvent.counter

          logger.debug(
            s"Passing ${eventBatch.size} events to the application handler ${eventHandler.name}."
          )
          // Measure only the synchronous part of the application handler so that we see how much the application handler
          // contributes to the sequential processing bottleneck.
          val asyncResultFT =
            Try(
              Timed
                .future(metrics.applicationHandle.metric, eventHandler(Traced(eventBatch)).unwrap)
            )

          def putApplicationHandlerFailure(
              failure: ApplicationHandlerFailure
          ): ApplicationHandlerFailure = {
            val alreadyCompleted = applicationHandlerFailure.putIfAbsent(failure)
            alreadyCompleted.foreach { earlierFailure =>
              logger.debug(show"Another event processing has previously failed: $earlierFailure")
            }
            logger.debug("Clearing the receivedEvents queue to unblock the subscription.")
            // Clear the receivedEvents queue, because the thread that inserts new events to the queue may block.
            // Clearing the queue is potentially dangerous, because it may result in data loss.
            // To prevent that, clear the queue only after setting applicationHandlerFailure.
            // - Once the applicationHandlerFailure has been set, any subsequent invocations of this method won't invoke
            //   the application handler.
            // - Ongoing invocations of this method are not affected by clearing the queue,
            //   because the events processed by the ongoing invocation have been drained from the queue before clearing.
            receivedEvents.clear()
            failure
          }

          def handleException(
              error: Throwable,
              syncProcessing: Boolean,
          ): ApplicationHandlerFailure = {
            val sync = if (syncProcessing) "Synchronous" else "Asynchronous"

            error match {
              case PassiveInstanceException(reason) =>
                logger.warn(
                  s"$sync event processing stopped because instance became passive"
                )
                putApplicationHandlerFailure(ApplicationHandlerPassive(reason))

              case _ =>
                logger.error(
                  s"$sync event processing failed for event batch with sequencer counters $firstSc to $lastSc.",
                  error,
                )
                putApplicationHandlerFailure(ApplicationHandlerException(error, firstSc, lastSc))
            }
          }

          def handleAsyncResult(
              asyncResultF: Future[UnlessShutdown[AsyncResult]]
          ): EitherT[Future, ApplicationHandlerFailure, Unit] =
            EitherTUtil
              .fromFuture(asyncResultF, handleException(_, syncProcessing = true))
              .subflatMap {
                case UnlessShutdown.Outcome(asyncResult) =>
                  val asyncSignalledF = asyncResult.unwrap.transform { result =>
                    // record errors and shutdown in `applicationHandlerFailure` and move on
                    result match {
                      case Success(outcome) =>
                        outcome
                          .onShutdown(
                            putApplicationHandlerFailure(ApplicationHandlerShutdown).discard
                          )
                          .discard
                      case Failure(error) =>
                        handleException(error, syncProcessing = false).discard
                    }
                    Success(UnlessShutdown.unit)
                  }.unwrap
                  // note, we are adding our async processing to the flush future, so we know once the async processing has finished
                  addToFlushAndLogError(
                    s"asynchronous event processing for event batch with sequencer counters $firstSc to $lastSc"
                  )(asyncSignalledF)
                  // we do not wait for the async results to finish, we are done here once the synchronous part is done
                  Right(())
                case UnlessShutdown.AbortedDueToShutdown =>
                  putApplicationHandlerFailure(ApplicationHandlerShutdown).discard
                  Left(ApplicationHandlerShutdown)
              }

          // note, here, we created the asyncResultF, which means we've completed the synchronous processing part.
          asyncResultFT.fold(
            error => EitherT.leftT[Future, Unit](handleException(error, syncProcessing = true)),
            handleAsyncResult,
          )
        }(EitherT.leftT[Future, Unit](_))
    }

  private def closeWithSubscriptionReason(
      subscriptionCloseReason: Try[SubscriptionCloseReason[SequencerClientSubscriptionError]]
  ): Unit = {

    val maybeCloseReason: Try[Either[SequencerClient.CloseReason, Unit]] =
      subscriptionCloseReason.map[Either[SequencerClient.CloseReason, Unit]] {
        case SubscriptionCloseReason.HandlerException(ex) =>
          Left(SequencerClient.CloseReason.UnrecoverableException(ex))
        case SubscriptionCloseReason.HandlerError(ApplicationHandlerPassive(_reason)) =>
          Left(SequencerClient.CloseReason.BecamePassive)
        case SubscriptionCloseReason.HandlerError(ApplicationHandlerShutdown) =>
          Left(SequencerClient.CloseReason.ClientShutdown)
        case SubscriptionCloseReason.HandlerError(err) =>
          Left(SequencerClient.CloseReason.UnrecoverableError(s"handler returned error: $err"))
        case permissionDenied: SubscriptionCloseReason.PermissionDeniedError =>
          Left(SequencerClient.CloseReason.PermissionDenied(s"$permissionDenied"))
        case subscriptionError: SubscriptionCloseReason.SubscriptionError =>
          Left(
            SequencerClient.CloseReason.UnrecoverableError(
              s"subscription implementation failed: $subscriptionError"
            )
          )
        case SubscriptionCloseReason.Closed => Left(SequencerClient.CloseReason.ClientShutdown)
        case SubscriptionCloseReason.TransportChange =>
          Right(()) // we don't want to close the sequencer client when changing transport
      }

    val _ = {
      val complete = closeReasonPromise.tryComplete _
      lazy val closeReason = maybeCloseReason.collect { case Left(error) =>
        error
      }
      maybeCloseReason match {
        case Failure(_) =>
          complete(closeReason)
        case Success(Left(_)) =>
          complete(closeReason)
        case Success(Right(())) =>
          false
      }
    }
  }

  /** Acknowledge that we have successfully processed all events up to and including the given timestamp.
    * The client should then never subscribe for events from before this point.
    */
  private[client] def acknowledge(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    transport.acknowledge(member, timestamp)

  def changeTransport(
      newTransport: SequencerClientTransport
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val oldTransport = currentTransport.getAndSet(newTransport)
    currentSubscription
      .get()
      .map(_.resubscribeOnTransportChange())
      .getOrElse(Future.unit)
      .thereafter { _ => oldTransport.close() }
  }

  /** Future which is completed when the client is not functional any more and is ready to be closed.
    * The value with which the future is completed will indicate the reason for completion.
    */
  def completion: Future[SequencerClient.CloseReason] = closeReasonPromise.future

  def closeSubscription(): Unit = {
    currentSubscription.getAndSet(None).foreach { subscription =>
      import TraceContext.Implicits.Empty.*
      logger.debug(s"Closing sequencer subscription...")
      subscription.close()
      logger.trace(s"Wait for the subscription to complete")
      timeouts.shutdownNetwork.await_()(subscription.closeReason)

      logger.trace(s"Wait for the handler to become idle")

      // This logs a warn if the handle does not become idle within 60 seconds.
      // This happen because the handler is not making progress, for example due to a db outage.
      timeouts.shutdownProcessing
        .valueOrLog(s"Clean close of the sequencer subscription $subscription timed out")(
          handlerIdle.get().future
        )
    }
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    Seq(
      // see comments above why we need two flushes
      flushCloseable("sequencer-client-flush-sync", timeouts.shutdownProcessing),
      flushCloseable("sequencer-client-flush-async", timeouts.shutdownProcessing),
      SyncCloseable(
        "sequencer-client-periodic-ack",
        toCloseableOption(periodicAcknowledgementsRef.get()).close(),
      ),
      SyncCloseable("sequencer-client-subscription", closeSubscription()),
      SyncCloseable("sequencer-client-transport", transport.close()),
      SyncCloseable("sequencer-client-recorder", recorderO.foreach(_.close())),
      SyncCloseable("sequencer-send-tracker", sendTracker.close()),
      SyncCloseable("sequenced-event-store", sequencedEventStore.close()),
      SyncCloseable(
        "sequencer-client-close-reason", {
          val _ =
            closeReasonPromise.tryComplete(Success(SequencerClient.CloseReason.ClientShutdown))
        },
      ),
    )
  }

  /** Returns a future that completes after asynchronous processing has completed for all events
    * whose synchronous processing has been completed prior to this call. May complete earlier if event processing
    * has failed.
    */
  @VisibleForTesting
  def flush(): Future[Unit] = doFlush()
}

trait SequencerClientFactory {
  def create(
      member: Member,
      sequencedEventStore: SequencedEventStore,
      sendTrackerStore: SendTrackerStore,
      signSubmission: TraceContext => SubmissionRequest => EitherT[Future, String, SignedContent[
        SubmissionRequest
      ]],
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClient]

}

trait SequencerClientTransportFactory {
  def makeTransport(
      connection: SequencerConnection,
      member: Member,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClientTransport]
}

object SequencerClient {

  def apply(
      connection: SequencerConnection,
      domainId: DomainId,
      sequencerId: SequencerId,
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      crypto: Crypto,
      agreedAgreementId: Option[ServiceAgreementId],
      config: SequencerClientConfig,
      traceContextPropagation: TracingConfig.Propagation,
      testingConfig: TestingConfigInternal,
      domainParameters: StaticDomainParameters,
      processingTimeout: ProcessingTimeout,
      clock: Clock,
      recordingConfigForMember: Member => Option[RecordingConfig],
      replayConfigForMember: Member => Option[ReplayConfig],
      metrics: SequencerClientMetrics,
      loggingConfig: LoggingConfig,
      loggerFactory: NamedLoggerFactory,
      supportedProtocolVersions: Seq[ProtocolVersion],
      minimumProtocolVersion: Option[ProtocolVersion],
  ): SequencerClientFactory with SequencerClientTransportFactory =
    new SequencerClientFactory with SequencerClientTransportFactory {
      override def create(
          member: Member,
          sequencedEventStore: SequencedEventStore,
          sendTrackerStore: SendTrackerStore,
          signSubmission: TraceContext => SubmissionRequest => EitherT[
            Future,
            String,
            SignedContent[
              SubmissionRequest
            ],
          ],
      )(implicit
          executionContext: ExecutionContextExecutor,
          materializer: Materializer,
          tracer: Tracer,
          traceContext: TraceContext,
      ): EitherT[Future, String, SequencerClient] = {
        // initialize recorder if it's been configured for the member (should only be used for testing)
        val recorderO = recordingConfigForMember(member).map { recordingConfig =>
          new SequencerClientRecorder(
            recordingConfig.fullFilePath,
            processingTimeout,
            loggerFactory,
          )
        }

        for {
          transport <- makeTransport(
            connection,
            member,
          )
          // fetch the initial set of pending sends to initialize the client with.
          // as it owns the client that should be writing to this store it should not be racy.
          initialPendingSends <- EitherT.right(sendTrackerStore.fetchPendingSends)
          sendTracker = new SendTracker(
            initialPendingSends,
            sendTrackerStore,
            metrics,
            loggerFactory,
          )
          // pluggable send approach to support transitioning to the new async sends
          validatorFactory = new SequencedEventValidatorFactory {
            override def create(
                initialLastEventProcessedO: Option[PossiblyIgnoredSerializedEvent],
                unauthenticated: Boolean,
            )(implicit loggingContext: NamedLoggingContext): SequencedEventValidator =
              if (config.skipSequencedEventValidation) {
                SequencedEventValidator.noValidation(domainId, sequencerId)(
                  NamedLoggingContext(loggerFactory, TraceContext.empty)
                )
              } else {
                new SequencedEventValidatorImpl(
                  initialLastEventProcessedO,
                  unauthenticated,
                  config.optimisticSequencedEventValidation,
                  domainId,
                  sequencerId,
                  syncCryptoApi,
                  loggerFactory,
                )
              }
          }
        } yield new SequencerClient(
          domainId,
          member,
          transport,
          config,
          testingConfig,
          domainParameters,
          processingTimeout,
          validatorFactory,
          clock,
          signSubmission,
          sequencedEventStore,
          sendTracker,
          metrics,
          recorderO,
          replayConfigForMember(member).isDefined,
          loggingConfig,
          loggerFactory,
        )
      }

      def makeTransport(
          connection: SequencerConnection,
          member: Member,
      )(implicit
          executionContext: ExecutionContextExecutor,
          materializer: Materializer,
          traceContext: TraceContext,
      ): EitherT[Future, String, SequencerClientTransport] = {
        def mkRealTransport: EitherT[Future, String, SequencerClientTransport] =
          connection match {
            case http: HttpSequencerConnection => httpTransport(http)
            case grpc: GrpcSequencerConnection => grpcTransport(grpc, member).toEitherT
          }

        val transportEitherT: EitherT[Future, String, SequencerClientTransport] =
          replayConfigForMember(member) match {
            case None => mkRealTransport
            case Some(ReplayConfig(recording, SequencerEvents)) =>
              EitherT.rightT(
                new ReplayingEventsSequencerClientTransport(
                  domainParameters.protocolVersion,
                  recording.fullFilePath,
                  metrics,
                  processingTimeout,
                  loggerFactory,
                )
              )
            case Some(ReplayConfig(recording, replaySendsConfig: SequencerSends)) =>
              for {
                underlyingTransport <- mkRealTransport
              } yield new ReplayingSendsSequencerClientTransport(
                domainParameters.protocolVersion,
                recording.fullFilePath,
                replaySendsConfig,
                member,
                underlyingTransport,
                metrics,
                processingTimeout,
                loggerFactory,
              )
          }

        for {
          transport <- transportEitherT
          // handshake to check that sequencer client supports the protocol version required by the sequencer
          _ <- SequencerHandshake.handshake(
            supportedProtocolVersions,
            minimumProtocolVersion,
            transport,
            config,
            processingTimeout,
            loggerFactory,
          )
        } yield transport
      }

      private def httpTransport(connection: HttpSequencerConnection)(implicit
          executionContext: ExecutionContext,
          traceContext: TraceContext,
      ): EitherT[Future, String, SequencerClientTransport] =
        HttpSequencerClient(
          crypto,
          connection,
          processingTimeout,
          traceContextPropagation,
          loggerFactory,
        ).map(client => new HttpSequencerClientTransport(client, processingTimeout, loggerFactory))

      private def grpcTransport(connection: GrpcSequencerConnection, member: Member)(implicit
          executionContext: ExecutionContextExecutor
      ): Either[String, SequencerClientTransport] = {
        def createChannel(conn: GrpcSequencerConnection) = {
          val channelBuilder = ClientChannelBuilder(loggerFactory)
          GrpcSequencerChannelBuilder(
            channelBuilder,
            conn,
            domainParameters.maxInboundMessageSize,
            traceContextPropagation,
            config.keepAliveClient,
          )
        }
        val channel = createChannel(connection)
        val auth = {
          val channelPerEndpoint = connection.endpoints.map { endpoint =>
            val subConnection = connection.copy(endpoints = NonEmpty.mk(Seq, endpoint))
            endpoint -> createChannel(subConnection)
          }.toMap
          new GrpcSequencerClientAuth(
            domainId,
            member,
            crypto,
            agreedAgreementId,
            channelPerEndpoint,
            supportedProtocolVersions,
            config.authToken,
            clock,
            processingTimeout,
            loggerFactory,
          )
        }
        val callOptions = {
          // the wait-for-ready call option is added for when round-robin-ing through connections
          // so that if one of them gets closed, we try the next one instead of unnecessarily failing.
          // wait-for-ready semantics: https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md
          // this is safe for non-idempotent RPCs.
          if (connection.endpoints.length > 1) CallOptions.DEFAULT.withWaitForReady()
          else CallOptions.DEFAULT
        }
        Right(
          new GrpcSequencerClientTransport(
            channel,
            callOptions,
            auth,
            metrics,
            processingTimeout,
            loggerFactory,
          )
        )
      }
    }

  sealed trait CloseReason

  object CloseReason {

    trait ErrorfulCloseReason

    case class PermissionDenied(cause: String) extends CloseReason

    case class UnrecoverableError(cause: String) extends ErrorfulCloseReason with CloseReason

    case class UnrecoverableException(throwable: Throwable)
        extends ErrorfulCloseReason
        with CloseReason

    case object ClientShutdown extends CloseReason

    case object BecamePassive extends CloseReason
  }

  /** Hook for informing tests about replay statistics.
    *
    * If a [[SequencerClient]] is used with
    * [[transports.replay.ReplayingEventsSequencerClientTransport]], the transport
    * will add a statistics to this queue whenever a replay attempt has completed successfully.
    *
    * A test can poll this statistics from the queue to determine whether the replay has completed and to
    * get statistics on the replay.
    *
    * LIMITATION: This is only suitable for manual / sequential test setups, as the statistics are shared through
    * a global queue.
    */
  @VisibleForTesting
  lazy val replayStatistics: BlockingQueue[ReplayStatistics] = new LinkedBlockingQueue()

  case class ReplayStatistics(
      inputPath: Path,
      numberOfEvents: Int,
      startTime: CantonTimestamp,
      duration: JDuration,
  )

  /** Utility to add retries around sends as an attempt to guarantee the send is eventually sequenced.
    */
  def sendWithRetries(
      sendBatch: SendCallback => EitherT[Future, SendAsyncClientError, Unit],
      maxRetries: Int,
      delay: FiniteDuration,
      sendDescription: String,
      errMsg: String,
      flagCloseable: FlagCloseable,
  )(implicit ec: ExecutionContext, loggingContext: ErrorLoggingContext): Future[Unit] = {
    def doSend(): Future[Unit] = {
      val callback = new SendCallbackWithFuture()
      for {
        _ <- EitherTUtil.toFuture(
          EitherTUtil
            .logOnError(sendBatch(callback), errMsg)
            .leftMap(err => new RuntimeException(s"$errMsg: $err"))
        )
        _ <- callback.result
          .map(SendResult.toTry(sendDescription))
          .flatMap(Future.fromTry)
      } yield ()
    }
    retry
      .Pause(loggingContext.logger, flagCloseable, maxRetries, delay, sendDescription)
      .apply(doSend(), AllExnRetryable)(retry.Success.always, ec, loggingContext.traceContext)
  }

  def signSubmissionRequest(topologyClient: DomainSyncCryptoClient)(implicit
      ec: ExecutionContext
  ): TraceContext => SubmissionRequest => EitherT[Future, String, SignedContent[
    SubmissionRequest
  ]] = { implicit traceContext => request =>
    val snapshot = topologyClient.headSnapshot
    SignedContent
      .create(
        topologyClient.pureCrypto,
        snapshot,
        request,
        Some(snapshot.ipsSnapshot.timestamp),
        HashPurpose.SubmissionRequestSignature,
      )
      .leftMap(_.toString)
  }
}
