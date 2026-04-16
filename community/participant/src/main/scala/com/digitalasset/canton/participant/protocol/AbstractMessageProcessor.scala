// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.crypto.{SynchronizerCryptoClient, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.SequencedEventUpdate
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessSet
import com.digitalasset.canton.participant.sync.SyncEphemeralState
import com.digitalasset.canton.protocol.Phase37Processor.PublishUpdateViaRecordOrderPublisher
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResponses,
  ProtocolMessage,
  SignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.client.SequencerClientSend.SendRequestTimestamps
import com.digitalasset.canton.sequencing.client.{
  BatchingSendQueue,
  SendAsyncClientError,
  SendCallback,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, MessageId, Recipients}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, LoggerUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

/** Collects helper methods for message processing */
abstract class AbstractMessageProcessor(
    ephemeral: SyncEphemeralState,
    crypto: SynchronizerCryptoClient,
    sequencerClient: SequencerClientSend,
    clock: Clock,
    protocolVersion: ProtocolVersion,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  /** Batching queue for confirmation response sends. Drains on each EC cycle,
    * coalescing concurrent sendAsync calls into tight back-to-back bursts.
    */
  private val responseBatchingSendQueue = new BatchingSendQueue(sequencerClient, loggerFactory)

  private def psid = sequencerClient.psid

  protected def terminateRequest(
      requestCounter: RequestCounter,
      requestTimestamp: CantonTimestamp,
      commitTime: CantonTimestamp,
      eventO: Option[SequencedEventUpdate],
      publishUpdate: PublishUpdateViaRecordOrderPublisher[SequencedEventUpdate],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      _ <- ephemeral.requestJournal.terminate(
        requestCounter,
        requestTimestamp,
        commitTime,
      )
    } yield publishUpdate(eventO)

  /** A clean replay replays a request whose request counter is below the clean head in the request
    * journal. Since the replayed request is clean, its effects are not persisted.
    */
  protected def isCleanReplay(requestCounter: RequestCounter): Boolean =
    requestCounter < ephemeral.startingPoints.processing.nextRequestCounter

  private def unlessCleanReplay(requestCounter: RequestCounter)(
      f: => FutureUnlessShutdown[?]
  ): FutureUnlessShutdown[Unit] =
    if (isCleanReplay(requestCounter)) FutureUnlessShutdown.unit else f.void

  protected def signResponses(
      ips: SynchronizerSnapshotSyncCryptoApi,
      responses: ConfirmationResponses,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SignedProtocolMessage[ConfirmationResponses]] =
    SignedProtocolMessage.trySignAndCreate(
      responses,
      ips,
      None, // `ConfirmationResponses` are always signed with a `fixed` timestamp.
    )

  // Assumes that we are not closing (i.e., that this is synchronized with shutdown somewhere higher up the call stack)
  protected def sendResponses(
      requestId: RequestId,
      messages: Seq[(ProtocolMessage, Recipients)],
      // use client.messageId. passed in here such that we can log it before sending
      messageId: Option[MessageId] = None,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    implicit val metricsContext: MetricsContext = MetricsContext(
      "type" -> "send-confirmation-response"
    )
    if (messages.isEmpty) FutureUnlessShutdown.unit
    else {
      logger.trace(s"Request $requestId: ProtocolProcessor scheduling the sending of responses")
      def errorBody = s"Request $requestId: Failed to send responses"

      val topologyTimestamp =
        if (protocolVersion >= ProtocolVersion.v35) None else Some(requestId.unwrap)

      for {
        synchronizerParameters <- crypto.ips
          .awaitSnapshot(requestId.unwrap)
          .flatMap(snapshot =>
            snapshot.findDynamicSynchronizerParametersOrDefault(psid.protocolVersion)
          )

        maxSequencingTime = requestId.unwrap.add(
          synchronizerParameters.confirmationResponseTimeout.unwrap
        )

        // Use drain-the-queue batching: queue the submission and let the
        // BatchingSendQueue flush it alongside other concurrent responses.
        // Under load, 10-20 confirmation responses coalesce into one flush
        // cycle, reducing per-message TCP/gRPC overhead by 2-3.6x.
        _ = responseBatchingSendQueue.sendAsync(
            Batch.of(psid.protocolVersion, messages*),
            timestamps = SendRequestTimestamps(
              topologyTimestamp = topologyTimestamp,
              approximateTimestampForSigning = clock.now,
              maxSequencingTime = maxSequencingTime,
            ),
            messageId = messageId.getOrElse(MessageId.randomMessageId()),
            aggregationRule = None,
            callback = SendCallback.log(s"Response message for request [$requestId]", logger),
            amplify = true,
            useConfirmationResponseAmplificationParameters = true,
          )
      } yield ()
    }
  }

  /** Immediately moves the request to Confirmed and register a timeout handler at the decision time
    * with the request tracker to cover the case that the mediator does not send a confirmation
    * result.
    */
  protected def prepareForMediatorResultOfBadRequest(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      publishUpdate: PublishUpdateViaRecordOrderPublisher[SequencedEventUpdate],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    crypto.ips
      .awaitSnapshot(timestamp)
      .flatMap(snapshot => snapshot.findDynamicSynchronizerParameters())
      .flatMap { synchronizerParametersE =>
        val decisionTimeE = synchronizerParametersE.flatMap(_.decisionTimeFor(timestamp))
        val decisionTimeF = decisionTimeE.fold(
          err => FutureUnlessShutdown.failed(new IllegalStateException(err)),
          FutureUnlessShutdown.pure,
        )

        def onTimeout: FutureUnlessShutdown[Unit] = {
          logger.debug(
            s"Bad request $requestCounter: Timed out without a confirmation result message."
          )
          synchronizeWithClosing(functionFullName) {
            decisionTimeF.flatMap(
              terminateRequest(
                requestCounter,
                timestamp,
                _,
                None,
                publishUpdate,
              )
            )

          }
        }

        registerRequestWithTimeout(
          requestCounter,
          sequencerCounter,
          timestamp,
          decisionTimeF,
          onTimeout,
        )
      }

  private def registerRequestWithTimeout(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      decisionTimeF: FutureUnlessShutdown[CantonTimestamp],
      onTimeout: => FutureUnlessShutdown[Unit],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      decisionTime <- decisionTimeF
      requestFutures <- ephemeral.requestTracker
        .addRequest(
          requestCounter,
          sequencerCounter,
          timestamp,
          timestamp,
          decisionTime,
          ActivenessSet.empty,
        )
        .valueOr(error =>
          ErrorUtil.internalError(new IllegalStateException(show"Request already exists: $error"))
        )
      _ <-
        unlessCleanReplay(requestCounter)(
          ephemeral.requestJournal.insert(requestCounter, timestamp)
        )
      _ <- requestFutures.activenessResult

      _ =
        if (!isCleanReplay(requestCounter)) {
          val timeoutF =
            requestFutures.timeoutResult.flatMap { timeoutResult =>
              if (timeoutResult.timedOut) onTimeout
              else FutureUnlessShutdown.unit
            }
          FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(timeoutF, "Handling timeout failed")
        }
    } yield ()

  /** Transition the request to Clean without doing anything */
  protected def invalidRequest(
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      eventO: Option[SequencedEventUpdate],
      publishUpdate: PublishUpdateViaRecordOrderPublisher[SequencedEventUpdate],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // Let the request immediately timeout (upon the next message) rather than explicitly adding an empty commit set
    // because we don't have a sequencer counter to associate the commit set with.
    val decisionTime = timestamp.immediateSuccessor
    registerRequestWithTimeout(
      requestCounter,
      sequencerCounter,
      timestamp,
      FutureUnlessShutdown.pure(decisionTime),
      terminateRequest(
        requestCounter,
        timestamp,
        decisionTime,
        eventO,
        publishUpdate,
      ),
    )
  }
}
