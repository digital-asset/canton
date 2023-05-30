// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.MediatorMessageId.VerdictMessageId
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  MediatorRequest,
  SignedProtocolMessage,
  Verdict,
}
import com.digitalasset.canton.sequencing.client.{
  SendCallback,
  SendResult,
  SendType,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  DeliverErrorReason,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[mediator] trait VerdictSender {
  def sendResult(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
  )(implicit traceContext: TraceContext): Future[Unit]

  def sendResultBatch(
      requestId: RequestId,
      batch: Batch[DefaultOpenEnvelope],
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
  )(implicit traceContext: TraceContext): Future[Unit]
}

private[mediator] object VerdictSender {
  def apply(
      sequencerSend: SequencerClientSend,
      crypto: DomainSyncCryptoClient,
      protocolVersion: ProtocolVersion,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): VerdictSender =
    new DefaultVerdictSender(sequencerSend, crypto, protocolVersion, loggerFactory)
}

private[mediator] class DefaultVerdictSender(
    sequencerSend: SequencerClientSend,
    crypto: DomainSyncCryptoClient,
    protocolVersion: ProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends VerdictSender
    with NamedLogging {
  override def sendResult(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val resultET = for {
      batch <- createResults(requestId, request, verdict)
      _ <- EitherT.right[SyncCryptoError](
        sendResultBatch(requestId, batch, decisionTime, aggregationRule)
      )
    } yield ()

    // we don't want to halt the mediator if an individual send fails or if we're unable to create a batch, so just log
    resultET
      .leftMap(err => logger.warn(s"Failed to create or send result message for $requestId: $err"))
      .value
      .map(_.merge)
  }

  override def sendResultBatch(
      requestId: RequestId,
      batch: Batch[DefaultOpenEnvelope],
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val callback: SendCallback = {
      case UnlessShutdown.Outcome(SendResult.Success(_)) =>
        logger.debug(s"Sent result for request ${requestId.unwrap}")
      case UnlessShutdown.Outcome(SendResult.Error(error)) =>
        val reason = error.reason
        reason match {
          case _: DeliverErrorReason.BatchRefused =>
            // TODO(i13155):
            if (reason.message.contains("was previously delivered at")) {
              logger.info(
                s"Result message was refused for $requestId: $reason"
              )
            } else {
              logger.warn(
                s"Result message was refused for $requestId: $reason"
              )
            }
          case _ =>
            logger.error(
              s"Failed to send result message for $requestId: $reason"
            )
        }
      case UnlessShutdown.Outcome(_: SendResult.Timeout) =>
        logger.warn("Sequencing result message timed out.")
      case UnlessShutdown.AbortedDueToShutdown =>
        logger.debug("Sequencing result processing was aborted due to shutdown")
    }

    // the result of send request will be logged within the returned future however any error is effectively
    // discarded. Any error logged by the eventual callback will most likely occur after the returned future has
    // completed.
    // we use decision-time for max-sequencing-time as recipients will simply ignore the message if received after
    // that point.
    val sendET = sequencerSend.sendAsync(
      batch,
      SendType.Other,
      Some(requestId.unwrap),
      callback = callback,
      maxSequencingTime = decisionTime,
      messageId = VerdictMessageId(requestId).toMessageId,
      aggregationRule = aggregationRule,
    )

    EitherTUtil
      .logOnError(sendET, s"Failed to send result to sequencer for request ${requestId.unwrap}")
      .value
      .void
  }

  private[this] def createResults(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Batch[DefaultOpenEnvelope]] =
    for {
      snapshot <- EitherT.right(crypto.awaitSnapshot(requestId.unwrap))
      informeesMap <- EitherT.right(
        informeesByParticipant(request.allInformees.toList, snapshot.ipsSnapshot)
      )
      envelopes <- {
        if (protocolVersion >= ProtocolVersion.v5) {
          val result = request.createMediatorResult(requestId, verdict, request.allInformees)
          val recipients =
            NonEmpty
              .from(informeesMap.keysIterator.toSeq.map { (r: Member) => NonEmpty(Set, r).toSet })
              .map(Recipients.groups)
              .getOrElse(
                // Should never happen as the topology (same snapshot) is checked in
                // `ConfirmationResponseProcessor.validateRequest`
                ErrorUtil.invalidState("No active participants for informees")
              )

          SignedProtocolMessage
            .signAndCreate(result, snapshot, protocolVersion)
            .map(signedResult => List(OpenEnvelope(signedResult, recipients)(protocolVersion)))
        } else {
          // TODO(i12171): Remove this block in 3.0
          informeesMap.toList
            .parTraverse { case (participantId, informees) =>
              val result = request.createMediatorResult(requestId, verdict, informees)
              SignedProtocolMessage
                .signAndCreate(result, snapshot, protocolVersion)
                .map(signedResult =>
                  OpenEnvelope(signedResult, Recipients.cc(participantId))(protocolVersion)
                )
            }
        }
      }
    } yield Batch(envelopes, protocolVersion)

  private def informeesByParticipant(
      informees: List[LfPartyId],
      topologySnapshot: TopologySnapshot,
  ): Future[Map[ParticipantId, Set[LfPartyId]]] =
    for {
      participantsByParty <- topologySnapshot.activeParticipantsOfParties(informees)
    } yield participantsByParty.foldLeft(Map.empty[ParticipantId, Set[LfPartyId]]) {
      case (acc, (party, participants)) =>
        participants.foldLeft(acc) { case (acc, participant) =>
          val parties = acc.getOrElse(participant, Set.empty) + party
          acc.updated(participant, parties)
        }
    }
}
