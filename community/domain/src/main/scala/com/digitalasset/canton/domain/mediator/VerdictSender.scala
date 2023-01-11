// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.MediatorMessageId.VerdictMessageId
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  MediatorRequest,
  SignedProtocolMessage,
  Verdict,
}
import com.digitalasset.canton.protocol.{RequestId, v0}
import com.digitalasset.canton.sequencing.client.{
  SendCallback,
  SendResult,
  SendType,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  DeliverErrorReason,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[mediator] trait VerdictSender {
  def sendResult(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit]

  def sendResultBatch(
      requestId: RequestId,
      batch: Batch[DefaultOpenEnvelope],
      decisionTime: CantonTimestamp,
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
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val resultET = for {
      batch <- createResults(requestId, request, verdict)
      _ <- EitherT.right[SyncCryptoError](
        sendResultBatch(requestId, batch, decisionTime)
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
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val callback: SendCallback = {
      case SendResult.Success(_) =>
        logger.debug(s"Sent result for request ${requestId.unwrap}")
      case SendResult.Error(error) =>
        val reason = error.reason
        reason match {
          case _: DeliverErrorReason.BatchRefused =>
            logger.warn(
              s"Result message was refused for $requestId: $reason"
            )
          case _ =>
            logger.error(
              s"Failed to send result message for $requestId: $reason"
            )
        }
      case _: SendResult.Timeout =>
        logger.warn("Sequencing result message timed out.")
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
    )

    EitherTUtil
      .logOnError(sendET, s"Failed to send result to sequencer for request ${requestId.unwrap}")
      .value
      .void
  }

  private def informeesGroupedByParticipant(requestId: RequestId, informees: Set[LfPartyId])(
      implicit traceContext: TraceContext
  ): Future[(Map[ParticipantId, Set[LfPartyId]], Set[LfPartyId])] = {

    def participantInformeeMapping(
        topologySnapshot: TopologySnapshot
    ): Future[(Map[ParticipantId, Set[LfPartyId]], Set[LfPartyId])] = {
      val start = Map.empty[ParticipantId, Set[LfPartyId]]
      val prefetchF =
        informees.toList.parTraverse(partyId =>
          topologySnapshot.activeParticipantsOf(partyId).map(res => (partyId, res))
        )
      prefetchF.map { fetched =>
        fetched.foldLeft((start, Set.empty[LfPartyId]))({
          case (
                (participantsToInformees, informeesNoParticipant),
                (informee, activeParticipants),
              ) =>
            val activeParticipantsOfInformee = activeParticipants.keySet
            if (activeParticipantsOfInformee.isEmpty)
              (participantsToInformees, informeesNoParticipant + informee)
            else {
              val updatedMap = activeParticipantsOfInformee.foldLeft(participantsToInformees)({
                case (map, participant) =>
                  map + (participant -> (map.getOrElse(participant, Set.empty) + informee))
              })
              (updatedMap, informeesNoParticipant)
            }
        })
      }
    }

    crypto.ips.awaitSnapshot(requestId.unwrap).flatMap(participantInformeeMapping)
  }

  private[this] def createResults(requestId: RequestId, request: MediatorRequest, verdict: Verdict)(
      implicit traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Batch[DefaultOpenEnvelope]] =
    for {
      informeesMapAndAnyInformeeNoActiveParticipant <- EitherT.right(
        informeesGroupedByParticipant(requestId, request.allInformees)
      )
      (informeesMap, informeesNoParticipant) = informeesMapAndAnyInformeeNoActiveParticipant
      snapshot <- EitherT.right(crypto.awaitSnapshot(requestId.unwrap))
      verdictWithInformeeCheck = {
        if (informeesNoParticipant.nonEmpty) {
          MediatorError.InvalidMessage.Reject(
            show"Rejected transaction due to informees not being hosted on an active participant: $informeesNoParticipant",
            v0.MediatorRejection.Code.InformeesNotHostedOnActiveParticipant,
          )(verdict.representativeProtocolVersion)
        } else verdict
      }
      envelopes <- informeesMap.toList
        .parTraverse { case (participantId, informees) =>
          val result = request.createMediatorResult(requestId, verdictWithInformeeCheck, informees)
          SignedProtocolMessage
            .create(result, snapshot, protocolVersion)
            .map(signedResult =>
              OpenEnvelope(signedResult, Recipients.cc(participantId), protocolVersion)
            )
        }
    } yield Batch(envelopes, protocolVersion)
}
