// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.Update.ReceivedAcsCommitment
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.pretty.PrettyUtil
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.protocol.v30 as v30participant
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.AcsCommitmentAlarm
import com.digitalasset.canton.protocol.Phase37Processor.PublishUpdateViaRecordOrderPublisher
import com.digitalasset.canton.protocol.messages.AcsCommitmentProtocolMessage
import com.digitalasset.canton.sequencing.protocol.OpenEnvelope
import com.digitalasset.canton.sequencing.{HandlerResult, UnthrottledImmediate}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ReleaseProtocolVersion,
}
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

trait ReceivedAcsCommitmentValidator {
  def validateAndPublish(
      ts: CantonTimestamp,
      envelopes: NonEmpty[Seq[OpenEnvelope[AcsCommitmentProtocolMessage]]],
      publish: PublishUpdateViaRecordOrderPublisher[ReceivedAcsCommitment],
  )(implicit traceContext: TraceContext): HandlerResult

}

object ReceivedAcsCommitmentValidator {
  object Noop extends ReceivedAcsCommitmentValidator {
    override def validateAndPublish(
        ts: CantonTimestamp,
        envelopes: NonEmpty[Seq[OpenEnvelope[AcsCommitmentProtocolMessage]]],
        publish: PublishUpdateViaRecordOrderPublisher[ReceivedAcsCommitment],
    )(implicit traceContext: TraceContext): HandlerResult = {
      publish.apply(None)
      HandlerResult.done
    }
  }
}

class ReceivedAcsCommitmentValidatorImpl(
    physicalSynchronizerId: PhysicalSynchronizerId,
    participantId: ParticipantId,
    synchronizerCrypto: SyncCryptoClient[SyncCryptoApi],
    metrics: CommitmentMetrics,
    validationParallelism: PositiveInt,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ReceivedAcsCommitmentValidator
    with NamedLogging {

  override def validateAndPublish(
      ts: CantonTimestamp,
      envelopes: NonEmpty[Seq[OpenEnvelope[AcsCommitmentProtocolMessage]]],
      publish: PublishUpdateViaRecordOrderPublisher[ReceivedAcsCommitment],
  )(implicit traceContext: TraceContext): HandlerResult =
    HandlerResult.asynchronous {
      MonadUtil
        .parTraverseFilterWithLimit(validationParallelism)(envelopes)(
          validateEnvelope(ts, _)
        )
        .map { messages =>
          NonEmpty.from(messages) match {
            case Some(validatedMessages) =>
              updateMetrics(validatedMessages)
              publish(Some(toReceivedAcsCommitment(ts, validatedMessages)))
            case None => publish(None)
          }
          UnthrottledImmediate
        }
    }

  private def validateEnvelope(
      timestamp: CantonTimestamp,
      envelope: OpenEnvelope[AcsCommitmentProtocolMessage],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AcsCommitmentProtocolMessage]] = {
    val commitment = envelope.protocolMessage.acsCommitment

    val staticValidation = for {
      validCounterParticipant <- Either.cond(
        commitment.counterparticipant == participantId.toLf,
        (),
        s"At $timestamp, (purportedly) ${commitment.sender} sent an ACS commitment to me, but the commitment lists ${commitment.counterparticipant} as the counterparticipant",
      )
      commitmentPeriodEndsInPast <- Either.cond(
        commitment.period.toInclusive <= timestamp,
        (),
        s"Received an ACS commitment with a future timestamp. (Purported) sender: ${commitment.sender}. Timestamp: ${commitment.period.toInclusive}, receive timestamp: $timestamp",
      )
    } yield ()
    val validatedE = for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](staticValidation)
      _ = logger.info(
        s"Checking commitment signature (purportedly by) ${commitment.sender} for period ${commitment.period}"
      )
      _ <- checkCommitmentSignature(envelope.protocolMessage)
    } yield ()
    validatedE.value.map {
      case Right(_) => Some(envelope.protocolMessage)
      case Left(err) =>
        val alarm = AcsCommitmentAlarm.Warn(s"ACS commitment validation failed: $err")
        alarm.report()
        None
    }
  }

  private def checkCommitmentSignature(
      message: AcsCommitmentProtocolMessage
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      cryptoSnapshot <- EitherT.liftF(
        synchronizerCrypto.awaitSnapshot(message.acsCommitment.period.toInclusive)
      )
      result <- AcsCommitmentProtocolMessage.verifySignature(cryptoSnapshot, message)
    } yield result

  private def updateMetrics(messages: NonEmpty[Seq[AcsCommitmentProtocolMessage]]): Unit = {
    val latestPeriodEnd =
      messages.maxBy1(_.acsCommitment.period.toInclusive).acsCommitment.period.toInclusive
    // TODO(#34085) Decide whether we want to label this metric by the sender
    //  (to be clarified with CN - consider that a participant may have many counterparticipants and
    //  therefore we may run into the limit of how many different labels can be attached to a metric)
    metrics.lastIncomingReceived.updateValue(
      // ACS commitments may come in any order. We therefore take the maximum of the period ends.
      _ max latestPeriodEnd.toMicros
    )
    // TODO(#34085) Rework participant latency metrics
  }

  // TODO(#34103) Find a better name for `ReceivedAcsCommitment`. It is odd that the singular contains
  //  multiple received ACS commitment envelopes.
  private def toReceivedAcsCommitment(
      timestamp: CantonTimestamp,
      messages: NonEmpty[Seq[AcsCommitmentProtocolMessage]],
  )(implicit traceContext: TraceContext): ReceivedAcsCommitment = {
    val serializedMessages =
      ReceivedAcsCommitments(messages).toByteString(ReleaseProtocolVersion.acsCommitmentRedesign.v)
    ReceivedAcsCommitment(
      physicalSynchronizerId.logical,
      timestamp,
      serializedMessages,
    )
  }

}

/** Contains the [[com.digitalasset.canton.protocol.messages.AcsCommitmentProtocolMessage]]s that
  * passed validation.
  */
final case class ReceivedAcsCommitments(messages: NonEmpty[Seq[AcsCommitmentProtocolMessage]])
    extends HasVersionedWrapper[ReceivedAcsCommitments] {

  override protected def companionObj: ReceivedAcsCommitments.type = ReceivedAcsCommitments

  def toProtoV30: v30participant.ReceivedAcsCommitments =
    v30participant.ReceivedAcsCommitments(messages.map(_.toByteString))
}

object ReceivedAcsCommitments
    extends HasVersionedMessageCompanion[ReceivedAcsCommitments]
    with PrettyUtil {
  override def name: String = "ReceivedAcsCommitments"

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(-1) -> unsupportedProtoCodec(ProtocolVersion.v34),
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.acsCommitmentRedesign,
        supportedProtoVersion(v30participant.ReceivedAcsCommitments)(fromProtoV30),
        _.toProtoV30,
      ),
    )

  private def fromProtoV30(
      proto: v30participant.ReceivedAcsCommitments
  ): ParsingResult[ReceivedAcsCommitments] =
    for {
      messages <- ProtoConverter.parseRequiredNonEmpty(
        AcsCommitmentProtocolMessage.fromTrustedByteStringPVV,
        "commitment",
        proto.commitment,
      )
    } yield ReceivedAcsCommitments(messages)
}
