// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Applicative
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, OtherError}
import com.digitalasset.canton._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.version.VersionedSequencedEvent
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{MemoizedEvidence, ProtoConverter}
import com.digitalasset.canton.util.{HasVersionedWrapper, NoCopy}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** The Deliver events are received as a consequence of a '''Send''' command, received by the recipients of the
  * originating '''Send''' event.
  */
sealed trait SequencedEvent[+Env]
    extends Product
    with Serializable
    with MemoizedEvidence
    with PrettyPrinting
    with HasVersionedWrapper[VersionedSequencedEvent] {

  /** a sequence counter for each recipient.
    */
  val counter: SequencerCounter

  /** a timestamp defining the order (requestId)
    */
  val timestamp: CantonTimestamp

  /** The domain which this deliver event belongs to */
  val domainId: DomainId

  val isDeliver: Boolean = {
    this match {
      case Deliver(_, _, _, _, _) => true
      case _ => false
    }
  }

  protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  // TODO(Andreas): Restrict visibility of this method
  private[sequencing] def traverse[F[_], Env2 <: Envelope[_]](f: Env => F[Env2])(implicit
      F: Applicative[F]
  ): F[SequencedEvent[Env2]]
}

object SequencedEvent {

  def fromByteString[Env <: Envelope[_]](
      envelopeDeserializer: v0.Envelope => ParsingResult[Env]
  )(bytes: ByteString): ParsingResult[SequencedEvent[Env]] =
    ProtoConverter
      .protoParser(VersionedSequencedEvent.parseFrom)(bytes)
      .flatMap(fromProtoWith(envelopeDeserializer)(_, bytes))

  private[sequencing] def fromProtoWith[Env <: Envelope[_]](
      envelopeDeserializer: v0.Envelope => ParsingResult[Env]
  )(
      sequencedEventP: VersionedSequencedEvent,
      bytes: ByteString,
  ): ParsingResult[SequencedEvent[Env]] = {
    sequencedEventP.version match {
      case VersionedSequencedEvent.Version.Empty =>
        Left(FieldNotSet("VersionedSequencedEvent.version"))
      case VersionedSequencedEvent.Version.V0(event) =>
        fromProtoWithV0(envelopeDeserializer)(event, bytes)
    }
  }

  private[sequencing] def fromProtoWithV0[Env <: Envelope[_]](
      envelopeDeserializer: v0.Envelope => ParsingResult[Env]
  )(
      sequencedEventP: v0.SequencedEvent,
      bytes: ByteString,
  ): ParsingResult[SequencedEvent[Env]] = {
    import cats.syntax.traverse._
    val v0.SequencedEvent(counter, tsP, domainIdP, mbMsgIdP, mbBatchP, mbDeliverErrorReasonP) =
      sequencedEventP
    for {
      timestamp <- ProtoConverter
        .required("SequencedEvent.timestamp", tsP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "SequencedEvent.domainId")
      mbBatch <- mbBatchP.traverse(Batch.fromProtoV0(envelopeDeserializer))
      mbDeliverErrorReason <- mbDeliverErrorReasonP.traverse(DeliverErrorReason.fromProtoV0)
      // errors have an error reason, delivers have a batch
      event <- ((mbDeliverErrorReason, mbBatch) match {
        case (Some(_), Some(_)) =>
          Left(OtherError("SequencedEvent cannot have both a deliver error and batch set"))
        case (None, None) =>
          Left(OtherError("SequencedEvent cannot have neither a deliver error nor a batch set"))
        case (Some(deliverErrorReason), None) =>
          for {
            msgId <- ProtoConverter
              .required("DeliverError", mbMsgIdP)
              .flatMap(MessageId.fromProtoPrimitive)
          } yield new DeliverError(counter, timestamp, domainId, msgId, deliverErrorReason)(
            Some(bytes)
          )
        case (None, Some(batch)) =>
          mbMsgIdP match {
            case None => Right(new Deliver(counter, timestamp, domainId, None, batch)(Some(bytes)))
            case Some(msgId) =>
              MessageId
                .fromProtoPrimitive(msgId)
                .flatMap(e =>
                  Right(new Deliver(counter, timestamp, domainId, Some(e), batch)(Some(bytes)))
                )
          }
      }): ParsingResult[SequencedEvent[Env]]
    } yield event
  }
}

case class DeliverError private[sequencing] (
    override val counter: SequencerCounter,
    override val timestamp: CantonTimestamp,
    override val domainId: DomainId,
    messageId: MessageId,
    reason: DeliverErrorReason,
)(val deserializedFrom: Option[ByteString])
    extends SequencedEvent[Nothing]
    with NoCopy {
  override def toProtoVersioned(version: ProtocolVersion): VersionedSequencedEvent =
    VersionedSequencedEvent(VersionedSequencedEvent.Version.V0(toProtoV0))

  def toProtoV0: v0.SequencedEvent = v0.SequencedEvent(
    counter = counter,
    timestamp = Some(timestamp.toProtoPrimitive),
    domainId = domainId.toProtoPrimitive,
    messageId = Some(messageId.toProtoPrimitive),
    batch = None,
    deliverErrorReason = Some(reason.toProtoV0),
  )

  private[sequencing] override def traverse[F[_], Env](f: Nothing => F[Env])(implicit
      F: Applicative[F]
  ): F[SequencedEvent[Env]] =
    F.pure(this)

  override def pretty: Pretty[DeliverError] = prettyOfClass(
    param("counter", _.counter),
    param("timestamp", _.timestamp),
    param("domain id", _.domainId),
    param("message id", _.messageId),
    param("reason", _.reason),
  )

}

object DeliverError {
  private[this] def apply(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageId: MessageId,
      reason: DeliverErrorReason,
  )(deserializedFrom: Option[ByteString]) =
    throw new UnsupportedOperationException("Use the public apply method instead")

  def create(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageId: MessageId,
      reason: DeliverErrorReason,
  ) =
    new DeliverError(counter, timestamp, domainId, messageId, reason)(None)
}

/** Intuitively, the member learns all envelopes addressed to it. It learns some recipients of
  * these envelopes, as defined by
  * [[com.digitalasset.canton.sequencing.protocol.Recipients.forMember]]
  *
  * @param counter   a monotonically increasing counter for each recipient.
  * @param timestamp a timestamp defining the order.
  * @param messageId   populated with the message id used on the originating send operation only for the sender
  * @param batch     a batch of envelopes.
  */
case class Deliver[+Env <: Envelope[_]] private[sequencing] (
    override val counter: SequencerCounter,
    override val timestamp: CantonTimestamp,
    override val domainId: DomainId,
    messageId: Option[MessageId],
    batch: Batch[Env],
)(val deserializedFrom: Option[ByteString])
    extends SequencedEvent[Env]
    with NoCopy {

  /** Is this deliver event a receipt for a message that the receiver previously sent?
    * (messageId is only populated for the sender)
    */
  lazy val isReceipt: Boolean = messageId.isDefined

  override def toProtoVersioned(version: ProtocolVersion): VersionedSequencedEvent =
    VersionedSequencedEvent(VersionedSequencedEvent.Version.V0(toProtoV0(version)))

  def toProtoV0(version: ProtocolVersion): v0.SequencedEvent = v0.SequencedEvent(
    counter = counter,
    timestamp = Some(timestamp.toProtoPrimitive),
    domainId = domainId.toProtoPrimitive,
    messageId = messageId.map(_.toProtoPrimitive),
    batch = Some(batch.toProtoV0(version)),
    deliverErrorReason = None,
  )

  override private[sequencing] def traverse[F[_], Env2 <: Envelope[_]](
      f: Env => F[Env2]
  )(implicit F: Applicative[F]) =
    F.map(batch.traverse(f))(
      new Deliver(counter, timestamp, domainId, messageId, _)(deserializedFrom)
    )

  @VisibleForTesting
  private[canton] def copy[Env2 >: Env <: Envelope[_]](
      counter: SequencerCounter = this.counter,
      timestamp: CantonTimestamp = this.timestamp,
      domainId: DomainId = this.domainId,
      messageId: Option[MessageId] = this.messageId,
      batch: Batch[Env2] = this.batch,
  ): Deliver[Env2] =
    new Deliver[Env2](counter, timestamp, domainId, messageId, batch)(None)

  override def pretty: Pretty[this.type] =
    prettyOfClass(
      param("counter", _.counter),
      param("timestamp", _.timestamp),
      unnamedParam(_.batch),
    )
}

object Deliver {
  private[this] def apply[Env <: Envelope[_]](
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageId: Option[MessageId],
      batch: Batch[Env],
  )(deserializedFrom: Option[ByteString]) =
    throw new UnsupportedOperationException("Use the public apply method instead")

  def create[Env <: Envelope[_]](
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageId: Option[MessageId],
      batch: Batch[Env],
  ) =
    new Deliver[Env](counter, timestamp, domainId, messageId, batch)(None)

  def fromSequencedEvent[Env <: Envelope[_]](
      deliverEvent: SequencedEvent[Env]
  ): Option[Deliver[Env]] =
    deliverEvent match {
      case deliver @ Deliver(_, _, _, _, _) => Some(deliver)
      case _: DeliverError => None
    }
}
