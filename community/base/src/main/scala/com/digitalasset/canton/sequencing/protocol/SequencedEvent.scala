// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.Applicative
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, ProtocolMessage}
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.sequencing.{EnvelopeBox, RawSignedContentEnvelopeBox}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion2,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** The Deliver events are received as a consequence of a '''Send''' command, received by the recipients of the
  * originating '''Send''' event.
  */
sealed trait SequencedEvent[+Env <: Envelope[_]]
    extends Product
    with Serializable
    with ProtocolVersionedMemoizedEvidence
    with PrettyPrinting
    with HasProtocolVersionedWrapper[SequencedEvent[Envelope[_]]] {

  @transient override protected lazy val companionObj: SequencedEvent.type = SequencedEvent

  protected def toProtoV0: v0.SequencedEvent
  protected def toProtoV1: v1.SequencedEvent

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
      case _: Deliver[_] => true
      case _: DeliverError => false
    }
  }

  protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  protected def traverse[F[_], Env2 <: Envelope[_]](f: Env => F[Env2])(implicit
      F: Applicative[F]
  ): F[SequencedEvent[Env2]]

  def envelopes: Seq[Env]
}

object SequencedEvent
    extends HasMemoizedProtocolVersionedWrapperCompanion2[
      SequencedEvent[Envelope[_]],
      SequencedEvent[ClosedEnvelope],
    ] {
  override protected def name: String = "SequencedEvent"

  override val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter.mk(ProtocolVersion.v3)(v0.SequencedEvent)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter.mk(
      // TODO(#12373) Adapt when releasing BFT
      ProtocolVersion.dev
    )(v1.SequencedEvent)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  private def fromProtoV0V1(
      counter: Long,
      tsP: Option[com.google.protobuf.timestamp.Timestamp],
      domainIdP: String,
      mbMsgIdP: Option[String],
      deserializedBatch: => ParsingResult[Option[Batch[ClosedEnvelope]]],
      mbDeliverErrorReasonP: Option[v0.DeliverErrorReason],
      bytes: ByteString,
      protoVersion: ProtoVersion,
  ): ParsingResult[SequencedEvent[ClosedEnvelope]] = {
    val protocolVersionRepresentative = protocolVersionRepresentativeFor(protoVersion)
    val sequencerCounter = SequencerCounter(counter)

    for {
      timestamp <- ProtoConverter
        .required("SequencedEvent.timestamp", tsP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "SequencedEvent.domainId")
      mbBatch <- deserializedBatch
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
          } yield new DeliverError(
            sequencerCounter,
            timestamp,
            domainId,
            msgId,
            deliverErrorReason,
          )(
            protocolVersionRepresentative,
            Some(bytes),
          ) {}
        case (None, Some(batch)) =>
          mbMsgIdP match {
            case None =>
              Right(
                Deliver(sequencerCounter, timestamp, domainId, None, batch)(
                  protocolVersionRepresentative,
                  Some(bytes),
                )
              )
            case Some(msgId) =>
              MessageId
                .fromProtoPrimitive(msgId)
                .map(msgId =>
                  Deliver(sequencerCounter, timestamp, domainId, Some(msgId), batch)(
                    protocolVersionRepresentative,
                    Some(bytes),
                  )
                )
          }
      }): ParsingResult[SequencedEvent[ClosedEnvelope]]
    } yield event
  }

  @VisibleForTesting
  private[sequencing] def fromProtoV0(sequencedEventP: v0.SequencedEvent)(
      bytes: ByteString
  ): ParsingResult[SequencedEvent[ClosedEnvelope]] = {
    val v0.SequencedEvent(counter, tsP, domainIdP, mbMsgIdP, mbBatchP, mbDeliverErrorReasonP) =
      sequencedEventP
    lazy val mbBatch = mbBatchP.traverse(
      // TODO(i10428) Prevent zip bombing when decompressing the request
      Batch.fromProtoV0(_, maxRequestSize = MaxRequestSizeToDeserialize.NoLimit)
    )
    fromProtoV0V1(
      counter,
      tsP,
      domainIdP,
      mbMsgIdP,
      mbBatch,
      mbDeliverErrorReasonP,
      bytes,
      ProtoVersion(0),
    )
  }

  private def fromProtoV1(sequencedEventP: v1.SequencedEvent)(
      bytes: ByteString
  ): ParsingResult[SequencedEvent[ClosedEnvelope]] = {
    import cats.syntax.traverse.*
    val v1.SequencedEvent(counter, tsP, domainIdP, mbMsgIdP, mbBatchP, mbDeliverErrorReasonP) =
      sequencedEventP
    lazy val mbBatch = mbBatchP.traverse(
      // TODO(i10428) Prevent zip bombing when decompressing the request
      Batch.fromProtoV1(_, maxRequestSize = MaxRequestSizeToDeserialize.NoLimit)
    )
    fromProtoV0V1(
      counter,
      tsP,
      domainIdP,
      mbMsgIdP,
      mbBatch,
      mbDeliverErrorReasonP,
      bytes,
      ProtoVersion(1),
    )
  }

  def fromByteStringOpen(hashOps: HashOps, protocolVersion: ProtocolVersion)(
      bytes: ByteString
  ): ParsingResult[SequencedEvent[DefaultOpenEnvelope]] =
    fromByteString(bytes).flatMap(_.traverse(_.openEnvelope(hashOps, protocolVersion)))

  implicit val sequencedEventEnvelopeBox: EnvelopeBox[SequencedEvent] =
    new EnvelopeBox[SequencedEvent] {
      override private[sequencing] def traverse[G[_], A <: Envelope[_], B <: Envelope[_]](
          event: SequencedEvent[A]
      )(f: A => G[B])(implicit G: Applicative[G]): G[SequencedEvent[B]] =
        event.traverse(f)
    }

  // It would be nice if we could appeal to a generic composition theorem here,
  // but the `MemoizeEvidence` bound in `SignedContent` doesn't allow a generic `Traverse` instance.
  implicit val signedContentEnvelopeBox: EnvelopeBox[RawSignedContentEnvelopeBox] =
    new EnvelopeBox[RawSignedContentEnvelopeBox] {
      override private[sequencing] def traverse[G[_], Env1 <: Envelope[_], Env2 <: Envelope[_]](
          signedEvent: SignedContent[SequencedEvent[Env1]]
      )(f: Env1 => G[Env2])(implicit G: Applicative[G]): G[RawSignedContentEnvelopeBox[Env2]] =
        signedEvent.traverse(_.traverse(f))
    }

  def openEnvelopes(
      event: SequencedEvent[ClosedEnvelope]
  )(protocolVersion: ProtocolVersion, hashOps: HashOps): (
      SequencedEvent[OpenEnvelope[ProtocolMessage]],
      Seq[ProtoDeserializationError],
  ) = event match {
    case deliver: Deliver[ClosedEnvelope] =>
      Deliver.openEnvelopes(deliver)(protocolVersion, hashOps)
    case deliver: DeliverError => (deliver, Seq.empty)
  }
}

sealed abstract case class DeliverError private[sequencing] (
    override val counter: SequencerCounter,
    override val timestamp: CantonTimestamp,
    override val domainId: DomainId,
    messageId: MessageId,
    reason: DeliverErrorReason,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[SequencedEvent.type],
    override val deserializedFrom: Option[ByteString],
) extends SequencedEvent[Nothing]
    with NoCopy {
  def toProtoV0: v0.SequencedEvent = v0.SequencedEvent(
    counter = counter.toProtoPrimitive,
    timestamp = Some(timestamp.toProtoPrimitive),
    domainId = domainId.toProtoPrimitive,
    messageId = Some(messageId.toProtoPrimitive),
    batch = None,
    deliverErrorReason = Some(reason.toProtoV0),
  )

  def toProtoV1: v1.SequencedEvent = v1.SequencedEvent(
    counter = counter.toProtoPrimitive,
    timestamp = Some(timestamp.toProtoPrimitive),
    domainId = domainId.toProtoPrimitive,
    messageId = Some(messageId.toProtoPrimitive),
    batch = None,
    deliverErrorReason = Some(reason.toProtoV0),
  )

  override protected def traverse[F[_], Env <: Envelope[_]](f: Nothing => F[Env])(implicit
      F: Applicative[F]
  ): F[SequencedEvent[Env]] = F.pure(this)

  override def pretty: Pretty[DeliverError] = prettyOfClass(
    param("counter", _.counter),
    param("timestamp", _.timestamp),
    param("domain id", _.domainId),
    param("message id", _.messageId),
    param("reason", _.reason),
  )

  def envelopes: Seq[Nothing] = Seq.empty
}

object DeliverError {
  def create(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageId: MessageId,
      reason: DeliverErrorReason,
      protocolVersion: ProtocolVersion,
  ) =
    new DeliverError(counter, timestamp, domainId, messageId, reason)(
      SequencedEvent.protocolVersionRepresentativeFor(protocolVersion),
      None,
    ) {}
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
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class Deliver[+Env <: Envelope[_]] private[sequencing] (
    override val counter: SequencerCounter,
    override val timestamp: CantonTimestamp,
    override val domainId: DomainId,
    messageId: Option[MessageId],
    batch: Batch[Env],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[SequencedEvent.type],
    val deserializedFrom: Option[ByteString],
) extends SequencedEvent[Env] {

  /** Is this deliver event a receipt for a message that the receiver previously sent?
    * (messageId is only populated for the sender)
    */
  lazy val isReceipt: Boolean = messageId.isDefined

  @VisibleForTesting
  protected[sequencing] def toProtoV0: v0.SequencedEvent = v0.SequencedEvent(
    counter = counter.toProtoPrimitive,
    timestamp = Some(timestamp.toProtoPrimitive),
    domainId = domainId.toProtoPrimitive,
    messageId = messageId.map(_.toProtoPrimitive),
    batch = Some(batch.toProtoV0),
    deliverErrorReason = None,
  )

  protected def toProtoV1: v1.SequencedEvent = v1.SequencedEvent(
    counter = counter.toProtoPrimitive,
    timestamp = Some(timestamp.toProtoPrimitive),
    domainId = domainId.toProtoPrimitive,
    messageId = messageId.map(_.toProtoPrimitive),
    batch = Some(batch.toProtoV1),
    deliverErrorReason = None,
  )

  protected def traverse[F[_], Env2 <: Envelope[_]](
      f: Env => F[Env2]
  )(implicit F: Applicative[F]) =
    F.map(batch.traverse(f))(
      Deliver(counter, timestamp, domainId, messageId, _)(
        representativeProtocolVersion,
        deserializedFrom,
      )
    )

  @VisibleForTesting
  private[canton] def copy[Env2 <: Envelope[_]](
      counter: SequencerCounter = this.counter,
      timestamp: CantonTimestamp = this.timestamp,
      domainId: DomainId = this.domainId,
      messageId: Option[MessageId] = this.messageId,
      batch: Batch[Env2] = this.batch,
      deserializedFrom: Option[ByteString] = None,
  ): Deliver[Env2] =
    Deliver[Env2](counter, timestamp, domainId, messageId, batch)(
      representativeProtocolVersion,
      deserializedFrom,
    )

  override def pretty: Pretty[this.type] =
    prettyOfClass(
      param("counter", _.counter),
      param("timestamp", _.timestamp),
      paramIfNonEmpty("message id", _.messageId),
      param("domain id", _.domainId),
      unnamedParam(_.batch),
    )

  def envelopes: Seq[Env] = batch.envelopes
}

object Deliver {
  def create[Env <: Envelope[_]](
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      domainId: DomainId,
      messageId: Option[MessageId],
      batch: Batch[Env],
      protocolVersion: ProtocolVersion,
  ): Deliver[Env] =
    Deliver[Env](counter, timestamp, domainId, messageId, batch)(
      SequencedEvent.protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  def fromSequencedEvent[Env <: Envelope[_]](
      deliverEvent: SequencedEvent[Env]
  ): Option[Deliver[Env]] =
    deliverEvent match {
      case deliver @ Deliver(_, _, _, _, _) => Some(deliver)
      case _: DeliverError => None
    }

  def openEnvelopes(
      deliver: Deliver[ClosedEnvelope]
  )(protocolVersion: ProtocolVersion, hashOps: HashOps): (
      Deliver[OpenEnvelope[ProtocolMessage]],
      Seq[ProtoDeserializationError],
  ) = {
    val (openBatch, openingErrors) =
      Batch.openEnvelopes(deliver.batch)(protocolVersion, hashOps)
    val openDeliver = deliver.copy(
      batch = openBatch,
      // Keep the serialized representation only if there were no errors
      deserializedFrom = if (openingErrors.isEmpty) deliver.deserializedFrom else None,
    )

    (openDeliver, openingErrors)
  }
}
