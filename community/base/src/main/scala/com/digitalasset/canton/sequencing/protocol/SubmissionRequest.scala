// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SubmissionRequest private (
    sender: Member,
    messageId: MessageId,
    isRequest: Boolean,
    batch: Batch[ClosedEnvelope],
    maxSequencingTime: CantonTimestamp,
    timestampOfSigningKey: Option[CantonTimestamp],
)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[SubmissionRequest],
    override val deserializedFrom: Option[ByteString] = None,
) extends HasProtocolVersionedWrapper[SubmissionRequest]
    with ProtocolVersionedMemoizedEvidence {
  private lazy val batchProtoV0: v0.CompressedBatch = batch.toProtoV0

  override val companionObj: SubmissionRequest.type = SubmissionRequest

  // Caches the serialized request to be able to do checks on its size without re-serializing
  lazy val toProtoV0: v0.SubmissionRequest = v0.SubmissionRequest(
    sender = sender.toProtoPrimitive,
    messageId = messageId.toProtoPrimitive,
    isRequest = isRequest,
    batch = Some(batchProtoV0),
    maxSequencingTime = Some(maxSequencingTime.toProtoPrimitive),
    timestampOfSigningKey = timestampOfSigningKey.map(_.toProtoPrimitive),
  )

  // No need to cache V1 because this is private and therefore properly memoized
  private def toProtoV1: v1.SubmissionRequest = v1.SubmissionRequest(
    sender = sender.toProtoPrimitive,
    messageId = messageId.toProtoPrimitive,
    isRequest = isRequest,
    batch = Some(batch.toProtoV1),
    maxSequencingTime = Some(maxSequencingTime.toProtoPrimitive),
    timestampOfSigningKey = timestampOfSigningKey.map(_.toProtoPrimitive),
  )

  def copy(
      sender: Member = sender,
      messageId: MessageId = messageId,
      isRequest: Boolean = isRequest,
      batch: Batch[ClosedEnvelope] = batch,
      maxSequencingTime: CantonTimestamp = maxSequencingTime,
      timestampOfSigningKey: Option[CantonTimestamp] = timestampOfSigningKey,
  ) = SubmissionRequest(
    sender,
    messageId,
    isRequest,
    batch,
    maxSequencingTime,
    timestampOfSigningKey,
  )(representativeProtocolVersion)

  def isConfirmationRequest(mediator: Member): Boolean =
    batch.envelopes.exists(_.recipients.allRecipients == Set(mediator)) && batch.envelopes.exists(
      e => e.recipients.allRecipients != Set(mediator)
    )

  def isConfirmationResponse(mediator: Member): Boolean =
    batch.envelopes.nonEmpty && batch.envelopes.forall(_.recipients.allRecipients == Set(mediator))

  def isMediatorResult(mediator: Member): Boolean = batch.envelopes.nonEmpty && sender == mediator

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString
}
sealed trait MaxRequestSizeToDeserialize {
  val toOption: Option[NonNegativeInt] = this match {
    case MaxRequestSizeToDeserialize.Limit(value) => Some(value)
    case MaxRequestSizeToDeserialize.NoLimit => None
  }
}
object MaxRequestSizeToDeserialize {
  final case class Limit(value: NonNegativeInt) extends MaxRequestSizeToDeserialize
  case object NoLimit extends MaxRequestSizeToDeserialize
}

object SubmissionRequest
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      SubmissionRequest,
      MaxRequestSizeToDeserialize,
    ] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter.mk(ProtocolVersion.v3)(v0.SubmissionRequest)(
      supportedProtoVersionMemoized(_) { (maxRequestSize, req) => bytes =>
        fromProtoV0(maxRequestSize)(req, Some(bytes))
      },
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter.mk(
      // TODO(#12373) Adapt when releasing BFT
      ProtocolVersion.dev
    )(v1.SubmissionRequest)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  override protected def name: String = "submission request"

  def apply(
      sender: Member,
      messageId: MessageId,
      isRequest: Boolean,
      batch: Batch[ClosedEnvelope],
      maxSequencingTime: CantonTimestamp,
      timestampOfSigningKey: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): SubmissionRequest =
    SubmissionRequest(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      timestampOfSigningKey,
    )(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV0(
      requestP: v0.SubmissionRequest,
      maxRequestSize: MaxRequestSizeToDeserialize,
  ): ParsingResult[SubmissionRequest] =
    fromProtoV0(maxRequestSize)(requestP, None)

  private def fromProtoV0(maxRequestSize: MaxRequestSizeToDeserialize)(
      requestP: v0.SubmissionRequest,
      bytes: Option[ByteString],
  ): ParsingResult[SubmissionRequest] = {
    val v0.SubmissionRequest(
      senderP,
      messageIdP,
      isRequest,
      batchP,
      maxSequencingTimeP,
      timestampOfSigningKey,
    ) = requestP

    for {
      sender <- Member.fromProtoPrimitive(senderP, "sender")
      messageId <- MessageId.fromProtoPrimitive(messageIdP)
      maxSequencingTime <- ProtoConverter
        .required("SubmissionRequest.maxSequencingTime", maxSequencingTimeP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
      batch <- ProtoConverter
        .required("SubmissionRequest.batch", batchP)
        .flatMap(Batch.fromProtoV0(_, maxRequestSize))
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
    } yield new SubmissionRequest(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      ts,
    )(
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      bytes,
    )
  }

  private def fromProtoV1(
      maxRequestSize: MaxRequestSizeToDeserialize,
      requestP: v1.SubmissionRequest,
  )(bytes: ByteString): ParsingResult[SubmissionRequest] = {
    val v1.SubmissionRequest(
      senderP,
      messageIdP,
      isRequest,
      batchP,
      maxSequencingTimeP,
      timestampOfSigningKey,
    ) = requestP

    for {
      sender <- Member.fromProtoPrimitive(senderP, "sender")
      messageId <- MessageId.fromProtoPrimitive(messageIdP)
      maxSequencingTime <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "SubmissionRequest.maxSequencingTime",
        maxSequencingTimeP,
      )
      batch <- ProtoConverter.parseRequired(
        Batch.fromProtoV1(_, maxRequestSize),
        "SubmissionRequest.batch",
        batchP,
      )
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
    } yield new SubmissionRequest(
      sender,
      messageId,
      isRequest,
      batch,
      maxSequencingTime,
      ts,
    )(
      protocolVersionRepresentativeFor(ProtoVersion(1)),
      Some(bytes),
    )
  }

  def usingSignedSubmissionRequest(protocolVersion: ProtocolVersion): Boolean =
    protocolVersion >= ProtocolVersion.v4

  def usingVersionedSubmissionRequest(protocolVersion: ProtocolVersion): Boolean =
    protocolVersion >= ProtocolVersion.v5
}
