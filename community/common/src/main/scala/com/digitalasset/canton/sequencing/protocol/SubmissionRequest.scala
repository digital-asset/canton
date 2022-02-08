// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.api.version.VersionedSubmissionRequest
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{
  HasProtoV0WithVersion,
  HasVersionedWrapper,
  HasVersionedWrapperCompanion,
}
import com.digitalasset.canton.version.ProtocolVersion

case class SubmissionRequest private (
    sender: Member,
    messageId: MessageId,
    isRequest: Boolean,
    batch: Batch[ClosedEnvelope],
    maxSequencingTime: CantonTimestamp,
    timestampOfSigningKey: Option[CantonTimestamp],
) extends HasVersionedWrapper[VersionedSubmissionRequest]
    with HasProtoV0WithVersion[v0.SubmissionRequest] {
  override def toProtoVersioned(version: ProtocolVersion): VersionedSubmissionRequest =
    VersionedSubmissionRequest(VersionedSubmissionRequest.Version.V0(toProtoV0(version)))

  // added for serializing in HttpSequencerClient
  def toByteArrayV0(version: ProtocolVersion): Array[Byte] = toProtoV0(version).toByteArray

  override def toProtoV0(version: ProtocolVersion): v0.SubmissionRequest = v0.SubmissionRequest(
    sender = sender.toProtoPrimitive,
    messageId = messageId.toProtoPrimitive,
    isRequest = isRequest,
    batch = Some(batch.toProtoV0(version)),
    maxSequencingTime = Some(maxSequencingTime.toProtoPrimitive),
    timestampOfSigningKey = timestampOfSigningKey.map(_.toProtoPrimitive),
  )

  def isConfirmationRequest(mediator: Member): Boolean =
    batch.envelopes.exists(_.recipients.allRecipients == Set(mediator)) && batch.envelopes.exists(
      e => e.recipients.allRecipients != Set(mediator)
    )

  def isConfirmationResponse(mediator: Member): Boolean =
    batch.envelopes.nonEmpty && batch.envelopes.forall(_.recipients.allRecipients == Set(mediator))
}

object SubmissionRequest
    extends HasVersionedWrapperCompanion[VersionedSubmissionRequest, SubmissionRequest] {
  override protected def ProtoClassCompanion: VersionedSubmissionRequest.type =
    VersionedSubmissionRequest
  override protected def name: String = "submission request"

  override def fromProtoVersioned(
      requestP: VersionedSubmissionRequest
  ): ParsingResult[SubmissionRequest] =
    requestP.version match {
      case VersionedSubmissionRequest.Version.Empty =>
        Left(FieldNotSet("VersionedSubmissionRequest.version"))
      case VersionedSubmissionRequest.Version.V0(request) => fromProtoV0(request)
    }

  def fromProtoV0(
      requestP: v0.SubmissionRequest
  ): ParsingResult[SubmissionRequest] = {
    val v0.SubmissionRequest(
      senderP,
      messageIdP,
      isRequest,
      batchP,
      maxSequencingTimeP,
      timestampOfSigningKey,
    ) =
      requestP

    for {
      sender <- Member.fromProtoPrimitive(senderP, "sender")
      messageId <- MessageId.fromProtoPrimitive(messageIdP)
      maxSequencingTime <- ProtoConverter
        .required("SubmissionRequest.maxSequencingTime", maxSequencingTimeP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
      batch <- ProtoConverter
        .required("SubmissionRequest.batch", batchP)
        .flatMap(Batch.fromProtoV0(ClosedEnvelope.fromProtoV0))
      ts <- timestampOfSigningKey.traverse(CantonTimestamp.fromProtoPrimitive)
    } yield new SubmissionRequest(sender, messageId, isRequest, batch, maxSequencingTime, ts)
  }
}
